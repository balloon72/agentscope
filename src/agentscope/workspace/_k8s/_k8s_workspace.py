# -*- coding: utf-8 -*-
"""K8sWorkspace — sandboxed workspace backed by a Kubernetes Pod.

Architecture
------------

Mirrors :class:`agentscope.workspace.DockerWorkspace` but swaps the
Docker engine for the Kubernetes API (``kubernetes_asyncio``):

* **Lifecycle.** ``initialize()`` looks up an existing Pod by label,
  reuses it if Running, deletes-and-recreates if Failed/Unknown, or
  creates a new one. PVCs survive Pod deletion for data persistence.
  ``close()`` deletes the Pod by default but can leave shared Pods in place
  with ``delete_pod_on_close=False`` (the multi-node API setting). PVCs are
  kept by default and are deleted only when Pod deletion is enabled and
  ``delete_pvc_on_close=True`` is passed at construction.
* **Persistence.** A PVC (``as-ws-{workspace_id}``) mounted at
  ``/workspace`` provides cross-Pod-restart persistence. Skills,
  ``.mcp``, sessions, and data survive restarts.
* **Bootstrap.** First-time provisioning installs system deps +
  uv + gateway venv + agentscope and uploads the gateway script.
  The probe + install loop lives on :class:`SandboxedWorkspaceBase`;
  this subclass only supplies Pod-specific shell commands via
  :meth:`_bootstrap_commands`.
* **MCP gateway.** Identical to Docker/E2B: a FastAPI process inside
  the Pod, host talks to it via :class:`GatewayClient` through the
  gateway shim (``exec_shell``-based transport, no host-to-Pod
  network required).
"""

import asyncio
import shlex
from typing import Any

from ..._logging import logger
from ...mcp import MCPClient
from .._sandboxed_base import SandboxedWorkspaceBase
from .._utils import _GATEWAY_BASE_REQUIREMENTS, DEFAULT_WORKSPACE_INSTRUCTIONS
from ._k8s_backend import K8sBackend
from ._constants import (
    DEFAULT_GATEWAY_PORT,
    DEFAULT_IMAGE,
    GATEWAY_HOME,
    POD_WORKDIR,
    SYSTEM_DEPS,
    WORKSPACE_EPOCH_ENV,
    WORKSPACE_EPOCH_LABEL,
    WORKSPACE_ID_LABEL,
    _k8s_safe_name,
)


class K8sWorkspaceLeaseLostError(RuntimeError):
    """Raised when a Pod belongs to a different workspace lease epoch.

    The workspace id is stable across process restarts, so it is not enough
    to key a Pod by name.  A process that still holds an old workspace object
    must fail closed once another owner has replaced the Pod for the same id.
    """


# ── the workspace ──────────────────────────────────────────────────


class K8sWorkspace(SandboxedWorkspaceBase):
    """Workspace backed by a Kubernetes Pod with PVC persistence.

    ``default_mcps`` and ``skill_paths`` are seed-time inputs and are
    not retained as instance state past :meth:`initialize`.
    """

    _gateway_home = GATEWAY_HOME

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        lease_epoch: str | int | None = None,
        epoch: str | int | None = None,
        # ── K8s connection ──
        kubeconfig: str | None = None,
        namespace: str = "agentscope",
        # ── Pod construction ──
        image: str = DEFAULT_IMAGE,
        image_pull_policy: str = "IfNotPresent",
        image_pull_secrets: list[str] | None = None,
        resources: dict[str, Any] | None = None,
        node_selector: dict[str, str] | None = None,
        tolerations: list[dict[str, Any]] | None = None,
        service_account: str | None = None,
        # ── Gateway ──
        gateway_port: int = DEFAULT_GATEWAY_PORT,
        extra_pip: list[str] | None = None,
        # ── Persistence ──
        delete_pod_on_close: bool = True,
        storage_class: str | None = None,
        storage_size: str = "1Gi",
        delete_pvc_on_close: bool = False,
        # ── Environment ──
        env: dict[str, str] | None = None,
        instructions: str = DEFAULT_WORKSPACE_INSTRUCTIONS,
        # ── Seed ──
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
    ) -> None:
        """Construct a :class:`K8sWorkspace`.

        The Pod is *not* started here; call :meth:`initialize`
        (or use the workspace as an ``async`` context manager).

        Args:
            workspace_id (`str | None`, optional):
                Stable identifier. ``None`` generates a fresh UUID.
            lease_epoch (`str | int | None`, optional):
                Fencing epoch for this workspace lease. The value is copied
                into a Pod label and the ``AGENTSCOPE_WORKSPACE_EPOCH``
                environment variable. Reattaching to a Pod with another
                epoch raises :class:`K8sWorkspaceLeaseLostError`. ``None``
                uses the compatibility epoch ``"0"``; distributed callers
                should pass the epoch returned by their lease store.
            epoch (`str | int | None`, optional):
                Short alias for ``lease_epoch``.
            kubeconfig (`str | None`, optional):
                Path to kubeconfig file. ``None`` uses in-cluster
                config.
            namespace (`str`, defaults to ``"agentscope"``):
                K8s namespace for the Pod and PVC.
            image (`str`, defaults to ``"python:3.11-slim"``):
                Container image.
            image_pull_policy (`str`, defaults to ``"IfNotPresent"``):
                K8s imagePullPolicy.
            image_pull_secrets (`list[str] | None`, optional):
                Names of K8s image pull secrets.
            resources (`dict[str, Any] | None`, optional):
                K8s ResourceRequirements dict.
            node_selector (`dict[str, str] | None`, optional):
                K8s nodeSelector.
            tolerations (`list[dict[str, Any]] | None`, optional):
                K8s tolerations list.
            service_account (`str | None`, optional):
                K8s serviceAccountName.
            gateway_port (`int`, defaults to `5600`):
                Port the gateway listens on inside the Pod.
            extra_pip (`list[str] | None`, optional):
                Extra packages for the gateway venv.
            storage_class (`str | None`, optional):
                K8s StorageClass name. ``None`` uses cluster default.
            storage_size (`str`, defaults to ``"1Gi"``):
                PVC size.
            delete_pod_on_close (`bool`, defaults to ``True``):
                Delete the workspace Pod during :meth:`close`. Set to
                ``False`` when multiple API workers share the Pod lifecycle.
            delete_pvc_on_close (`bool`, defaults to ``False``):
                When ``True`` and Pod deletion is enabled, :meth:`close`
                also deletes the PVC.
            env (`dict[str, str] | None`, optional):
                Environment variables for the container.
            instructions (`str`):
                System-prompt fragment template.
            default_mcps (`list[MCPClient] | None`, optional):
                MCPs seeded on first init.
            skill_paths (`list[str] | None`, optional):
                Skill directories seeded on first init.
        """
        super().__init__(
            workspace_id=workspace_id,
            default_mcps=default_mcps,
            skill_paths=skill_paths,
        )

        # ── serializable config ─────────────────────────────────
        self.workdir = POD_WORKDIR
        if (
            lease_epoch is not None
            and epoch is not None
            and str(lease_epoch) != str(epoch)
        ):
            raise ValueError("lease_epoch and epoch must match when both are set")
        effective_epoch = lease_epoch if lease_epoch is not None else epoch
        self.lease_epoch = "0" if effective_epoch is None else str(effective_epoch)
        if not self.lease_epoch or len(self.lease_epoch) > 63:
            raise ValueError("lease_epoch must be 1-63 characters")
        if any(
            not (c.isascii() and (c.isalnum() or c in {"-", "_", "."}))
            for c in self.lease_epoch
        ):
            raise ValueError(
                "lease_epoch must contain only ASCII letters, digits, "
                "hyphens, underscores, or dots",
            )
        self._kubeconfig = kubeconfig
        self._namespace = namespace
        self._image = image
        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = list(image_pull_secrets or [])
        self._resources = resources
        self._node_selector = node_selector
        self._tolerations = tolerations
        self._service_account = service_account
        self.gateway_port = gateway_port
        self.extra_pip: list[str] = list(extra_pip or [])
        self._delete_pod_on_close = delete_pod_on_close
        self._storage_class = storage_class
        self._storage_size = storage_size
        self._delete_pvc_on_close = delete_pvc_on_close
        self.env: dict[str, str] = dict(env or {})
        self.instructions = instructions.format(
            backend="Kubernetes-based",
            workdir=self.workdir,
        )

        # ── runtime state (K8s-only) ────────────────────────────
        self._api_client: Any = None
        self._v1: Any = None  # CoreV1Api
        self._pod_name: str = ""

    # ── lifecycle hooks ─────────────────────────────────────────

    async def _provision_backend(self) -> None:
        """Create or reattach to the K8s Pod, bootstrap if needed."""
        from kubernetes_asyncio import client as k8s_client
        from kubernetes_asyncio import config as k8s_config

        if self._kubeconfig:
            await k8s_config.load_kube_config(
                config_file=self._kubeconfig,
            )
        else:
            try:
                k8s_config.load_incluster_config()
            except k8s_config.ConfigException:
                await k8s_config.load_kube_config()

        self._api_client = k8s_client.ApiClient()
        self._v1 = k8s_client.CoreV1Api(self._api_client)

        self._pod_name = _k8s_safe_name(self.workspace_id)

        await self._ensure_namespace()
        await self._ensure_pvc()
        await self._ensure_pod()
        await self._wait_pod_running()

        self._backend = K8sBackend(
            api_client=self._api_client,
            namespace=self._namespace,
            pod_name=self._pod_name,
            container_name="workspace",
            workdir=POD_WORKDIR,
            lease_check=self.check_lease,
        )

    async def _teardown_backend(self) -> None:
        """Delete the Pod (and optionally the PVC), release the API
        client. Errors are swallowed so teardown is always safe.
        """
        lease_valid = True
        if self._v1 is not None and self._pod_name:
            if self._delete_pod_on_close:
                try:
                    # Never let a stale process delete a replacement Pod
                    # owned by a newer epoch. Losing the lease is expected
                    # during a takeover; the current owner remains untouched.
                    resource_version = (
                        await self._check_lease_resource_version()
                    )
                    if resource_version is None:
                        raise K8sWorkspaceLeaseLostError(
                            "Workspace Pod has no resourceVersion",
                        )
                    await self._delete_pod_if_current(resource_version)
                except K8sWorkspaceLeaseLostError:
                    lease_valid = False
                    logger.info(
                        "K8sWorkspace: skip Pod delete after lease loss (%s)",
                        self._pod_name,
                    )
                except Exception as e:
                    logger.warning("K8sWorkspace: Pod delete failed: %s", e)

            if (
                self._delete_pod_on_close
                and self._delete_pvc_on_close
                and lease_valid
            ):
                try:
                    await self._delete_pvc_if_current()
                except K8sWorkspaceLeaseLostError:
                    logger.info(
                        "K8sWorkspace: skip PVC delete after lease loss (%s)",
                        self._pod_name,
                    )
                except Exception as e:
                    logger.warning(
                        "K8sWorkspace: PVC delete failed: %s",
                        e,
                    )

        if self._api_client is not None:
            try:
                await self._api_client.close()
            except Exception:
                pass
            self._api_client = None
            self._v1 = None

    # ── instructions ────────────────────────────────────────────

    async def get_instructions(self) -> str:
        """Return the system-prompt fragment for this workspace."""
        return self.instructions

    # ── internals: K8s resource management ─────────────────────

    async def _ensure_namespace(self) -> None:
        """Create the namespace if it doesn't exist."""
        from kubernetes_asyncio.client.rest import ApiException

        try:
            await self._v1.read_namespace(self._namespace)
        except ApiException as e:
            if e.status == 404:
                from kubernetes_asyncio import client as k8s_client

                ns = k8s_client.V1Namespace(
                    metadata=k8s_client.V1ObjectMeta(
                        name=self._namespace,
                    ),
                )
                await self._v1.create_namespace(ns)
            else:
                raise

    async def _ensure_pvc(self) -> None:
        """Create or reuse the PVC for workspace persistence.

        Deletion-in-progress is detected via
        ``metadata.deletion_timestamp`` (K8s sets this field when a
        ``DELETE`` has been accepted but finalizers have not yet
        completed).  ``status.phase`` does not have a
        ``"Terminating"`` value in upstream Kubernetes.
        """
        from kubernetes_asyncio.client.rest import ApiException

        pvc_name = self._pod_name
        try:
            pvc = await self._v1.read_namespaced_persistent_volume_claim(
                pvc_name,
                self._namespace,
            )
            if pvc.metadata and pvc.metadata.deletion_timestamp is not None:
                # A stale owner must not recreate a PVC that a newer owner is
                # deleting.  Validate before waiting for deletion; once the
                # object disappears there is no metadata left to fence on.
                self._validate_pvc_lease(pvc)
                logger.info(
                    "K8sWorkspace: PVC %r is being deleted, waiting...",
                    pvc_name,
                )
                await self._wait_pvc_deleted(pvc_name)
                await self._create_pvc(pvc_name)
            else:
                self._validate_pvc_lease(pvc)
        except ApiException as e:
            if e.status == 404:
                await self._create_pvc(pvc_name)
            else:
                raise

    async def _create_pvc(self, pvc_name: str) -> None:
        """Create a new PVC."""
        from kubernetes_asyncio import client as k8s_client

        spec_kwargs: dict[str, Any] = {
            "access_modes": ["ReadWriteOnce"],
            "resources": k8s_client.V1VolumeResourceRequirements(
                requests={"storage": self._storage_size},
            ),
        }
        if self._storage_class is not None:
            spec_kwargs["storage_class_name"] = self._storage_class

        pvc = k8s_client.V1PersistentVolumeClaim(
            metadata=k8s_client.V1ObjectMeta(
                name=pvc_name,
                namespace=self._namespace,
                labels={
                    "app.kubernetes.io/managed-by": "agentscope",
                    WORKSPACE_ID_LABEL: self.workspace_id,
                    WORKSPACE_EPOCH_LABEL: self.lease_epoch,
                },
            ),
            spec=k8s_client.V1PersistentVolumeClaimSpec(**spec_kwargs),
        )
        await self._v1.create_namespaced_persistent_volume_claim(
            self._namespace,
            pvc,
        )

    async def _wait_pvc_deleted(
        self,
        pvc_name: str,
        timeout: float = 60.0,
    ) -> None:
        """Poll until the PVC is fully deleted."""
        from kubernetes_asyncio.client.rest import ApiException

        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                await self._v1.read_namespaced_persistent_volume_claim(
                    pvc_name,
                    self._namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    return
                raise
            await asyncio.sleep(1.0)
        raise RuntimeError(
            f"PVC {pvc_name!r} did not finish deleting " f"within {timeout}s",
        )

    def _validate_pvc_lease(self, pvc: Any) -> None:
        """Validate identity and epoch persisted on the workspace PVC."""
        metadata = getattr(pvc, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        if (
            labels.get("app.kubernetes.io/managed-by") != "agentscope"
            or labels.get(WORKSPACE_ID_LABEL) != self.workspace_id
        ):
            raise K8sWorkspaceLeaseLostError(
                f"PVC {self._pod_name!r} is not owned by workspace "
                f"{self.workspace_id!r}.",
            )
        label_epoch = labels.get(WORKSPACE_EPOCH_LABEL)
        if label_epoch != self.lease_epoch:
            raise K8sWorkspaceLeaseLostError(
                f"PVC {self._pod_name!r} has lease epoch "
                f"{label_epoch!r}; expected {self.lease_epoch!r}.",
            )

    async def _ensure_pod(self) -> None:
        """Create or reuse the workspace Pod.

        Only terminal phases (``Failed``, ``Unknown``, ``Succeeded``)
        trigger a delete-and-recreate cycle.  ``Pending`` Pods are left
        for :meth:`_wait_pod_running` which inspects container statuses
        for early failure detection.

        A Pod whose ``metadata.deletion_timestamp`` is set is already
        being deleted (by a previous ``close()`` or an external actor)
        — attaching to it would race the terminator, so we wait for it
        to disappear and then create a fresh one.
        """
        from kubernetes_asyncio.client.rest import ApiException

        _REBUILD_PHASES = frozenset({"Failed", "Unknown", "Succeeded"})

        try:
            pod = await self._v1.read_namespaced_pod(
                self._pod_name,
                self._namespace,
            )
            phase = pod.status.phase if pod.status else None
            self._validate_pod_lease(pod)
            deletion_ts = (
                pod.metadata.deletion_timestamp
                if pod.metadata is not None
                else None
            )
            if deletion_ts is not None:
                logger.info(
                    "K8sWorkspace: Pod %r is being deleted, "
                    "waiting to recreate",
                    self._pod_name,
                )
                await self._wait_pod_deleted()
                await self._create_pod()
                return
            if phase in {"Running", "Pending"}:
                return
            if phase in _REBUILD_PHASES:
                logger.info(
                    "K8sWorkspace: Pod %r is %s, deleting and recreating",
                    self._pod_name,
                    phase,
                )
                try:
                    resource_version = getattr(
                        getattr(pod, "metadata", None),
                        "resource_version",
                        None,
                    )
                    await self._delete_pod_if_current(resource_version)
                except ApiException:
                    pass
                await self._wait_pod_deleted()
                await self._create_pod()
            else:
                logger.warning(
                    "K8sWorkspace: Pod %r has unexpected phase %r, "
                    "deleting and recreating",
                    self._pod_name,
                    phase,
                )
                try:
                    resource_version = getattr(
                        getattr(pod, "metadata", None),
                        "resource_version",
                        None,
                    )
                    await self._delete_pod_if_current(resource_version)
                except ApiException:
                    pass
                await self._wait_pod_deleted()
                await self._create_pod()
        except ApiException as e:
            if e.status == 404:
                await self._create_pod()
            else:
                raise

    def _validate_pod_lease(self, pod: Any) -> None:
        """Validate ownership metadata before touching an existing Pod.

        Identity labels and both lease metadata surfaces are checked.  The
        workspace id is required because the deterministic, sanitised Pod
        name can collide for distinct user ids.
        """
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        if (
            labels.get("app.kubernetes.io/managed-by") != "agentscope"
            or labels.get("agentscope.workspace") != "true"
            or labels.get(WORKSPACE_ID_LABEL) != self.workspace_id
        ):
            raise K8sWorkspaceLeaseLostError(
                f"Pod {self._pod_name!r} is not owned by workspace "
                f"{self.workspace_id!r}.",
            )
        label_epoch = labels.get(WORKSPACE_EPOCH_LABEL)

        spec = getattr(pod, "spec", None)
        containers = getattr(spec, "containers", None) or []
        env_epoch: str | None = None
        for container in containers:
            for env_var in getattr(container, "env", None) or []:
                if getattr(env_var, "name", None) == WORKSPACE_EPOCH_ENV:
                    env_epoch = getattr(env_var, "value", None)
                    break
            if env_epoch is not None:
                break

        if label_epoch != self.lease_epoch or env_epoch != self.lease_epoch:
            raise K8sWorkspaceLeaseLostError(
                f"Pod {self._pod_name!r} has lease epoch "
                f"label={label_epoch!r}, env={env_epoch!r}; "
                f"expected {self.lease_epoch!r}.",
            )

    async def _read_checked_pod(self) -> Any:
        """Read the backing Pod and verify the current lease metadata."""
        from kubernetes_asyncio.client.rest import ApiException

        if self._v1 is None or not self._pod_name:
            raise K8sWorkspaceLeaseLostError(
                "K8s workspace is not attached to a Pod",
            )
        try:
            pod = await self._v1.read_namespaced_pod(
                self._pod_name,
                self._namespace,
            )
        except ApiException as exc:
            raise K8sWorkspaceLeaseLostError(
                f"Unable to verify workspace Pod {self._pod_name!r}",
            ) from exc
        self._validate_pod_lease(pod)
        return pod

    async def check_lease(self) -> None:
        """Fail closed if the backing Pod has been fenced by another owner.

        Workspace backends call this before every exec/write operation.  It
        is also public so applications performing direct lifecycle work can
        check ownership before a side effect.
        """
        await self._read_checked_pod()

    async def _check_lease_resource_version(self) -> str | None:
        """Return the checked Pod version for a conditional delete."""
        pod = await self._read_checked_pod()
        return getattr(
            getattr(pod, "metadata", None),
            "resource_version",
            None,
        )

    async def _delete_pod_if_current(
        self,
        resource_version: str | None,
    ) -> None:
        """Delete the Pod only if it is still the object we inspected.

        Kubernetes resource-version preconditions turn the lease check and
        delete into a compare-and-delete operation.  A newer owner replacing
        the Pod in between receives HTTP 409 and the stale caller fails
        closed instead of deleting the replacement by deterministic name.
        """
        from kubernetes_asyncio import client as k8s_client
        from kubernetes_asyncio.client.rest import ApiException

        if not resource_version:
            raise K8sWorkspaceLeaseLostError(
                f"Workspace Pod {self._pod_name!r} has no resourceVersion",
            )
        body = k8s_client.V1DeleteOptions(
            preconditions=k8s_client.V1Preconditions(
                resource_version=str(resource_version),
            ),
        )
        try:
            await self._v1.delete_namespaced_pod(
                self._pod_name,
                self._namespace,
                body=body,
            )
        except ApiException as exc:
            if exc.status == 409:
                raise K8sWorkspaceLeaseLostError(
                    f"Workspace Pod {self._pod_name!r} changed before delete",
                ) from exc
            raise

    async def _delete_pvc_if_current(self) -> None:
        """Delete the PVC only while its epoch and version are unchanged."""
        from kubernetes_asyncio import client as k8s_client
        from kubernetes_asyncio.client.rest import ApiException

        try:
            pvc = await self._v1.read_namespaced_persistent_volume_claim(
                self._pod_name,
                self._namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise
        self._validate_pvc_lease(pvc)
        resource_version = getattr(
            getattr(pvc, "metadata", None),
            "resource_version",
            None,
        )
        if not resource_version:
            raise K8sWorkspaceLeaseLostError(
                f"Workspace PVC {self._pod_name!r} has no resourceVersion",
            )
        body = k8s_client.V1DeleteOptions(
            preconditions=k8s_client.V1Preconditions(
                resource_version=str(resource_version),
            ),
        )
        try:
            await self._v1.delete_namespaced_persistent_volume_claim(
                self._pod_name,
                self._namespace,
                body=body,
            )
        except ApiException as exc:
            if exc.status == 409:
                raise K8sWorkspaceLeaseLostError(
                    f"Workspace PVC {self._pod_name!r} changed before delete",
                ) from exc
            raise

    async def _create_pod(self) -> None:
        """Create the workspace Pod."""
        from kubernetes_asyncio import client as k8s_client

        # The lease epoch is reserved by the manager.  Refuse a conflicting
        # caller-provided value rather than silently allowing a Pod whose
        # environment disagrees with its control-plane label.
        configured_epoch = self.env.get(WORKSPACE_EPOCH_ENV)
        if configured_epoch is not None and configured_epoch != self.lease_epoch:
            raise ValueError(
                f"{WORKSPACE_EPOCH_ENV} is reserved for the workspace lease "
                f"and must equal {self.lease_epoch!r}.",
            )
        env = dict(self.env)
        env[WORKSPACE_EPOCH_ENV] = self.lease_epoch
        container_env = [
            k8s_client.V1EnvVar(name=k, value=v) for k, v in env.items()
        ]

        container = k8s_client.V1Container(
            name="workspace",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            command=["sleep", "infinity"],
            working_dir=POD_WORKDIR,
            ports=[
                k8s_client.V1ContainerPort(
                    container_port=self.gateway_port,
                ),
            ],
            resources=(
                k8s_client.V1ResourceRequirements(**self._resources)
                if self._resources
                else None
            ),
            volume_mounts=[
                k8s_client.V1VolumeMount(
                    name="workspace-data",
                    mount_path=POD_WORKDIR,
                ),
            ],
            env=container_env,
        )

        volumes = [
            k8s_client.V1Volume(
                name="workspace-data",
                persistent_volume_claim=(
                    k8s_client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=self._pod_name,
                    )
                ),
            ),
        ]

        spec_kwargs: dict[str, Any] = {
            "restart_policy": "OnFailure",
            "containers": [container],
            "volumes": volumes,
        }
        if self._node_selector:
            spec_kwargs["node_selector"] = self._node_selector
        if self._tolerations:
            spec_kwargs["tolerations"] = [
                k8s_client.V1Toleration(**t) for t in self._tolerations
            ]
        if self._service_account:
            spec_kwargs["service_account_name"] = self._service_account
        if self._image_pull_secrets:
            spec_kwargs["image_pull_secrets"] = [
                k8s_client.V1LocalObjectReference(name=s)
                for s in self._image_pull_secrets
            ]

        pod = k8s_client.V1Pod(
            metadata=k8s_client.V1ObjectMeta(
                name=self._pod_name,
                namespace=self._namespace,
                labels={
                    "app.kubernetes.io/managed-by": "agentscope",
                    "agentscope.workspace": "true",
                    WORKSPACE_ID_LABEL: self.workspace_id,
                    WORKSPACE_EPOCH_LABEL: self.lease_epoch,
                },
            ),
            spec=k8s_client.V1PodSpec(**spec_kwargs),
        )
        await self._v1.create_namespaced_pod(self._namespace, pod)

    async def _wait_pod_running(self, timeout: float = 120.0) -> None:
        """Poll until the Pod phase is Running.

        Also inspects container statuses during the Pending phase to
        detect unrecoverable conditions (``ImagePullBackOff``,
        ``ErrImagePull``, ``InvalidImageName``) early instead of
        waiting for the full timeout.
        """
        _TERMINAL_WAITING_REASONS = frozenset(
            {
                "ImagePullBackOff",
                "ErrImagePull",
                "InvalidImageName",
                "CrashLoopBackOff",
            },
        )
        _UNSCHEDULABLE_TYPES = frozenset(
            {"PodScheduled"},
        )

        deadline = asyncio.get_event_loop().time() + timeout
        delay = 0.5
        while asyncio.get_event_loop().time() < deadline:
            pod = await self._v1.read_namespaced_pod(
                self._pod_name,
                self._namespace,
            )
            phase = pod.status.phase if pod.status else None
            if phase == "Running":
                return
            if phase in ("Failed", "Unknown"):
                raise RuntimeError(
                    f"Pod {self._pod_name!r} entered {phase} state",
                )

            if phase == "Pending" and pod.status:
                for cs in pod.status.container_statuses or []:
                    if cs.state and cs.state.waiting:
                        reason = cs.state.waiting.reason or ""
                        if reason in _TERMINAL_WAITING_REASONS:
                            msg = cs.state.waiting.message or reason
                            raise RuntimeError(
                                f"Pod {self._pod_name!r} container "
                                f"is stuck: {msg}",
                            )
                for cond in pod.status.conditions or []:
                    if (
                        cond.type in _UNSCHEDULABLE_TYPES
                        and cond.status == "False"
                        and cond.reason == "Unschedulable"
                    ):
                        raise RuntimeError(
                            f"Pod {self._pod_name!r} is "
                            f"unschedulable: {cond.message}",
                        )

            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 3.0)
        raise RuntimeError(
            f"Pod {self._pod_name!r} did not become Running "
            f"within {timeout}s",
        )

    async def _wait_pod_deleted(self, timeout: float = 30.0) -> None:
        """Poll until the Pod is gone."""
        from kubernetes_asyncio.client.rest import ApiException

        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                await self._v1.read_namespaced_pod(
                    self._pod_name,
                    self._namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    return
                raise
            await asyncio.sleep(1.0)
        raise RuntimeError(
            f"Pod {self._pod_name!r} did not finish deleting "
            f"within {timeout}s",
        )

    # ── internals: bootstrap ────────────────────────────────────

    def _bootstrap_commands(self) -> list[str]:
        """Shell commands that provision this Pod once.

        Only runs when the gateway script is missing (fresh PVC, or a
        Pod that died before the script was written). Slim base image
        runs as root, so no ``sudo`` needed — uv lands at
        ``/usr/local/bin/uv`` which is on the default PATH.
        """
        pip_pkgs = list(_GATEWAY_BASE_REQUIREMENTS) + list(self.extra_pip)
        # Quote every requirement string so entries containing spaces
        # or shell metacharacters (e.g. version specifiers wrapped in
        # brackets, direct-URL installs) cannot break the ``sh -c``
        # command or become an injection vector.
        pip_args = " ".join(shlex.quote(p) for p in pip_pkgs)
        sys_deps = " ".join(shlex.quote(d) for d in SYSTEM_DEPS)

        return [
            f"apt-get update -qq "
            f"&& apt-get install -y --no-install-recommends {sys_deps} "
            f"&& rm -rf /var/lib/apt/lists/*",
            "curl -LsSf https://astral.sh/uv/install.sh "
            "| env UV_INSTALL_DIR=/usr/local/bin "
            "INSTALLER_NO_MODIFY_PATH=1 sh",
            f"uv venv {self._gateway_venv}",
            f"uv pip install --python {self._gateway_python} {pip_args}",
            f"uv pip install --python {self._gateway_python} "
            f"--no-deps 'agentscope'",
        ]
