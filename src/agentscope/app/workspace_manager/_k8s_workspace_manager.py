# -*- coding: utf-8 -*-
"""K8sWorkspaceManager — lifecycle manager for :class:`K8sWorkspace`.

Mirrors :class:`E2BWorkspaceManager` 1:1 in its public surface
(``get_workspace`` / ``create_workspace`` / ``close`` / ``close_all``)
so callers do not branch on backend.

Differences from the E2B manager:

* Reattachment uses a deterministic Pod/PVC name derived from
  ``workspace_id`` (via ``_k8s_safe_name``).
  :meth:`K8sWorkspace.initialize` handles rediscovery.
* ``close()`` deletes the Pod by default; set ``delete_pod_on_close=False``
  for a multi-node API worker that shares Pod lifecycle with other workers.
  PVC deletion is governed by the workspace's ``_delete_pvc_on_close``
  attribute and is skipped whenever Pod deletion is disabled.
* Idle workspaces are evicted by a dedicated background sweeper task.
"""

import asyncio
import os
import time
from typing import Any, Self

from typing_extensions import deprecated

from ..._logging import logger
from ...mcp import MCPClient
from ...workspace import K8sWorkspace
from ...workspace._k8s._constants import (
    DEFAULT_GATEWAY_PORT,
    DEFAULT_IMAGE,
)
from ._base import WorkspaceManagerBase, IsolationPolicy

DEFAULT_SWEEP_INTERVAL = 300.0


class K8sWorkspaceManager(WorkspaceManagerBase):
    """Manages :class:`K8sWorkspace` instances with TTL-based caching.

    Use as an ``async with`` context manager: entering starts the TTL
    sweeper, exiting stops it and closes every cached workspace.
    """

    def __init__(
        self,
        *,
        isolation: IsolationPolicy = IsolationPolicy.PER_AGENT,
        lease_epoch: str | int | None = None,
        epoch: str | int | None = None,
        namespace: str = "agentscope",
        kubeconfig: str | None = None,
        image: str = DEFAULT_IMAGE,
        image_pull_policy: str = "IfNotPresent",
        image_pull_secrets: list[str] | None = None,
        resources: dict[str, Any] | None = None,
        node_selector: dict[str, str] | None = None,
        tolerations: list[dict[str, Any]] | None = None,
        service_account: str | None = None,
        gateway_port: int = DEFAULT_GATEWAY_PORT,
        extra_pip: list[str] | None = None,
        storage_class: str | None = None,
        storage_size: str = "1Gi",
        env: dict[str, str] | None = None,
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
        ttl: float = 3600.0,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        delete_pod_on_close: bool = True,
        delete_pvc_on_close: bool = False,
    ) -> None:
        """Initialize the K8s workspace manager.

        Args:
            isolation (`IsolationPolicy`, defaults to `PER_AGENT`):
                Isolation grain for :meth:`assign_workspace_id`. See
                :class:`DockerWorkspaceManager` for semantics.
            namespace (`str`, defaults to ``"agentscope"``):
                K8s namespace for workspace Pods and PVCs.
            kubeconfig (`str | None`, optional):
                Path to kubeconfig file. ``None`` uses in-cluster
                config.
            image (`str`, defaults to ``"python:3.11-slim"``):
                Container image for workspace Pods.
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
                K8s StorageClass name.
            storage_size (`str`, defaults to ``"1Gi"``):
                PVC size.
            env (`dict[str, str] | None`, optional):
                Environment variables for workspace containers.
            default_mcps (`list[MCPClient] | None`, optional):
                MCPs seeded into new workspaces.
            skill_paths (`list[str] | None`, optional):
                Skill directories seeded into new workspaces.
            ttl (`float`, defaults to `3600.0`):
                Seconds before an idle workspace is evicted.
            sweep_interval (`float`, defaults to `300.0`):
                How often the sweeper runs.
            delete_pod_on_close (`bool`, defaults to ``True``):
                Delete Pods when cached workspaces close. Set to ``False``
                when multiple API workers share the Pod lifecycle.
            delete_pvc_on_close (`bool`, defaults to ``False``):
                Default ``delete_pvc_on_close`` value for newly
                created workspaces. PVC deletion is ignored when
                ``delete_pod_on_close`` is disabled.
        """
        if (
            lease_epoch is not None
            and epoch is not None
            and str(lease_epoch) != str(epoch)
        ):
            raise ValueError("lease_epoch and epoch must match when both are set")
        self._namespace = namespace
        effective_epoch = lease_epoch if lease_epoch is not None else epoch
        self._lease_epoch = (
            effective_epoch
            if effective_epoch is not None
            else os.getenv("AGENTSCOPE_WORKSPACE_EPOCH")
        )
        self._kubeconfig = kubeconfig
        self._image = image
        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = list(image_pull_secrets or [])
        self._resources = resources
        self._node_selector = node_selector
        self._tolerations = tolerations
        self._service_account = service_account
        self._gateway_port = gateway_port
        self._extra_pip = list(extra_pip or [])
        self._storage_class = storage_class
        self._storage_size = storage_size
        self._env = dict(env or {})
        self._default_mcps = list(default_mcps or [])
        self._skill_paths = list(skill_paths or [])
        self._ttl = ttl
        self._sweep_interval = sweep_interval
        self._delete_pod_on_close = delete_pod_on_close
        self._delete_pvc_on_close = delete_pvc_on_close
        super().__init__(isolation=isolation)

        # workspace_id → (workspace, last_access_monotonic)
        self._cache: dict[str, tuple[K8sWorkspace, float]] = {}
        # A per-workspace initialization task prevents duplicate Pods while
        # allowing unrelated workspace IDs to initialize concurrently.  Keep
        # this separate from ``_lock``: holding the cache lock while awaiting
        # ``initialize`` serialized every workspace creation.
        self._pending: dict[str, asyncio.Task[K8sWorkspace]] = {}
        # Number of callers currently waiting on each shared initializer.
        # This lets the completion callback distinguish a cancelled final
        # waiter (whose result must be closed) from a cancellation while
        # another caller can still consume the shared workspace.
        self._pending_waiters: dict[str, int] = {}
        # Workspace IDs whose orphaned initializer result is being closed.
        # New callers wait on this barrier before starting a replacement so
        # an old close cannot tear down a newly reattached Pod/PVC.
        self._closing: dict[str, asyncio.Future[None]] = {}
        self._lock = asyncio.Lock()
        self._sweep_task: asyncio.Task[None] | None = None

    @property
    def delete_pod_on_close(self) -> bool:
        """Whether closing a workspace deletes its shared Pod."""
        return self._delete_pod_on_close

    @delete_pod_on_close.setter
    def delete_pod_on_close(self, value: bool) -> None:
        """Update Pod cleanup policy for subsequently closed workspaces."""
        self._delete_pod_on_close = bool(value)
        # A manager can be rebound into an app after prewarming. Keep any
        # already-cached workspaces aligned with the new lifecycle policy.
        for workspace, _ in self._cache.values():
            workspace._delete_pod_on_close = self._delete_pod_on_close

    # ── workspace construction ────────────────────────────────────

    async def _build_and_start(
        self,
        *,
        workspace_id: str | None,
    ) -> K8sWorkspace:
        """Construct a :class:`K8sWorkspace` and run ``initialize``."""
        ws = K8sWorkspace(
            workspace_id=workspace_id,
            lease_epoch=self._lease_epoch,
            kubeconfig=self._kubeconfig,
            namespace=self._namespace,
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            image_pull_secrets=self._image_pull_secrets,
            resources=self._resources,
            node_selector=self._node_selector,
            tolerations=self._tolerations,
            service_account=self._service_account,
            gateway_port=self._gateway_port,
            extra_pip=self._extra_pip,
            storage_class=self._storage_class,
            storage_size=self._storage_size,
            delete_pod_on_close=self._delete_pod_on_close,
            delete_pvc_on_close=self._delete_pvc_on_close,
            env=self._env,
            default_mcps=self._default_mcps,
            skill_paths=self._skill_paths,
        )
        try:
            await ws.initialize()
        except BaseException:
            # Initialization may have created a Pod/PVC and an API client
            # before failing (or being cancelled).  Always tear those down
            # so failed requests do not leak cluster resources.
            try:
                await ws.close()
            except BaseException:
                pass
            raise
        # A cached workspace is intentionally not probed on every cache hit.
        # Its backend checks the Pod before side effects and reports a
        # confirmed 404/identity/epoch fence here so this manager can evict
        # only that stale object and let the next request reattach cleanly.
        ws._lease_lost_callback = self._evict_confirmed_stale_workspace
        return ws

    async def _evict_confirmed_stale_workspace(self, workspace: K8sWorkspace) -> None:
        """Evict and close one workspace after a confirmed lease loss.

        Identity checking matters because a replacement workspace may already
        have been cached while an older operation was still unwinding.  In
        that case the stale callback must not evict or close the replacement.
        """
        async with self._lock:
            entry = self._cache.get(workspace.workspace_id)
            if entry is None or entry[0] is not workspace:
                return
            self._cache.pop(workspace.workspace_id, None)
            self._start_closing_locked(workspace.workspace_id, workspace)
            closing = self._closing.get(workspace.workspace_id)
        if closing is not None:
            await asyncio.shield(closing)

    # ── public API ────────────────────────────────────────────────

    async def get_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        workspace_id: str | None = None,
    ) -> K8sWorkspace:
        """Return an initialised workspace, reattaching on cache miss.

        Args:
            user_id (`str`):
                Owning user identifier.
            agent_id (`str`):
                Agent identifier.
            session_id (`str`):
                Session identifier (unused; Pods are per-workspace).
            workspace_id (`str | None`, optional):
                Stable workspace identifier — the cache key. When
                unset the manager falls back to
                :meth:`assign_workspace_id`.

        Returns:
            `K8sWorkspace`:
                A live, initialised workspace.
        """
        del session_id

        if not workspace_id:
            workspace_id = await self.assign_workspace_id(
                user_id=user_id,
                agent_id=agent_id,
                session_id="",
            )

        # A cancelled final waiter may leave a completed initializer whose
        # local workspace is still being torn down by the done callback.  Do
        # not create a replacement with the same deterministic Pod name until
        # that teardown has finished.
        while True:
            async with self._lock:
                closing = self._closing.get(workspace_id)
            if closing is None:
                break
            await asyncio.shield(closing)
            async with self._lock:
                if self._closing.get(workspace_id) is closing:
                    self._closing.pop(workspace_id, None)

        async with self._lock:
            cached = self._cache.get(workspace_id)
            if cached is not None:
                ws, _ = cached
                self._cache[workspace_id] = (ws, time.monotonic())
                return ws

            task = self._pending.get(workspace_id)
            if task is None:
                task = asyncio.create_task(
                    self._build_and_start(workspace_id=workspace_id),
                )
                task.add_done_callback(
                    lambda completed, wid=workspace_id: self._on_pending_done(
                        wid,
                        completed,
                    ),
                )
                self._pending[workspace_id] = task
                self._pending_waiters[workspace_id] = 0
            self._pending_waiters[workspace_id] = (
                self._pending_waiters.get(workspace_id, 0) + 1
            )

        # Shield the shared task so cancellation of one request does not
        # cancel initialization for all other callers of this workspace.
        try:
            ws = await asyncio.shield(task)
        except BaseException:
            # Remove failed tasks so a later request can retry.  If this
            # waiter itself was cancelled, leave a still-running task for
            # other callers to await.  The helper also handles the case where
            # cancellation arrives after the initializer has completed.
            await self._release_pending_waiter(workspace_id, task)
            raise
        try:
            async with self._lock:
                if self._pending.get(workspace_id) is task:
                    self._pending_waiters[workspace_id] = max(
                        0,
                        self._pending_waiters.get(workspace_id, 1) - 1,
                    )
                # The task may have completed concurrently with another
                # waiter; only the task that owns the pending entry removes
                # it.
                owns_pending = self._pending.get(workspace_id) is task
                shutdown = False
                if owns_pending:
                    self._pending.pop(workspace_id, None)
                    self._pending_waiters.pop(workspace_id, None)
                cached = self._cache.get(workspace_id)
                if cached is not None:
                    return cached[0]
                if not owns_pending:
                    # close()/close_all() removed the pending entry while the
                    # initializer was finishing. Do not repopulate the cache
                    # after shutdown; release the just-created workspace.
                    shutdown = True
                    close_after = not self._pending_close_claimed(task)
                else:
                    self._cache[workspace_id] = (ws, time.monotonic())
                    close_after = False
        except BaseException:
            # ``shield(task)`` has returned, but cancellation can still be
            # delivered while waiting for the bookkeeping lock.  Make sure
            # this waiter is counted down before propagating cancellation.
            await self._release_pending_waiter(workspace_id, task)
            raise
        if shutdown:
            if close_after:
                await self._safe_close(ws)
            raise asyncio.CancelledError("workspace closed during initialization")
        return ws

    async def _release_pending_waiter(
        self,
        workspace_id: str,
        task: asyncio.Task[K8sWorkspace],
    ) -> None:
        """Drop one waiter and close a completed orphan when it was final."""
        async with self._lock:
            if self._pending.get(workspace_id) is not task:
                return
            remaining = max(
                0,
                self._pending_waiters.get(workspace_id, 1) - 1,
            )
            self._pending_waiters[workspace_id] = remaining
            if not task.done() or remaining != 0:
                return
            self._pending.pop(workspace_id, None)
            self._pending_waiters.pop(workspace_id, None)
            if self._claim_pending_close(task):
                orphan = self._pending_result(task)
                if orphan is not None:
                    self._start_closing_locked(workspace_id, orphan)

    def _on_pending_done(
        self,
        workspace_id: str,
        task: asyncio.Task[K8sWorkspace],
    ) -> None:
        """Reconcile a shared initializer after its last waiter changes.

        A shielded waiter can be cancelled in the same event-loop turn that
        the initializer completes.  The cancelled waiter may therefore miss
        the ``task.done()`` check in :meth:`get_workspace`.  Reconcile from a
        done callback so a successful result is either cached for remaining
        waiters or closed when the cancellation was the final one.
        """
        try:
            reconcile = asyncio.create_task(
                self._reconcile_pending_done(workspace_id, task),
            )
        except RuntimeError:
            # The event loop is shutting down.  The manager's normal
            # close_all path will still handle tasks that it owns.
            return
        reconcile.add_done_callback(self._consume_reconcile_failure)

    async def _reconcile_pending_done(
        self,
        workspace_id: str,
        task: asyncio.Task[K8sWorkspace],
    ) -> None:
        """Adopt or close a completed pending initializer exactly once."""
        # Let shielded waiter continuations observe the completion first.
        # This closes the narrow case where every waiter is cancelled in the
        # same turn after the initializer task becomes done.
        await asyncio.sleep(0)
        workspace = self._pending_result(task)
        async with self._lock:
            owns_pending = self._pending.get(workspace_id) is task
            cached = self._cache.get(workspace_id)
            if owns_pending:
                self._pending.pop(workspace_id, None)
                waiters = self._pending_waiters.pop(workspace_id, 0)
                if workspace is None:
                    return
                if cached is not None:
                    if cached[0] is workspace:
                        return
                    # A replacement was cached while this initializer was
                    # finishing.  Do not evict the replacement.
                    self._start_closing_locked(workspace_id, workspace)
                elif waiters > 0:
                    self._cache[workspace_id] = (workspace, time.monotonic())
                elif self._claim_pending_close(task):
                    self._start_closing_locked(workspace_id, workspace)
            elif workspace is not None:
                # close()/close_all() or a waiter already removed the task.
                # A matching cache entry owns this result; otherwise close it
                # unless shutdown already claimed that responsibility.
                if cached is not None and cached[0] is workspace:
                    return
                if not self._pending_close_claimed(task):
                    self._claim_pending_close(task)
                    self._start_closing_locked(workspace_id, workspace)

    def _start_closing_locked(
        self,
        workspace_id: str,
        workspace: K8sWorkspace,
    ) -> None:
        """Start an orphan close while publishing a replacement barrier.

        The caller must hold ``_lock``.  Publishing the future before the
        lock is released makes replacement initialization wait until this
        workspace's backend teardown is complete.
        """
        existing = self._closing.get(workspace_id)
        if existing is not None:
            if not existing.done():
                return
            self._closing.pop(workspace_id, None)
        close_task = asyncio.create_task(self._safe_close(workspace))
        self._closing[workspace_id] = close_task
        close_task.add_done_callback(
            lambda completed, wid=workspace_id: self._finish_closing(
                wid,
                completed,
            ),
        )

    async def _close_pending_result(
        self,
        task: asyncio.Task[K8sWorkspace],
    ) -> None:
        """Wait for a detached initializer and close a successful result.

        The close task is published in ``_closing`` before its initializer is
        cancelled.  This is important during manager shutdown: a new caller
        must not start a replacement for the same deterministic Pod name
        while the old initializer is still unwinding (or while its successful
        result is being torn down).
        """
        await asyncio.gather(task, return_exceptions=True)
        workspace = self._pending_result(task)
        if workspace is not None:
            await self._safe_close(workspace)

    def _start_pending_close_locked(
        self,
        workspace_id: str,
        task: asyncio.Task[K8sWorkspace],
    ) -> None:
        """Publish a close barrier for a detached initializer.

        The caller must hold ``_lock``.  Unlike ``_start_closing_locked``,
        this helper waits for the initializer itself before closing its
        successful result, allowing ``close_all`` to publish the barrier
        before releasing the cache lock.
        """
        existing = self._closing.get(workspace_id)
        if existing is not None and not existing.done():
            return
        if existing is not None:
            self._closing.pop(workspace_id, None)
        close_task = asyncio.create_task(self._close_pending_result(task))
        self._closing[workspace_id] = close_task
        close_task.add_done_callback(
            lambda completed, wid=workspace_id: self._finish_closing(
                wid,
                completed,
            ),
        )

    def _finish_closing(
        self,
        workspace_id: str,
        task: asyncio.Task[None],
    ) -> None:
        """Drop a completed replacement barrier without a lock round-trip."""
        if self._closing.get(workspace_id) is task:
            self._closing.pop(workspace_id, None)
        self._consume_reconcile_failure(task)

    @staticmethod
    def _consume_reconcile_failure(task: asyncio.Task[None]) -> None:
        """Consume errors from the asynchronous pending reconciliation."""
        if task.cancelled():
            return
        try:
            task.exception()
        except BaseException:
            return

    @staticmethod
    def _pending_close_claimed(task: asyncio.Task[K8sWorkspace]) -> bool:
        """Return whether a manager shutdown already claimed this task.

        A waiter can resume after :meth:`close_all` has removed its pending
        initializer.  Keeping the claim on the task prevents that waiter
        from closing the same workspace a second time.
        """
        return bool(getattr(task, "_agentscope_close_claimed", False))

    @classmethod
    def _claim_pending_close(cls, task: asyncio.Task[K8sWorkspace]) -> bool:
        """Claim ownership of closing a pending task's result."""
        if cls._pending_close_claimed(task):
            return False
        setattr(task, "_agentscope_close_claimed", True)
        return True

    @staticmethod
    def _pending_result(
        task: asyncio.Task[K8sWorkspace],
    ) -> K8sWorkspace | None:
        """Return a successful pending workspace, if one exists."""
        if task.cancelled():
            return None
        try:
            return task.result()
        except BaseException:
            return None

    @deprecated(
        "K8sWorkspaceManager.create_workspace is deprecated; "
        "use get_workspace(workspace_id=None) instead.",
        category=None,
    )
    async def create_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
    ) -> K8sWorkspace:
        """Build a brand-new workspace and track it.

        .. deprecated::
            Use :meth:`get_workspace` with ``workspace_id=None`` — it
            falls back to :meth:`assign_workspace_id` under the
            manager's isolation policy and reuses the cache path.

        Args:
            user_id (`str`):
                Owning user identifier.
            agent_id (`str`):
                Agent identifier.
            session_id (`str`):
                Session identifier (unused).

        Returns:
            `K8sWorkspace`:
                The newly built workspace, already initialised.
        """
        del session_id, user_id, agent_id

        ws = await self._build_and_start(workspace_id=None)
        async with self._lock:
            self._cache[ws.workspace_id] = (ws, time.monotonic())
        return ws

    async def close(self, workspace_id: str) -> None:
        """Close and evict a single workspace.

        Args:
            workspace_id (`str`):
                The workspace to close.
        """
        async with self._lock:
            entry = self._cache.pop(workspace_id, None)
            pending = self._pending.pop(workspace_id, None)
            self._pending_waiters.pop(workspace_id, None)
            if pending is not None:
                self._claim_pending_close(pending)
                self._start_pending_close_locked(workspace_id, pending)
            elif entry is not None:
                self._start_closing_locked(workspace_id, entry[0])
            closing = self._closing.get(workspace_id)
        if pending is not None and not pending.done():
            pending.cancel()
        if pending is not None:
            await asyncio.shield(
                asyncio.gather(pending, return_exceptions=True),
            )
        if closing is not None:
            await asyncio.shield(closing)
            async with self._lock:
                if self._closing.get(workspace_id) is closing:
                    self._closing.pop(workspace_id, None)

    async def close_all(self) -> None:
        """Close every cached workspace in parallel."""
        async with self._lock:
            entries = list(self._cache.items())
            self._cache.clear()
            for workspace_id, (workspace, _) in entries:
                self._start_closing_locked(workspace_id, workspace)
            pending_items = list(self._pending.items())
            pending = [task for _, task in pending_items]
            self._pending.clear()
            self._pending_waiters.clear()
            for workspace_id, task in pending_items:
                self._claim_pending_close(task)
                self._start_pending_close_locked(workspace_id, task)
            # Include barriers published above in the snapshot.  New callers
            # observe these entries after the lock is released and wait until
            # the corresponding initializer result has been closed.
            closing = list(self._closing.items())
        for task in pending:
            if not task.done():
                task.cancel()
        if pending:
            await asyncio.shield(
                asyncio.gather(*pending, return_exceptions=True),
            )
        if closing:
            await asyncio.shield(
                asyncio.gather(
                    *(task for _, task in closing),
                    return_exceptions=True,
                ),
            )
            async with self._lock:
                for workspace_id, task in closing:
                    if self._closing.get(workspace_id) is task:
                        self._closing.pop(workspace_id, None)

    # ── async context manager ─────────────────────────────────────

    async def __aenter__(self) -> Self:
        """Start the TTL sweeper task."""
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._sweep_loop())
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Stop the TTL sweeper, then close every cached workspace."""
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except (asyncio.CancelledError, Exception):
                pass
            self._sweep_task = None
        await self.close_all()

    # ── background sweeper ───────────────────────────────────────

    async def _sweep_loop(self) -> None:
        """Periodically evict idle workspaces."""
        while True:
            try:
                await asyncio.sleep(self._sweep_interval)
            except asyncio.CancelledError:
                return
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("K8s workspace sweeper tick failed")

    async def _sweep_once(self) -> None:
        """One sweeper tick: evict expired entries and close them."""
        now = time.monotonic()
        async with self._lock:
            expired_ids = [
                wid
                for wid, (_, ts) in self._cache.items()
                if now - ts > self._ttl
            ]
            evicted = []
            for workspace_id in expired_ids:
                workspace, _ = self._cache.pop(workspace_id)
                self._start_closing_locked(workspace_id, workspace)
                closing = self._closing.get(workspace_id)
                if closing is not None:
                    evicted.append(closing)
        if not evicted:
            return
        await asyncio.shield(
            asyncio.gather(*evicted, return_exceptions=True),
        )

    @staticmethod
    async def _safe_close(ws: K8sWorkspace) -> None:
        """Close a workspace, logging any failure instead of raising.

        Args:
            ws (`K8sWorkspace`):
                The workspace to close.
        """
        try:
            await ws.close()
        except Exception:
            logger.exception(
                "Failed to close K8sWorkspace %s",
                ws.workspace_id,
            )
