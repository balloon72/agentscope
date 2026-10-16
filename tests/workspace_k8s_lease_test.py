# -*- coding: utf-8 -*-
"""Unit coverage for K8s workspace lease metadata and fencing."""
from types import SimpleNamespace
import unittest
from unittest import IsolatedAsyncioTestCase, TestCase

from agentscope.workspace import K8sWorkspace, K8sWorkspaceLeaseLostError
from agentscope.workspace._k8s._constants import (
    WORKSPACE_EPOCH_ENV,
    WORKSPACE_EPOCH_LABEL,
)

try:
    from kubernetes_asyncio.client.rest import ApiException
except ImportError:  # pragma: no cover - optional workspace-k8s extra
    ApiException = None  # type: ignore[assignment,misc]


def _pod(*, epoch_label: str | None, epoch_env: str | None) -> object:
    """Build the tiny Pod shape consumed by ``_validate_pod_lease``."""
    return SimpleNamespace(
        metadata=SimpleNamespace(
            labels=(
                {WORKSPACE_EPOCH_LABEL: epoch_label}
                if epoch_label is not None
                else {}
            ),
        ),
        spec=SimpleNamespace(
            containers=[
                SimpleNamespace(
                    env=(
                        [
                            SimpleNamespace(
                                name=WORKSPACE_EPOCH_ENV,
                                value=epoch_env,
                            ),
                        ]
                        if epoch_env is not None
                        else []
                    ),
                ),
            ],
        ),
    )


class K8sWorkspaceLeaseTest(TestCase):
    """The control-plane and in-container epochs must agree."""

    def test_matching_label_and_environment_are_accepted(self) -> None:
        workspace = K8sWorkspace(workspace_id="lease-test", lease_epoch=7)
        workspace._pod_name = "as-ws-lease-test"  # pylint: disable=protected-access
        workspace._validate_pod_lease(  # pylint: disable=protected-access
            _pod(epoch_label="7", epoch_env="7"),
        )

    def test_mismatched_epoch_is_fenced(self) -> None:
        workspace = K8sWorkspace(workspace_id="lease-test", lease_epoch="new")
        workspace._pod_name = "as-ws-lease-test"  # pylint: disable=protected-access
        with self.assertRaises(K8sWorkspaceLeaseLostError):
            workspace._validate_pod_lease(  # pylint: disable=protected-access
                _pod(epoch_label="old", epoch_env="old"),
            )

    def test_missing_environment_is_fenced_even_when_label_matches(self) -> None:
        workspace = K8sWorkspace(workspace_id="lease-test", lease_epoch="1")
        workspace._pod_name = "as-ws-lease-test"  # pylint: disable=protected-access
        with self.assertRaises(K8sWorkspaceLeaseLostError):
            workspace._validate_pod_lease(  # pylint: disable=protected-access
                _pod(epoch_label="1", epoch_env=None),
            )


@unittest.skipUnless(
    ApiException is not None,
    "requires the optional workspace-k8s dependency",
)
class K8sWorkspaceDeleteFenceTest(IsolatedAsyncioTestCase):
    """Pod/PVC cleanup must use Kubernetes compare-and-delete semantics."""

    async def test_close_deletes_only_the_checked_pod_version(self) -> None:
        """A replacement Pod cannot be deleted after a stale read."""

        class FakeApiClient:
            async def close(self) -> None:
                pass

        class FakeV1:
            def __init__(self) -> None:
                self.delete_body = None

            async def read_namespaced_pod(self, name: str, namespace: str):
                del name, namespace
                return _pod_with_metadata(epoch="7", resource_version="41")

            async def delete_namespaced_pod(
                self,
                name: str,
                namespace: str,
                *,
                body: object,
            ) -> None:
                del name, namespace
                self.delete_body = body

        workspace = K8sWorkspace(
            workspace_id="delete-fence",
            lease_epoch="7",
        )
        workspace._pod_name = "as-ws-delete-fence"  # pylint: disable=protected-access
        fake_v1 = FakeV1()
        workspace._v1 = fake_v1  # pylint: disable=protected-access
        workspace._api_client = FakeApiClient()  # pylint: disable=protected-access

        await workspace._teardown_backend()  # pylint: disable=protected-access

        body = fake_v1.delete_body
        self.assertIsNotNone(body)
        self.assertEqual(
            body.preconditions.resource_version,  # type: ignore[union-attr]
            "41",
        )

    async def test_conflict_fences_cleanup_and_skips_pvc_delete(self) -> None:
        """HTTP 409 from a stale Pod delete must not cascade to the PVC."""

        class FakeApiClient:
            async def close(self) -> None:
                pass

        class FakeV1:
            def __init__(self) -> None:
                self.pvc_delete_calls = 0

            async def read_namespaced_pod(self, name: str, namespace: str):
                del name, namespace
                return _pod_with_metadata(epoch="7", resource_version="41")

            async def delete_namespaced_pod(self, name: str, namespace: str, *, body: object):
                del name, namespace, body
                raise ApiException(status=409, reason="resourceVersion changed")

            async def delete_namespaced_persistent_volume_claim(
                self,
                name: str,
                namespace: str,
                *,
                body: object,
            ) -> None:
                del name, namespace, body
                self.pvc_delete_calls += 1

        workspace = K8sWorkspace(
            workspace_id="delete-conflict",
            lease_epoch="7",
            delete_pvc_on_close=True,
        )
        workspace._pod_name = "as-ws-delete-conflict"  # pylint: disable=protected-access
        fake_v1 = FakeV1()
        workspace._v1 = fake_v1  # pylint: disable=protected-access
        workspace._api_client = FakeApiClient()  # pylint: disable=protected-access

        await workspace._teardown_backend()  # pylint: disable=protected-access

        self.assertEqual(fake_v1.pvc_delete_calls, 0)

    async def test_stale_owner_cannot_recreate_terminating_pvc(self) -> None:
        """A terminating PVC from a newer epoch must fence old init."""

        class FakeV1:
            async def read_namespaced_persistent_volume_claim(
                self,
                name: str,
                namespace: str,
            ) -> object:
                del name, namespace
                return SimpleNamespace(
                    metadata=SimpleNamespace(
                        labels={WORKSPACE_EPOCH_LABEL: "new"},
                        deletion_timestamp="now",
                    ),
                )

        workspace = K8sWorkspace(
            workspace_id="pvc-fence",
            lease_epoch="old",
        )
        workspace._pod_name = "as-ws-pvc-fence"  # pylint: disable=protected-access
        workspace._v1 = FakeV1()  # pylint: disable=protected-access

        with self.assertRaises(K8sWorkspaceLeaseLostError):
            await workspace._ensure_pvc()  # pylint: disable=protected-access


def _pod_with_metadata(*, epoch: str, resource_version: str) -> object:
    """Build a fake Pod including the metadata needed by cleanup tests."""
    pod = _pod(epoch_label=epoch, epoch_env=epoch)
    pod.metadata.resource_version = resource_version  # type: ignore[attr-defined]
    return pod

