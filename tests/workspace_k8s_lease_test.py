# -*- coding: utf-8 -*-
"""Unit coverage for K8s workspace lease metadata and fencing."""
from types import SimpleNamespace
from unittest import TestCase

from agentscope.workspace import K8sWorkspace, K8sWorkspaceLeaseLostError
from agentscope.workspace._k8s._constants import (
    WORKSPACE_EPOCH_ENV,
    WORKSPACE_EPOCH_LABEL,
)


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

