# -*- coding: utf-8 -*-
"""Unit coverage for create_app's multi-node fail-closed checks."""
import os
import tempfile
from unittest import TestCase
from unittest.mock import patch

from agentscope.app._app import _validate_multi_node_configuration
from agentscope.app.message_bus import InMemoryMessageBus
from agentscope.app.workspace_manager import (
    K8sWorkspaceManager,
    LocalWorkspaceManager,
)


class MultiNodeGuardrailsTest(TestCase):
    """Local process resources must not pass multi-node validation."""

    def setUp(self) -> None:
        self._workdir = tempfile.TemporaryDirectory()
        self._manager = LocalWorkspaceManager(self._workdir.name)

    def tearDown(self) -> None:
        self._workdir.cleanup()

    def _validate(self, **kwargs: object) -> str | None:
        values: dict[str, object] = {
            "message_bus": InMemoryMessageBus(),
            "workspace_manager": self._manager,
            "multi_node": False,
            "worker_role": None,
            "enable_index_worker": False,
            "enable_channel_worker": False,
            "enable_scheduler": False,
            "knowledge_base_manager": None,
            "download_secret": None,
        }
        values.update(kwargs)
        return _validate_multi_node_configuration(**values)  # type: ignore[arg-type]

    def test_local_manager_is_rejected_before_secret_check(self) -> None:
        with self.assertRaisesRegex(ValueError, "LocalWorkspaceManager"):
            self._validate(
                multi_node=True,
                worker_role="api",
                download_secret="shared",
            )

    def test_process_local_bus_is_rejected(self) -> None:
        class DistributedWorkspaceManager:
            pass

        with self.assertRaisesRegex(ValueError, "InMemoryMessageBus"):
            self._validate(
                multi_node=True,
                worker_role="api",
                workspace_manager=DistributedWorkspaceManager(),
                download_secret="shared",
            )

    def test_multi_node_requires_explicit_worker_role(self) -> None:
        class DistributedWorkspaceManager:
            pass

        with self.assertRaisesRegex(ValueError, "worker_role"):
            self._validate(
                multi_node=True,
                worker_role=None,
                workspace_manager=DistributedWorkspaceManager(),
                message_bus=object(),
                # These are create_app's defaults. They must not silently
                # start singleton workers on every replica.
                enable_index_worker=True,
                enable_channel_worker=True,
                enable_scheduler=True,
                download_secret="shared",
            )

    def test_shared_secret_can_come_from_environment(self) -> None:
        class DistributedWorkspaceManager:
            pass

        with patch.dict(os.environ, {"AGENTSCOPE_DOWNLOAD_SECRET": "shared"}):
            self.assertEqual(
                _validate_multi_node_configuration(
                    message_bus=object(),
                    workspace_manager=DistributedWorkspaceManager(),
                    multi_node=True,
                    worker_role="api",
                    enable_index_worker=False,
                    enable_channel_worker=False,
                    enable_scheduler=False,
                    knowledge_base_manager=None,
                    download_secret=None,
                ),
                "shared",
            )

    def test_worker_role_all_accepts_matching_flags(self) -> None:
        class DistributedWorkspaceManager:
            pass

        self.assertEqual(
            _validate_multi_node_configuration(
                message_bus=object(),
                workspace_manager=DistributedWorkspaceManager(),
                multi_node=True,
                worker_role="all",
                enable_index_worker=False,
                enable_channel_worker=True,
                enable_scheduler=True,
                knowledge_base_manager=None,
                download_secret="shared",
            ),
            "shared",
        )

    def test_worker_role_must_match_flags(self) -> None:
        with self.assertRaisesRegex(ValueError, "worker_role"):
            self._validate(worker_role="api", enable_scheduler=True)

    def test_k8s_api_worker_keeps_shared_pod_alive(self) -> None:
        manager = K8sWorkspaceManager()
        _validate_multi_node_configuration(
            message_bus=object(),
            workspace_manager=manager,
            multi_node=True,
            worker_role="api",
            enable_index_worker=False,
            enable_channel_worker=False,
            enable_scheduler=False,
            knowledge_base_manager=None,
            download_secret="shared",
        )
        self.assertFalse(manager.delete_pod_on_close)

