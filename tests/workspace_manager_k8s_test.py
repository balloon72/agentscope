# -*- coding: utf-8 -*-
"""Tests for :class:`K8sWorkspaceManager` cache initialization."""

import asyncio
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import patch

from agentscope.app.workspace_manager import K8sWorkspaceManager


class _FakeWorkspace:
    created: list["_FakeWorkspace"] = []
    active = 0
    max_active = 0

    def __init__(self, **kwargs: object) -> None:
        self.workspace_id = str(kwargs["workspace_id"])
        self.closed = False
        type(self).created.append(self)

    async def initialize(self) -> None:
        type(self).active += 1
        type(self).max_active = max(type(self).max_active, type(self).active)
        await asyncio.sleep(0.01)
        type(self).active -= 1

    async def close(self) -> None:
        self.closed = True


class _BlockingWorkspace(_FakeWorkspace):
    started = asyncio.Event()

    async def initialize(self) -> None:
        type(self).started.set()
        await asyncio.Event().wait()


class TestK8sWorkspaceManager(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        _FakeWorkspace.created.clear()
        _FakeWorkspace.active = 0
        _FakeWorkspace.max_active = 0
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _FakeWorkspace,
        )
        self.patch.start()

    async def asyncTearDown(self) -> None:
        self.patch.stop()

    async def test_same_workspace_initializes_once(self) -> None:
        manager = K8sWorkspaceManager()
        results = await asyncio.gather(
            *(manager.get_workspace("u", "a", str(i), "same") for i in range(8)),
        )

        self.assertEqual(len(_FakeWorkspace.created), 1)
        self.assertTrue(all(result is results[0] for result in results))

    async def test_different_workspaces_initialize_concurrently(self) -> None:
        manager = K8sWorkspaceManager()
        await asyncio.gather(
            manager.get_workspace("u", "a", "s1", "one"),
            manager.get_workspace("u", "a", "s2", "two"),
        )

        self.assertEqual(len(_FakeWorkspace.created), 2)
        self.assertEqual(_FakeWorkspace.max_active, 2)

    async def test_close_all_cancels_pending_initializers(self) -> None:
        _BlockingWorkspace.started = asyncio.Event()
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _BlockingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        waiter = asyncio.create_task(
            manager.get_workspace("u", "a", "s", "pending"),
        )
        await asyncio.wait_for(_BlockingWorkspace.started.wait(), timeout=1)
        await manager.close_all()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(manager._pending, {})

