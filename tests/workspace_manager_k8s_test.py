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
        self.close_calls = 0
        type(self).created.append(self)

    async def initialize(self) -> None:
        type(self).active += 1
        type(self).max_active = max(type(self).max_active, type(self).active)
        await asyncio.sleep(0.01)
        type(self).active -= 1

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _BlockingWorkspace(_FakeWorkspace):
    started = asyncio.Event()

    async def initialize(self) -> None:
        type(self).started.set()
        await asyncio.Event().wait()


class _FailingWorkspace(_FakeWorkspace):
    async def initialize(self) -> None:
        raise RuntimeError("boom")


class _CompletingWorkspace(_FakeWorkspace):
    started = asyncio.Event()
    release = asyncio.Event()

    async def initialize(self) -> None:
        type(self).started.set()
        await type(self).release.wait()


class _BlockingCloseWorkspace(_CompletingWorkspace):
    close_started = asyncio.Event()
    close_release = asyncio.Event()

    async def close(self) -> None:
        type(self).close_started.set()
        await type(self).close_release.wait()
        await super().close()


class _LeaseReportingWorkspace(_FakeWorkspace):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._lease_lost_callback = None

    async def report_confirmed_lease_loss(self) -> None:
        assert self._lease_lost_callback is not None
        await self._lease_lost_callback(self)


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

    async def test_failed_initialization_closes_workspace(self) -> None:
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _FailingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        with self.assertRaisesRegex(RuntimeError, "boom"):
            await manager.get_workspace("u", "a", "s", "failed")
        self.assertEqual(len(_FailingWorkspace.created), 1)
        self.assertTrue(_FailingWorkspace.created[0].closed)
        self.assertEqual(manager._cache, {})

    async def test_close_all_closes_successful_initializer_after_waiter_cancel(
        self,
    ) -> None:
        _CompletingWorkspace.started = asyncio.Event()
        _CompletingWorkspace.release = asyncio.Event()
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _CompletingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        waiter = asyncio.create_task(
            manager.get_workspace("u", "a", "s", "completed"),
        )
        await asyncio.wait_for(_CompletingWorkspace.started.wait(), timeout=1)
        pending = manager._pending["completed"]
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        _CompletingWorkspace.release.set()
        workspace = await pending

        await manager.close_all()

        self.assertTrue(workspace.closed)
        self.assertEqual(workspace.close_calls, 1)
        for _ in range(5):
            if not manager._closing:
                break
            await asyncio.sleep(0)
        self.assertEqual(manager._pending, {})
        self.assertEqual(manager._cache, {})

    async def test_close_closes_successful_initializer_after_waiter_cancel(self) -> None:
        _CompletingWorkspace.started = asyncio.Event()
        _CompletingWorkspace.release = asyncio.Event()
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _CompletingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        waiter = asyncio.create_task(
            manager.get_workspace("u", "a", "s", "completed"),
        )
        await asyncio.wait_for(_CompletingWorkspace.started.wait(), timeout=1)
        pending = manager._pending["completed"]
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        _CompletingWorkspace.release.set()
        workspace = await pending

        await manager.close("completed")

        self.assertTrue(workspace.closed)
        self.assertEqual(workspace.close_calls, 1)
        self.assertEqual(manager._pending, {})
        self.assertEqual(manager._cache, {})

    async def test_final_waiter_cancel_closes_late_initializer(self) -> None:
        """A cancelled final waiter must not strand a successful initializer."""
        _CompletingWorkspace.started = asyncio.Event()
        _CompletingWorkspace.release = asyncio.Event()
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _CompletingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        waiter = asyncio.create_task(
            manager.get_workspace("u", "a", "s", "late"),
        )
        await asyncio.wait_for(_CompletingWorkspace.started.wait(), timeout=1)
        pending = manager._pending["late"]

        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(manager._pending_waiters["late"], 0)

        # Complete after the cancellation.  The done callback, rather than a
        # waiter, now owns the result and must close it exactly once.
        workspace = _CompletingWorkspace.created[-1]
        _CompletingWorkspace.release.set()
        await pending
        for _ in range(5):
            if workspace.closed:
                break
            await asyncio.sleep(0)

        self.assertTrue(workspace.closed)
        self.assertEqual(workspace.close_calls, 1)
        self.assertEqual(manager._pending, {})
        self.assertEqual(manager._pending_waiters, {})
        self.assertEqual(manager._cache, {})
        for _ in range(5):
            if not manager._closing:
                break
            await asyncio.sleep(0)
        self.assertEqual(manager._closing, {})

    async def test_cancelled_waiter_does_not_close_shared_initializer(self) -> None:
        """A remaining waiter still receives a result after one cancels."""
        _CompletingWorkspace.started = asyncio.Event()
        _CompletingWorkspace.release = asyncio.Event()
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _CompletingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        first = asyncio.create_task(
            manager.get_workspace("u", "a", "s1", "shared"),
        )
        await asyncio.wait_for(_CompletingWorkspace.started.wait(), timeout=1)
        second = asyncio.create_task(
            manager.get_workspace("u", "a", "s2", "shared"),
        )
        await asyncio.sleep(0)

        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertEqual(manager._pending_waiters["shared"], 1)

        _CompletingWorkspace.release.set()
        workspace = await second

        self.assertFalse(workspace.closed)
        self.assertIs(manager._cache["shared"][0], workspace)
        await manager.close_all()

    async def test_late_orphan_close_blocks_same_id_replacement(self) -> None:
        """A replacement cannot reattach while the orphan is closing."""
        _BlockingCloseWorkspace.started = asyncio.Event()
        _BlockingCloseWorkspace.release = asyncio.Event()
        _BlockingCloseWorkspace.close_started = asyncio.Event()
        _BlockingCloseWorkspace.close_release = asyncio.Event()
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _BlockingCloseWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        waiter = asyncio.create_task(
            manager.get_workspace("u", "a", "s", "raced"),
        )
        await asyncio.wait_for(_BlockingCloseWorkspace.started.wait(), timeout=1)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter

        _BlockingCloseWorkspace.release.set()
        old = _BlockingCloseWorkspace.created[-1]
        pending = manager._pending["raced"]
        await pending
        await asyncio.wait_for(
            _BlockingCloseWorkspace.close_started.wait(),
            timeout=1,
        )

        replacement_task = asyncio.create_task(
            manager.get_workspace("u", "a", "s2", "raced"),
        )
        await asyncio.sleep(0)
        self.assertFalse(replacement_task.done())
        self.assertEqual(len(_BlockingCloseWorkspace.created), 1)

        _BlockingCloseWorkspace.close_release.set()
        replacement = await replacement_task

        self.assertTrue(old.closed)
        self.assertIsNot(old, replacement)
        self.assertFalse(replacement.closed)
        self.assertEqual(manager._closing, {})
        await manager.close_all()

    async def test_confirmed_lease_loss_evicts_only_the_stale_workspace(self) -> None:
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _LeaseReportingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        workspace = await manager.get_workspace("u", "a", "s", "stale")

        await workspace.report_confirmed_lease_loss()

        self.assertEqual(manager._cache, {})
        self.assertTrue(workspace.closed)
        self.assertEqual(workspace.close_calls, 1)

    async def test_stale_callback_cannot_evict_replacement_workspace(self) -> None:
        self.patch.stop()
        self.patch = patch(
            "agentscope.app.workspace_manager._k8s_workspace_manager.K8sWorkspace",
            _LeaseReportingWorkspace,
        )
        self.patch.start()
        manager = K8sWorkspaceManager()
        stale = await manager.get_workspace("u", "a", "s", "same")
        replacement = await manager.get_workspace("u", "a", "s", "same")
        self.assertIs(stale, replacement)

        # Simulate a replacement entering the cache after the stale request
        # started unwinding.  The identity check must preserve it.
        replacement = _LeaseReportingWorkspace(workspace_id="same")
        async with manager._lock:
            manager._cache["same"] = (replacement, 0.0)
        await stale.report_confirmed_lease_loss()

        self.assertIs(manager._cache["same"][0], replacement)
        self.assertFalse(replacement.closed)
        self.assertFalse(stale.closed)

