"""文件后端重启后必须保留未整体结算任务的失败事实。"""

import json

import pytest

from openviking.service.task_domain import TaskStatus, WorkState
from openviking.service.task_store import PersistentTaskStore
from openviking.service.task_tracker import TaskTracker
from tests.test_task_tracker import _FakeAgfs

OWNER = {"account_id": "restart-check", "user_id": "owner"}


async def pending_task_with_failed_child(task_type="content_write"):
    agfs = _FakeAgfs()
    tracker = TaskTracker(PersistentTaskStore(agfs))
    await tracker.create(task_type, task_id="restart-task", **OWNER)
    await tracker.start("restart-task", **OWNER)
    await tracker.register_work("restart-task", "failed-child", "Semantic", **OWNER)
    await tracker.register_work("restart-task", "pending-child", "Embedding", **OWNER)
    await tracker.complete("restart-task", {"accepted": True}, **OWNER)
    await tracker.mark_work_failed(
        "restart-task", "failed-child", "synthetic child failure", **OWNER
    )
    assert (await tracker.get("restart-task", **OWNER)).status is TaskStatus.RUNNING
    return agfs


@pytest.mark.asyncio
async def test_failed_child_state_survives_before_parent_finishes():
    agfs = await pending_task_with_failed_child()
    restarted = TaskTracker(PersistentTaskStore(agfs))
    await restarted.get("restart-task", **OWNER)
    assert (
        await restarted.get_work_state("restart-task", "failed-child", **OWNER) is WorkState.FAILED
    )


@pytest.mark.asyncio
async def test_recovered_pending_child_cannot_turn_previous_failure_into_success():
    agfs = await pending_task_with_failed_child()
    restarted = TaskTracker(PersistentTaskStore(agfs))
    await restarted.reconcile_delivery("restart-task", "pending-child", "Embedding", **OWNER)
    await restarted.mark_work_done("restart-task", "pending-child", **OWNER)
    final = await restarted.get("restart-task", **OWNER)
    assert final.status is TaskStatus.FAILED
    assert final.error == "synthetic child failure"
    assert final.result["queue_status"]["Semantic"]["error_count"] == 1


@pytest.mark.asyncio
async def test_repeated_reload_does_not_duplicate_failure_or_cross_owner():
    agfs = await pending_task_with_failed_child()
    for _ in range(2):
        restarted = TaskTracker(PersistentTaskStore(agfs))
        await restarted.get("restart-task", **OWNER)
        status = await restarted.build_queue_status("restart-task", **OWNER)
        assert status["Semantic"]["error_count"] == 1
        assert (
            await restarted.get("restart-task", account_id="another-account", user_id="owner")
            is None
        )
    await restarted.reconcile_delivery("restart-task", "pending-child", "Embedding", **OWNER)
    await restarted.mark_work_done("restart-task", "pending-child", **OWNER)
    assert (await restarted.get("restart-task", **OWNER)).status is TaskStatus.FAILED


@pytest.mark.asyncio
async def test_failure_checkpoint_write_failure_does_not_publish_memory(monkeypatch):
    agfs = _FakeAgfs()
    store = PersistentTaskStore(agfs)
    tracker = TaskTracker(store)
    await tracker.create("content_write", task_id="restart-task", **OWNER)
    for work in ["failed-child", "pending-child"]:
        await tracker.register_work("restart-task", work, "Semantic", **OWNER)
    await tracker.complete("restart-task", {"accepted": True}, **OWNER)
    write = store._write_task

    async def reject_write(_task):
        raise OSError("synthetic persistence failure")

    monkeypatch.setattr(store, "_write_task", reject_write)
    with pytest.raises(OSError, match="synthetic persistence failure"):
        await tracker.mark_work_failed("restart-task", "failed-child", "failure", **OWNER)
    assert (
        await tracker.get_work_state("restart-task", "failed-child", **OWNER) is WorkState.PENDING
    )
    monkeypatch.setattr(store, "_write_task", write)
    await tracker.mark_work_failed("restart-task", "failed-child", "failure", **OWNER)
    restarted = TaskTracker(PersistentTaskStore(agfs))
    await restarted.reconcile_delivery("restart-task", "pending-child", "Semantic", **OWNER)
    await restarted.mark_work_done("restart-task", "pending-child", **OWNER)
    task = await restarted.get("restart-task", **OWNER)
    assert task.status is TaskStatus.FAILED
    assert task.result["queue_status"]["Semantic"]["error_count"] == 1


@pytest.mark.asyncio
async def test_cancellation_still_wins_and_keeps_failure_in_queue_history():
    agfs = await pending_task_with_failed_child("add_resource")
    restarted = TaskTracker(PersistentTaskStore(agfs))
    await restarted.reconcile_delivery("restart-task", "pending-child", "Embedding", **OWNER)
    await restarted.cancel("restart-task", **OWNER)
    await restarted.mark_work_done("restart-task", "pending-child", **OWNER)
    task = await restarted.get("restart-task", **OWNER)
    assert task.status is TaskStatus.CANCELLED
    assert task.result["queue_status"]["Semantic"]["error_count"] == 1


@pytest.mark.asyncio
async def test_explicit_discard_does_not_restore_removed_failure():
    agfs = await pending_task_with_failed_child()
    tracker = TaskTracker(PersistentTaskStore(agfs))
    await tracker.reconcile_delivery("restart-task", "pending-child", "Embedding", **OWNER)
    await tracker.discard_work("restart-task", "failed-child", **OWNER)
    restarted = TaskTracker(PersistentTaskStore(agfs))
    await restarted.reconcile_delivery("restart-task", "pending-child", "Embedding", **OWNER)
    assert await restarted.get_work_state("restart-task", "failed-child", **OWNER) is None
    await restarted.mark_work_done("restart-task", "pending-child", **OWNER)
    assert (await restarted.get("restart-task", **OWNER)).status is TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_invalid_checkpoint_does_not_poison_cache():
    agfs = await pending_task_with_failed_child()
    path = next(path for path in agfs.files if path.endswith("restart-task.json"))
    payload = json.loads(agfs.files[path])
    payload["_file_failed_works_v1"][0]["user_id"] = "different-owner"
    agfs.files[path] = json.dumps(payload)
    restarted = TaskTracker(PersistentTaskStore(agfs))
    for _ in range(2):
        with pytest.raises(ValueError, match="checkpoint owner or state"):
            await restarted.get("restart-task", **OWNER)
