"""任务文件损坏、原子替换及失败检查点的合成回归。"""

import asyncio
import errno
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from openviking.service import task_store
from openviking.service import task_tracker as tracker_module
from openviking.service.task_domain import (
    TaskRecord,
    TaskStatus,
    WorkRecord,
    WorkRecoveryMode,
    WorkState,
)
from openviking.service.task_store import PersistentTaskStore, StoreOperationCode
from openviking.service.task_tracker import TaskTracker
from openviking.storage.queuefs import queue_manager
from tests.test_task_tracker import _FakeAgfs

pytestmark = pytest.mark.asyncio
OWNER = {"account_id": "acme", "user_id": "alice"}


@pytest.fixture
def store_runtime(monkeypatch):
    agfs = _FakeAgfs()
    store = PersistentTaskStore(agfs)
    log = Mock()
    monkeypatch.setattr(task_store, "logger", log)
    # 每个用例模拟独立进程，生产集合仍跨 store 实例去重。
    monkeypatch.setattr(task_store, "_CORRUPT_TASK_PATHS", set())
    return agfs, store, log


@pytest.mark.parametrize(
    "raw,error_type",
    [(b"", "JSONDecodeError"), (b"[]", "ValueError"), (b"\xff", "UnicodeDecodeError")],
)
async def test_corrupt_task_is_missing_without_rewriting_evidence(store_runtime, raw, error_type):
    agfs, store, log = store_runtime
    path = store._task_path(**OWNER, task_id="broken")
    await store._ensure_task_dir(**OWNER)
    agfs.files[path] = raw
    work = WorkRecord(work_id="delivery", task_id="broken", queue_name="Embedding", **OWNER)

    result = await store.reconcile_delivery(work, **OWNER)
    assert result.code is StoreOperationCode.NOT_FOUND
    assert await store._load_task("broken", **OWNER) is None
    await TaskTracker(store).reconcile_delivery("broken", "delivery", "Embedding", **OWNER)
    # 分页、全量列举和另一 store 实例均不重复报告同一路径。
    assert await store.list(**OWNER, limit=10) == []
    assert await store._list_owner_payloads(**OWNER) == []
    assert [item async for item in store._iter_task_payloads()] == [None]
    assert await PersistentTaskStore(agfs)._load_task("broken", **OWNER) is None

    log.error.assert_called_once()
    assert log.error.call_args.args[1:] == (path, len(raw), error_type)
    assert agfs.files == {path: raw}
    assert agfs.rm_calls == []
    assert agfs.write_calls == []


@pytest.mark.parametrize("phase", ["write", "mv"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_failed_replace_preserves_original_and_original_exception(
    store_runtime, monkeypatch, phase, cleanup_fails
):
    agfs, store, log = store_runtime
    task = TaskRecord(task_id="original", task_type="add_resource", **OWNER)
    await store._write_task(task)
    path = store._task_path(**OWNER, task_id=task.task_id)
    original = agfs.files[path]
    error = OSError(errno.ENOSPC, "模拟磁盘已满")
    original_write = agfs.write

    def fail_write(temporary, data, **kwargs):
        original_write(temporary, b"", **kwargs)
        raise error

    def fail_move(old_path, new_path, **kwargs):
        assert agfs.files[old_path]
        assert agfs.files[new_path] == original
        raise error

    monkeypatch.setattr(agfs, phase, fail_write if phase == "write" else fail_move)
    agfs.fail_rm = cleanup_fails
    task.status = TaskStatus.RUNNING

    with pytest.raises(OSError) as caught:
        await store._write_task(task)

    assert caught.value is error
    assert agfs.files[path] == original
    temporary = agfs.rm_calls[-1]["path"]
    assert temporary.startswith(f"{path}.tmp-") and not temporary.endswith(".json")
    assert agfs.rm_calls[-1]["ctx"]["disable_auto_pathlock"] == "true"
    if cleanup_fails:
        assert temporary in agfs.files
        log.error.assert_called_once()
    else:
        assert list(agfs.files) == [path]
        log.error.assert_not_called()


async def test_successful_replace_publishes_complete_payload(store_runtime, monkeypatch):
    agfs, store, _log = store_runtime
    task = TaskRecord(task_id="replace", task_type="add_resource", **OWNER)
    await store._write_task(task)
    path = store._task_path(**OWNER, task_id=task.task_id)
    original = agfs.files[path]
    original_mv = agfs.mv

    def inspect_move(old_path, new_path, *, ctx):
        assert new_path == path and agfs.files[path] == original
        assert json.loads(agfs.files[old_path])["status"] == "running"
        assert ctx["disable_auto_pathlock"] == "true"
        return original_mv(old_path, new_path, ctx=ctx)

    monkeypatch.setattr(agfs, "mv", inspect_move)
    task.status = TaskStatus.RUNNING
    await store._write_task(task)

    assert json.loads(agfs.files[path])["status"] == "running"
    assert list(agfs.files) == [path]
    assert all(".tmp-" in call["path"] for call in agfs.write_calls)


@pytest.mark.parametrize("phase", ["write", "mv"])
async def test_missing_directory_retries_atomic_replace(store_runtime, monkeypatch, phase):
    agfs, store, _log = store_runtime
    task = TaskRecord(task_id="directory-retry", task_type="add_resource", **OWNER)
    await store._ensure_task_dir(**OWNER)
    initial_mkdir_count = len(agfs.mkdir_calls)
    original_operation = getattr(agfs, phase)
    calls = 0

    def missing_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileNotFoundError("模拟目录消失")
        return original_operation(*args, **kwargs)

    monkeypatch.setattr(agfs, phase, missing_once)
    await store._write_task(task)

    assert calls == 2
    assert len(agfs.mkdir_calls) == initial_mkdir_count * 2
    assert list(agfs.files) == [store._task_path(**OWNER, task_id=task.task_id)]


def _task_with_invalid_checkpoint(invalid):
    task = TaskRecord(task_id="checkpoint", task_type="add_resource", **OWNER)
    work = WorkRecord(
        work_id="failed",
        task_id=task.task_id,
        queue_name="Embedding",
        state=WorkState.FAILED,
        **OWNER,
    )
    valid = {**asdict(work), "state": "failed"}
    invalid_payload = {**valid, "work_id": "invalid"}
    if invalid == "owner":
        invalid_payload["user_id"] = "another-owner"
    elif invalid == "state":
        invalid_payload["state"] = "done"
    elif invalid == "duplicate":
        invalid_payload = dict(valid)
    task._extra_fields[task_store._FAILED_WORK_CHECKPOINT] = (
        {} if invalid == "not-list" else [valid, invalid_payload]
    )
    return task


@pytest.mark.parametrize("invalid", ["owner", "state", "duplicate", "not-list"])
async def test_invalid_checkpoint_get_rejects_every_load_without_poisoning_cache(
    store_runtime, invalid
):
    agfs, store, log = store_runtime
    task = _task_with_invalid_checkpoint(invalid)
    await store._write_task(task)
    reloaded = PersistentTaskStore(agfs)
    tracker = TaskTracker(reloaded)
    original = dict(agfs.files)
    message = "Invalid failed Work checkpoint"
    if invalid != "not-list":
        message += " owner or state"

    for _ in range(2):
        with pytest.raises(ValueError, match=f"^{message}$"):
            await tracker.get(task.task_id, **OWNER)
        assert reloaded._task_cache == {}
        assert reloaded._work_sets == {}
    assert agfs.files == original
    log.error.assert_not_called()


def _queue_snapshot(task_ids):
    async def snapshot():
        for task_id in task_ids:
            yield {
                "id": f"message-{task_id}",
                "data": json.dumps(
                    {
                        "task_id": task_id,
                        "_task_work_id": f"work-{task_id}",
                        "private_payload": "不应出现在日志中的合成载荷",
                        **OWNER,
                    }
                ),
            }

    manager = queue_manager.QueueManager(object())
    manager._queues["Embedding"] = SimpleNamespace(iter_snapshot=snapshot)
    return manager


@pytest.mark.parametrize("invalid", ["owner", "state", "duplicate", "not-list"])
async def test_prepare_task_tracking_isolates_invalid_checkpoint_and_restores_next_task(
    store_runtime, monkeypatch, invalid
):
    agfs, store, store_log = store_runtime
    broken = _task_with_invalid_checkpoint(invalid)
    healthy = TaskRecord(task_id="healthy", task_type="add_resource", **OWNER)
    for task in (broken, healthy):
        await store._write_task(task)
    tracker = TaskTracker(PersistentTaskStore(agfs))
    restore = AsyncMock(wraps=tracker.restore_work_tasks)
    monkeypatch.setattr(tracker, "restore_work_tasks", restore)
    log = Mock()
    monkeypatch.setattr(queue_manager, "logger", log)
    manager = _queue_snapshot([broken.task_id, healthy.task_id])

    restored = await manager.prepare_task_tracking(tracker)

    assert [task.task_id for task in restored] == [healthy.task_id]
    restore.assert_awaited_once_with({healthy.task_id: (OWNER["account_id"], OWNER["user_id"])})
    assert (
        await tracker.get_work_state(healthy.task_id, f"work-{healthy.task_id}", **OWNER)
        is WorkState.PENDING
    )
    log.error.assert_called_once()
    message = "Invalid failed Work checkpoint"
    if invalid != "not-list":
        message += " owner or state"
    assert log.error.call_args.args[1:] == (
        broken.task_id,
        f"work-{broken.task_id}",
        "Embedding",
        "ValueError",
        message,
    )
    store_log.error.assert_not_called()
    with pytest.raises(ValueError, match="Invalid failed Work checkpoint"):
        await tracker.get(broken.task_id, **OWNER)


async def test_restore_work_tasks_isolates_invalid_checkpoint_and_restores_next_task(
    store_runtime, monkeypatch
):
    agfs, store, store_log = store_runtime
    broken = _task_with_invalid_checkpoint("owner")
    healthy = TaskRecord(task_id="healthy", task_type="add_resource", **OWNER)
    for task in (broken, healthy):
        await store._write_task(task)
    tracker = TaskTracker(PersistentTaskStore(agfs))
    log = Mock()
    monkeypatch.setattr(tracker_module, "logger", log)

    restored = await tracker.restore_work_tasks(
        {task.task_id: (OWNER["account_id"], OWNER["user_id"]) for task in (broken, healthy)}
    )

    assert [task.task_id for task in restored] == [healthy.task_id]
    log.error.assert_called_once()
    assert log.error.call_args.args[1:] == (
        broken.task_id,
        OWNER["account_id"],
        OWNER["user_id"],
        "ValueError",
        "Invalid failed Work checkpoint owner or state",
    )
    store_log.error.assert_not_called()
    with pytest.raises(ValueError, match="Invalid failed Work checkpoint owner or state"):
        await tracker.get(broken.task_id, **OWNER)


@pytest.mark.parametrize(
    "error", [OSError("合成读失败"), TypeError("合成类型错误"), asyncio.CancelledError()]
)
@pytest.mark.parametrize("phase", ["reconcile", "restore"])
async def test_startup_recovery_propagates_other_exceptions(monkeypatch, error, phase):
    log = Mock()
    monkeypatch.setattr(queue_manager, "logger", log)
    monkeypatch.setattr(tracker_module, "logger", log)
    if phase == "reconcile":
        tracker = SimpleNamespace(
            work_recovery_mode=WorkRecoveryMode.QUEUE_SNAPSHOT,
            reconcile_delivery=AsyncMock(side_effect=error),
            restore_work_tasks=AsyncMock(),
        )
        manager = _queue_snapshot(["task"])
        with pytest.raises(type(error)) as caught:
            await manager.prepare_task_tracking(tracker)
        tracker.restore_work_tasks.assert_not_awaited()
    else:
        tracker = TaskTracker(PersistentTaskStore(_FakeAgfs()))
        monkeypatch.setattr(tracker, "get", AsyncMock(side_effect=error))
        with pytest.raises(type(error)) as caught:
            await tracker.restore_work_tasks({"task": (OWNER["account_id"], OWNER["user_id"])})
    assert caught.value is error
    log.error.assert_not_called()
