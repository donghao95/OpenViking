"""任务文件损坏、原子替换及失败检查点的合成回归。"""

import errno
import json
from dataclasses import asdict
from unittest.mock import Mock

import pytest

from openviking.service import task_store
from openviking.service.task_domain import TaskRecord, TaskStatus, WorkRecord, WorkState
from openviking.service.task_store import PersistentTaskStore, StoreOperationCode
from openviking.service.task_tracker import TaskTracker
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


@pytest.mark.parametrize(
    "invalid",
    [None, {}, {"state": "unknown"}, {"state": "failed", "unexpected": True}],
)
async def test_invalid_checkpoint_entry_does_not_hide_valid_failed_work(store_runtime, invalid):
    agfs, store, log = store_runtime
    task = TaskRecord(task_id="checkpoint", task_type="add_resource", **OWNER)
    work = WorkRecord(
        work_id="failed",
        task_id=task.task_id,
        queue_name="Embedding",
        state=WorkState.FAILED,
        **OWNER,
    )
    task._extra_fields[task_store._FAILED_WORK_CHECKPOINT] = [
        invalid,
        {**asdict(work), "state": "failed"},
    ]
    await store._write_task(task)
    reloaded = PersistentTaskStore(agfs)

    assert (await reloaded._load_task(task.task_id, **OWNER)).task_id == task.task_id
    assert await reloaded.get_work(task.task_id, work.work_id, **OWNER) == work
    log.error.assert_called_once()
    assert log.error.call_args.args[1:3] == (task.task_id, 0)


async def test_non_list_checkpoint_is_ignored(store_runtime):
    agfs, store, log = store_runtime
    task = TaskRecord(task_id="bad-checkpoint", task_type="add_resource", **OWNER)
    task._extra_fields[task_store._FAILED_WORK_CHECKPOINT] = {}
    await store._write_task(task)
    reloaded = PersistentTaskStore(agfs)

    assert await reloaded._load_task(task.task_id, **OWNER) is not None
    assert await reloaded.get_work(task.task_id, "absent", **OWNER) is None
    log.error.assert_called_once()
    assert log.error.call_args.args[1] == task.task_id
