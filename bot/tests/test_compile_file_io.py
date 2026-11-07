"""慢磁盘不能阻塞编译控制面，取消也不能让旧写入越过任务锁。"""

import asyncio
import json
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from vikingbot.compile.models import CompileTask, SanitizedCompileRequest, utc_now
from vikingbot.compile.pipeline_io import ROOT, TaskFiles
from vikingbot.compile.store import CompileTaskStore
from vikingbot.config.schema import SandboxConfig, SessionKey
from vikingbot.sandbox.backends.direct import DirectBackend

from openviking.service.compile_service import CompileService
from openviking_cli.utils.config.open_viking_config import CompileApiConfig


@contextmanager
def held_io():
    started, release = threading.Event(), threading.Event()
    # 旧实现会阻塞事件循环；安全计时器避免回归测试自身永久挂起。
    timer = threading.Timer(2, release.set)
    timer.start()
    try:
        yield started, release
    finally:
        release.set()
        timer.cancel()
        timer.join()


def task(task_id):
    return CompileTask(
        task_id=task_id,
        principal_scope="test",
        sanitized_request=SanitizedCompileRequest(
            **{
                "from": ["viking://resources/source"],
                "to": "viking://resources/wiki",
                "skill": "viking://user/test/skills/wiki",
                "instruction": "保留来源引用。",
            }
        ),
        status="running",
        stage="pipeline",
        created_at=utc_now(),
        updated_at=utc_now(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_slow_compile_shard_keeps_loop_responsive_and_settles_on_cancel(
    tmp_path, monkeypatch, cancel
):
    sandbox = DirectBackend(
        SandboxConfig(), SessionKey(type="compile", channel_id="test", chat_id="test"), tmp_path
    )
    original = Path.write_text
    target = tmp_path / ROOT / "source.json"
    with held_io() as (started, release):

        def write(path, *args, **kwargs):
            if path.parent == target.parent and path.name.startswith("source.json."):
                started.set()
                assert release.wait(3)
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", write)
        writer = asyncio.create_task(TaskFiles(sandbox).put("source", {"text": "原文"}))
        try:
            assert await asyncio.to_thread(started.wait, 1)
            assert not writer.done(), "文件写入阻塞了事件循环，控制面无法及时响应"
            if cancel:
                writer.cancel()
                await asyncio.sleep(0)
                assert not writer.done(), "取消不能让仍在执行的后台写入脱离任务生命周期"
        finally:
            release.set()
            if cancel:
                with pytest.raises(asyncio.CancelledError):
                    await writer
            else:
                await writer
    if cancel:
        assert not target.exists(), "取消的分片不得发布为完整文件"
        temporary = list(target.parent.glob("source.json.*.tmp"))
        assert len(temporary) == 1
        assert json.loads(temporary[0].read_text()) == {"text": "原文"}
    else:
        assert json.loads(target.read_text()) == {"text": "原文"}
    with pytest.raises(PermissionError):
        await sandbox.write_file("../outside.json", "禁止越界")


@pytest.mark.asyncio
async def test_slow_status_read_does_not_block_unrelated_task(tmp_path, monkeypatch):
    store = CompileTaskStore(tmp_path)
    await store.create(task("cmp_slow"))
    await store.create(task("cmp_other"))
    original = Path.read_text
    with held_io() as (started, release):

        def read(path, *args, **kwargs):
            if path.name == "cmp_slow.json":
                started.set()
                assert release.wait(3)
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read)
        slow = asyncio.create_task(store.get("cmp_slow"))
        try:
            assert await asyncio.to_thread(started.wait, 1)
            assert not slow.done(), "状态文件读取阻塞了事件循环"
            other = await asyncio.wait_for(store.get("cmp_other"), 1)
            assert other.task_id == "cmp_other"
        finally:
            release.set()
            await slow
    assert await store.get("cmp_missing") is None


@pytest.mark.asyncio
async def test_cancelled_status_write_cannot_overwrite_newer_state(tmp_path, monkeypatch):
    store = CompileTaskStore(tmp_path)
    await store.create(task("cmp_serial"))
    original = store._write_atomic
    with held_io() as (started, release):

        def write(path, current):
            if current.stage == "slow":
                started.set()
                assert release.wait(3)
            return original(path, current)

        monkeypatch.setattr(store, "_write_atomic", write)
        first = asyncio.create_task(
            store.update("cmp_serial", lambda current: setattr(current, "stage", "slow"))
        )
        second = None
        try:
            assert await asyncio.to_thread(started.wait, 1)
            assert not first.done(), "状态写入阻塞了事件循环"
            first.cancel()
            second = asyncio.create_task(
                store.update("cmp_serial", lambda current: setattr(current, "status", "cancelled"))
            )
            await asyncio.sleep(0.02)
            assert not first.done()
            assert not second.done(), "任务锁必须覆盖后台写入的实际生命周期"
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await first
            if second is not None:
                await second
    assert (await store.get("cmp_serial")).status == "cancelled"
    assert store._locks == {}


@pytest.mark.parametrize("configured", [False, True])
def test_local_compile_respects_timeout_and_poll_configuration(configured):
    config = CompileApiConfig(
        **({"http_timeout_seconds": 120, "poll_interval_ms": 45000} if configured else {})
    )
    service = CompileService(config, SimpleNamespace(), SimpleNamespace())
    service.configure_local_backend("http://127.0.0.1:1", "synthetic-gateway")
    endpoint = service._endpoint()
    assert endpoint.local
    assert endpoint.http_timeout_seconds == config.http_timeout_seconds
    assert endpoint.poll_interval_ms == config.poll_interval_ms


@pytest.mark.asyncio
async def test_store_preserves_duplicate_and_missing_task_errors(tmp_path):
    store = CompileTaskStore(tmp_path)
    await store.create(task("cmp_duplicate"))
    with pytest.raises(FileExistsError):
        await store.create(task("cmp_duplicate"))
    with pytest.raises(FileNotFoundError):
        await store.update("cmp_missing", lambda current: None)
