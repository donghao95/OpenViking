"""验证真实绑定的有界恢复，以及分页异常不能造成静默漏读。"""

import json
import os
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from openviking.storage.queuefs.named_queue import NamedQueue
from openviking.utils.agfs_utils import RagfsBindingConfig, create_agfs_client
from openviking_cli.utils.config.agfs_config import AGFSConfig
from openviking_cli.utils.config.cache_config import CacheConfig


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["memory", "sqlite", "cache"])
async def test_native_binding_recovers_every_page_without_ack(tmp_path, backend):
    if backend == "cache" and not os.environ.get("REDIS_URL"):
        pytest.skip("Redis 集成需要 REDIS_URL")
    config = RagfsBindingConfig(
        agfs=AGFSConfig(path=str(tmp_path), backend="local", queuefs={"backend": backend}),
        service_name=f"recovery-{tmp_path.name}",
        cache=(
            CacheConfig(provider="redis", params={"endpoints": [os.environ["REDIS_URL"]]})
            if backend == "cache"
            else None
        ),
    )
    client = create_agfs_client(config)
    queue = NamedQueue(client, "/queue", "Recovery")
    for index in range(75):
        await queue.enqueue({"number": index, "text": "测试" * 1000})
    before = await queue._read_backend_status()
    messages = [message async for message in queue.iter_snapshot()]
    assert len(messages) == len({message["id"] for message in messages}) == 75
    assert sorted(json.loads(message["data"])["number"] for message in messages) == list(range(75))
    assert await queue._read_backend_status() == before


@pytest.mark.asyncio
async def test_snapshot_pages_release_prior_payload_and_forward_cursor():
    queue = NamedQueue(object(), "/queue", "Recovery")
    queue._initialized = True
    first = {"messages": [{"id": "one", "data": "{}"}], "through": "9", "next_cursor": "3"}
    second = {"messages": [], "through": "9", "next_cursor": None}
    transport = AsyncMock()
    transport.read.side_effect = [json.dumps(first).encode(), json.dumps(second).encode()]
    queue._async_agfs = transport
    assert [message["id"] async for message in queue.iter_snapshot()] == ["one"]
    queries = [parse_qs(urlsplit(call.args[0]).query) for call in transport.read.await_args_list]
    assert "after" not in queries[0]
    assert queries[1]["after"] == ["3"] and queries[1]["through"] == ["9"]
    assert all("/messages_page?" in call.args[0] for call in transport.read.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        {"messages": [], "through": "9", "next_cursor": "3"},
        {"messages": [{"id": "x"}], "through": "9", "next_cursor": "3"},
        {"messages": [], "through": "10", "next_cursor": None},
        [],
    ],
)
async def test_invalid_page_fails_without_retrying_full_snapshot(bad):
    queue = NamedQueue(object(), "/queue", "Recovery")
    queue._initialized = True
    transport = AsyncMock()
    transport.read.side_effect = [
        json.dumps({"messages": [{"id": "x"}], "through": "9", "next_cursor": "3"}).encode(),
        json.dumps(bad).encode(),
    ]
    queue._async_agfs = transport
    with pytest.raises(RuntimeError):
        [message async for message in queue.iter_snapshot()]
    assert transport.read.await_count == 2
