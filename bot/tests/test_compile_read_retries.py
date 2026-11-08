"""Compile 来源读取的有限重试，只使用假客户端和合成响应。"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import httpx
import pytest
from vikingbot.compile import service as compile_service
from vikingbot.compile.models import CompileFailure, CompileLimits
from vikingbot.compile.service import BotCompileService, _SDKOpenVikingError

from openviking_cli.client.http import AsyncHTTPClient
from openviking_cli.exceptions import InvalidArgumentError, NotFoundError, OpenVikingError

pytestmark = pytest.mark.asyncio
ROOT = "viking://resources/synthetic"
URI = f"{ROOT}/notes.md"


@pytest.fixture
def runtime(monkeypatch):
    sleep = AsyncMock()
    log = Mock()
    monkeypatch.setattr(compile_service.asyncio, "sleep", sleep)
    monkeypatch.setattr(compile_service, "logger", log)
    service = BotCompileService.__new__(BotCompileService)
    service.limits = CompileLimits()
    client = SimpleNamespace(
        stat=AsyncMock(return_value={"isDir": False}),
        list_resources=AsyncMock(return_value=[{"uri": URI}]),
        read_raw=AsyncMock(return_value="合成来源内容"),
    )
    return service, client, sleep, log


async def test_stat_timeout_then_success_retries_once(runtime):
    service, client, sleep, log = runtime
    client.stat.side_effect = [httpx.ReadTimeout("模拟读超时"), {"isDir": False}]

    assert await service._list_source_files(client, [URI]) == [URI]

    assert client.stat.await_args_list == [call(URI), call(URI)]
    sleep.assert_awaited_once_with(5)
    log.warning.assert_called_once()
    assert log.warning.call_args.args[1:] == ("stat", URI, 1, "ReadTimeout")


async def test_three_timeouts_preserve_last_exception(runtime):
    service, client, sleep, log = runtime
    errors = [httpx.ReadTimeout(f"模拟读超时 {index}") for index in range(3)]
    client.stat.side_effect = errors

    with pytest.raises(httpx.ReadTimeout) as caught:
        await service._list_source_files(client, [URI])

    assert caught.value is errors[-1]
    assert client.stat.await_count == 3
    assert sleep.await_args_list == [call(5), call(20)]
    assert log.warning.call_count == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 500])
async def test_http_non_gateway_errors_do_not_retry(runtime, status):
    service, client, sleep, _log = runtime
    error = httpx.HTTPStatusError(
        "合成 HTTP 错误",
        request=httpx.Request("GET", "http://synthetic.invalid"),
        response=httpx.Response(status),
    )
    client.stat.side_effect = error

    with pytest.raises(httpx.HTTPStatusError) as caught:
        await service._list_source_files(client, [URI])

    assert caught.value is error
    client.stat.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.parametrize(
    "error",
    [
        asyncio.CancelledError(),
        ValueError("合成校验错误"),
        NotFoundError(URI),
        InvalidArgumentError("合成参数错误"),
        OpenVikingError("HTTP 503", code="INTERNAL"),
        CompileFailure("SOURCE_READ_FAILED", "合成业务错误", stage="pipeline"),
        _SDKOpenVikingError("合成业务错误", code="INTERNAL"),
    ],
)
async def test_cancellation_and_business_errors_propagate_immediately(runtime, error):
    service, client, sleep, _log = runtime
    client.stat.side_effect = error

    with pytest.raises(type(error)) as caught:
        await service._list_source_files(client, [URI])

    assert caught.value is error
    client.stat.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.parametrize("status", [502, 503, 504])
@pytest.mark.parametrize("response_kind", ["httpx", "sdk-text", "sdk-json"])
async def test_gateway_errors_retry_using_actual_sdk_exception(runtime, status, response_kind):
    service, client, sleep, _log = runtime
    request = httpx.Request("GET", "http://synthetic.invalid")
    if response_kind == "httpx":
        response = httpx.Response(status, request=request)
        with pytest.raises(httpx.HTTPStatusError) as caught:
            response.raise_for_status()
    else:
        response = (
            httpx.Response(status, request=request, text="合成网关响应")
            if response_kind == "sdk-text"
            else httpx.Response(status, request=request, json={})
        )
        with pytest.raises(_SDKOpenVikingError) as caught:
            AsyncHTTPClient.__new__(AsyncHTTPClient)._handle_response_data(response)
    client.stat.side_effect = [caught.value, {"isDir": False}]

    assert await service._list_source_files(client, [URI]) == [URI]
    assert client.stat.await_count == 2
    sleep.assert_awaited_once_with(5)


async def test_listing_retry_keeps_page_offset_and_inventory(runtime):
    service, client, sleep, _log = runtime
    client.stat.return_value = {"isDir": True}
    first_page = [{"uri": f"{ROOT}/{index:03}.md"} for index in range(500)]
    client.list_resources.side_effect = [
        first_page,
        httpx.ReadTimeout("模拟第二页超时"),
        [{"uri": URI}],
    ]

    assert await service._list_source_files(client, [ROOT]) == sorted(
        [*(item["uri"] for item in first_page), URI]
    )
    assert client.list_resources.await_args_list == [
        call(ROOT, node_limit=500, offset=0),
        call(ROOT, node_limit=500, offset=500),
        call(ROOT, node_limit=500, offset=500),
    ]
    sleep.assert_awaited_once_with(5)


@pytest.mark.parametrize(
    "error_type",
    [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError],
)
async def test_source_read_retries_network_errors_and_preserves_content(runtime, error_type):
    service, client, sleep, _log = runtime
    client.read_raw.side_effect = [error_type("模拟网络错误"), "合成来源内容"]

    batches = await service._prepare_source_batches(client, [ROOT], files=[URI])

    assert [(part.uri, part.content) for batch in batches for part in batch] == [
        (URI, "合成来源内容")
    ]
    assert client.read_raw.await_args_list == [call(URI), call(URI)]
    sleep.assert_awaited_once_with(5)


async def test_image_download_timeout_retries_read_only_calls(runtime):
    service, client, sleep, _log = runtime
    # 最小 PNG 头足够完成原有图片格式识别，不调用外部服务。
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 24
    client.stat.return_value = {"size": len(png)}
    client.download_bytes = AsyncMock(side_effect=[httpx.ReadTimeout("模拟下载超时"), png])
    uri = f"{ROOT}/image.png"

    batches = await service._prepare_source_batches(client, [ROOT], files=[uri])

    assert batches[0][0].uri == uri
    assert batches[0][0].image
    assert client.stat.await_args_list == [call(uri), call(uri)]
    assert client.download_bytes.await_args_list == [call(uri), call(uri)]
    client.read_raw.assert_not_awaited()
    sleep.assert_awaited_once_with(5)
