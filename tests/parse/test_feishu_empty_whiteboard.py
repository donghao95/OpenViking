"""飞书空画板可能省略nodes字段；保留预览图且不吞掉真实API或结构错误。"""

import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from PIL import Image

from openviking.parse.accessors.feishu_accessor import FeishuAccessor
from openviking_cli.exceptions import InvalidArgumentError, OpenVikingError
from openviking_cli.utils.config.parser_config import FeishuConfig


def accessor(monkeypatch, data, *, download_images=True, preview=None, code=0):
    stream = io.BytesIO()
    Image.new("RGB", (32, 32), "white").save(stream, format="PNG")
    image = stream.getvalue() if preview is None else preview
    nodes_response = SimpleNamespace(
        success=lambda: code == 0,
        code=code,
        msg="synthetic response",
        raw=SimpleNamespace(content=json.dumps({"code": code, "data": data}).encode()),
    )
    image_response = SimpleNamespace(
        success=lambda: True,
        raw=SimpleNamespace(content=image, headers={"content-type": "image/png"}),
    )
    worker = FeishuAccessor()
    worker._source_url = "https://example.feishu.cn/docx/synthetic_doc"
    client = SimpleNamespace(
        request=MagicMock(),
        board=SimpleNamespace(
            v1=SimpleNamespace(whiteboard=SimpleNamespace(download_as_image=MagicMock()))
        ),
    )
    calls = []

    def call(method, request, token):
        calls.append(request)
        return nodes_response if len(calls) == 1 else image_response

    monkeypatch.setattr(worker, "_get_client", lambda **kwargs: client)
    monkeypatch.setattr(
        worker, "_get_config", lambda: FeishuConfig(download_images=download_images)
    )
    monkeypatch.setattr(worker, "_call_api", call)
    return worker, image, calls


def test_missing_nodes_preserves_preview_bytes_and_document_link(monkeypatch):
    worker, image, calls = accessor(monkeypatch, {})
    block = SimpleNamespace(board=SimpleNamespace(token="synthetic_board"), block_id="block_1")
    markdown = worker._whiteboard_block_to_markdown(block, {}, document_id="synthetic_doc")
    markdown, images = worker._resolve_image_refs(markdown)
    assert "https://example.feishu.cn/docx/synthetic_doc#block_1" in markdown
    assert "images/whiteboard_synthetic_board.png" in markdown
    assert list(images.values()) == [image]
    assert worker._whiteboards.nodes == 0
    assert worker._whiteboards.cache["synthetic_board"].markdown == ""
    assert len(calls) == 2
    assert worker._read_whiteboard("synthetic_board", None).preview[0] == image
    assert len(calls) == 2


def test_missing_nodes_without_image_download_keeps_source_link(monkeypatch):
    worker, _, calls = accessor(monkeypatch, {}, download_images=False)
    block = SimpleNamespace(board=SimpleNamespace(token="synthetic_board"), block_id="block_1")
    markdown = worker._whiteboard_block_to_markdown(block, {}, document_id="synthetic_doc")
    assert markdown == "[Whiteboard](https://example.feishu.cn/docx/synthetic_doc#block_1)"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "data", [None, [], "invalid", {"nodes": None}, {"nodes": "invalid"}, {"nodes": [None]}]
)
def test_malformed_nodes_are_still_rejected(monkeypatch, data):
    worker, _, calls = accessor(monkeypatch, data)
    with pytest.raises(OpenVikingError, match="Invalid Feishu whiteboard nodes"):
        worker._read_whiteboard("synthetic_board", None)
    assert len(calls) == 1


def test_permission_error_is_not_treated_as_empty_board(monkeypatch):
    worker, _, calls = accessor(monkeypatch, {}, code=99991679)
    with pytest.raises(OpenVikingError):
        worker._read_whiteboard("synthetic_board", None)
    assert len(calls) == 1


def test_missing_nodes_does_not_hide_invalid_preview(monkeypatch):
    worker, _, _ = accessor(monkeypatch, {}, preview=b"not an image")
    with pytest.raises(InvalidArgumentError, match="not an ingestible image"):
        worker._read_whiteboard("synthetic_board", None)
