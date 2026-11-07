"""图片投递在文本向量模型下的恢复，以及失败不冒充成功。"""

import asyncio
import base64
import io
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from PIL import Image

from openviking.models.embedder.base import EmbedResult, extract_text_from_content
from openviking.storage.collection_schemas import TextEmbeddingHandler
from openviking.storage.index_action import IndexAction
from openviking.storage.queuefs.embedding_msg import EmbeddingMsg
from openviking.storage.queuefs.process_result import ProcessOutcome
from openviking_cli.utils.config.parser_config import ImageConfig


class Embedder:
    def __init__(self, multimodal=False):
        self.supports_multimodal = multimodal
        self.inputs = []

    def prepare_embedding_input(self, content):
        return content if self.supports_multimodal else extract_text_from_content(content)

    async def embed_async(self, content, is_query=False):
        assert isinstance(content, list) or content.strip()
        self.inputs.append(content)
        return EmbedResult(dense_vector=[0.1, 0.2])


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(
        "openviking_cli.utils.config.get_openviking_config",
        lambda: SimpleNamespace(image=ImageConfig()),
    )
    vlm = SimpleNamespace(
        get_vision_completion_async=AsyncMock(return_value="synthetic image caption")
    )
    resolver = SimpleNamespace(get_vlm=AsyncMock(return_value=vlm))
    monkeypatch.setattr(
        "openviking.storage.viking_fs._base._instance",
        SimpleNamespace(_vlm_resolver=resolver),
    )
    db = SimpleNamespace(
        is_closing=False,
        has_queue_manager=False,
        account_uses_content_field=AsyncMock(return_value=False),
        upsert=AsyncMock(return_value="record-1"),
    )
    embedder = Embedder()
    handler = TextEmbeddingHandler(db, SimpleNamespace(bind=lambda _account: embedder))
    return handler, embedder, vlm, resolver, db


def delivery(*, text=None, abstract="", image_format="PNG"):
    stream = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(stream, format=image_format)
    raw = stream.getvalue()
    parts = [] if text is None else [{"type": "text", "text": text}]
    parts.append(
        {
            "type": "image_url",
            "image_url": {
                "url": "data:image/"
                + image_format.lower()
                + ";base64,"
                + base64.b64encode(raw).decode()
            },
        }
    )
    msg = EmbeddingMsg(
        message=parts,
        context_data={
            "account_id": "tenant-a",
            "uri": "viking://resources/image.png",
            "abstract": abstract,
            "level": 2,
        },
        action=IndexAction.UPSERT,
    )
    return {"id": "message-1", "data": msg.to_json()}, raw


@pytest.mark.parametrize(("image_format", "abstract"), [("PNG", ""), ("TIFF", ""), ("PNG", " \t")])
async def test_legacy_image_only_delivery_gets_account_caption_and_vector(
    setup, image_format, abstract
):
    handler, embedder, vlm, resolver, db = setup
    message, raw = delivery(image_format=image_format, abstract=abstract)
    original = dict(message)
    result = await handler.on_dequeue(message)
    assert result.outcome is ProcessOutcome.SUCCESS
    assert result.value["vector_written"]
    assert embedder.inputs == ["synthetic image caption"]
    resolver.get_vlm.assert_awaited_once_with("tenant-a")
    written = db.upsert.await_args.args[0]
    assert written["abstract"] == "synthetic image caption"
    assert written["vector"] == [0.1, 0.2]
    assert message == original
    model_image = vlm.get_vision_completion_async.await_args.kwargs["images"][0]
    if image_format == "PNG":
        assert model_image == raw
    else:
        with Image.open(io.BytesIO(model_image)) as img:
            assert img.format == "JPEG"


@pytest.mark.parametrize(
    ("text", "abstract", "expected"),
    [("existing text", "", "existing text"), (None, "existing caption", "existing caption")],
)
async def test_existing_text_or_caption_does_not_call_vlm(setup, text, abstract, expected):
    handler, embedder, vlm, _, _ = setup
    message, _ = delivery(text=text, abstract=abstract)
    assert (await handler.on_dequeue(message)).outcome is ProcessOutcome.SUCCESS
    assert embedder.inputs == [expected]
    vlm.get_vision_completion_async.assert_not_awaited()


async def test_multimodal_model_keeps_original_image_input(setup):
    handler, embedder, vlm, _, _ = setup
    embedder.supports_multimodal = True
    message, _ = delivery()
    assert (await handler.on_dequeue(message)).outcome is ProcessOutcome.SUCCESS
    assert embedder.inputs[0][0]["type"] == "image_url"
    vlm.get_vision_completion_async.assert_not_awaited()


@pytest.mark.parametrize("failure", ["empty", "exception", "timeout"])
async def test_caption_failure_never_writes_vector_or_calls_text_model(setup, monkeypatch, failure):
    handler, embedder, vlm, _, db = setup
    if failure == "empty":
        vlm.get_vision_completion_async.return_value = "   "
    elif failure == "exception":
        vlm.get_vision_completion_async.side_effect = RuntimeError("synthetic VLM failure")
    else:

        async def pending(**kwargs):
            await asyncio.Event().wait()

        vlm.get_vision_completion_async.side_effect = pending
        monkeypatch.setattr(
            "openviking.storage.collection_schemas.IMAGE_EMBEDDING_CAPTION_TIMEOUT_SECONDS", 0.01
        )
    message, _ = delivery()
    assert (await handler.on_dequeue(message)).outcome is ProcessOutcome.FAILED
    assert not embedder.inputs
    db.upsert.assert_not_awaited()


async def test_caption_cancellation_propagates_without_writing_vector(setup):
    handler, embedder, vlm, _, db = setup
    started = asyncio.Event()

    async def pending(**kwargs):
        started.set()
        await asyncio.Event().wait()

    vlm.get_vision_completion_async.side_effect = pending
    message, _ = delivery()
    job = asyncio.create_task(handler.on_dequeue(message))
    await asyncio.wait_for(started.wait(), timeout=1)
    job.cancel()
    with pytest.raises(asyncio.CancelledError):
        await job
    assert not embedder.inputs
    db.upsert.assert_not_awaited()


@pytest.mark.parametrize(
    "raw", [b"<!doctype html><html>synthetic</html>", b"not an image", b"\x89PNG\r\n"]
)
async def test_invalid_image_never_calls_model_or_writes_vector(setup, raw):
    handler, embedder, vlm, _, db = setup
    message, _ = delivery()
    payload = json.loads(message["data"])
    payload["payload"]["message"][0]["image_url"]["url"] = (
        "data:image/png;base64," + base64.b64encode(raw).decode()
    )
    message["data"] = json.dumps(payload)
    result = await handler.on_dequeue(message)
    assert result.outcome is ProcessOutcome.FAILED
    assert "not a decodable image" in result.error
    vlm.get_vision_completion_async.assert_not_awaited()
    assert not embedder.inputs
    db.upsert.assert_not_awaited()
