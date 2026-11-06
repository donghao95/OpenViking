import base64
import io

import pytest
from PIL import Image

from openviking.models.embedder.base import DenseEmbedderBase, EmbedResult
from openviking.utils.image_search import (
    build_multimodal_embedding_input,
    image_bytes_to_data_uri,
    image_bytes_to_model_data_uri,
    normalize_client_image_input,
    prepare_image_bytes_for_model,
)
from openviking_cli.utils.config.parser_config import ImageConfig


class TextOnlyEmbedder(DenseEmbedderBase):
    def embed(self, content, is_query=False):
        return EmbedResult(dense_vector=[1.0])

    def get_dimension(self) -> int:
        return 1


class MultimodalEmbedder(TextOnlyEmbedder):
    @property
    def supports_multimodal(self) -> bool:
        return True


def test_text_only_embedder_drops_image_parts():
    parts = build_multimodal_embedding_input(
        "cat photo",
        ["data:image/png;base64,YQ==", "viking://resources/photos/cat.png"],
    )

    assert parts == [
        {"type": "text", "text": "cat photo"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,YQ=="}},
        {"type": "image_url", "image_url": {"url": "viking://resources/photos/cat.png"}},
    ]
    assert TextOnlyEmbedder("test").prepare_embedding_input(parts) == "cat photo"
    assert MultimodalEmbedder("test").prepare_embedding_input(parts) == parts


def test_image_helpers_accept_base64_bytes_and_image_uris(tmp_path):
    image_path = tmp_path / "sample.jpg"
    image_path.write_bytes(b"jpg")

    assert image_bytes_to_data_uri(b"png").startswith("data:image/png;base64,")
    assert normalize_client_image_input(image_path).startswith("data:image/jpeg;base64,")


def test_model_image_input_downsamples_jpeg_incompatible_modes():
    buf = io.BytesIO()
    Image.new("LA", (80, 220), color=(255, 255)).save(buf, format="PNG")

    data_uri = image_bytes_to_model_data_uri(
        buf.getvalue(),
        "large-la.png",
        config=ImageConfig(
            preview_max_dimension=64,
            max_file_size_mb=100.0,
            large_image_threshold_dimension=100,
        ),
    )

    assert data_uri.startswith("data:image/jpeg;base64,")
    _, encoded = data_uri.split(";base64,", 1)
    with Image.open(io.BytesIO(base64.b64decode(encoded))) as img:
        assert img.mode == "RGB"
        assert max(img.size) <= 64


@pytest.mark.parametrize("image_format", ["TIFF", "BMP"])
def test_small_unsupported_model_images_are_normalized_without_changing_source(image_format):
    source = io.BytesIO()
    Image.new("RGB", (16, 12), color="red").save(source, format=image_format)
    original = source.getvalue()

    data_uri = image_bytes_to_model_data_uri(original, "attachment.png")
    assert data_uri.startswith("data:image/jpeg;base64,")
    with Image.open(io.BytesIO(base64.b64decode(data_uri.split(",", 1)[1]))) as image:
        assert image.format == "JPEG"
        assert image.size == (16, 12)
    with Image.open(io.BytesIO(original)) as image:
        assert image.format == image_format


def test_small_supported_model_image_keeps_original_bytes():
    source = io.BytesIO()
    Image.new("RGBA", (16, 12), color=(255, 0, 0, 128)).save(source, format="PNG")
    original = source.getvalue()

    assert prepare_image_bytes_for_model(original) == original
