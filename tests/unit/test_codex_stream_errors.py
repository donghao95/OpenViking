"""Responses失败、截断和拒答必须保留失败语义，不能变成空摘要或半截成功。"""

from types import SimpleNamespace

import pytest

from openviking.models.model_error import ModelError, ModelErrorKind
from openviking.models.vlm.backends.codex_responses_adapter import CodexCompletionsAdapter


class Stream:
    def __init__(self, events):
        self.events = events
        self.closed = False

    def __iter__(self):
        return iter(self.events)

    def close(self):
        self.closed = True


def adapter(events):
    stream = Stream(events)
    client = SimpleNamespace(responses=SimpleNamespace(create=lambda **kwargs: stream))
    return CodexCompletionsAdapter(lambda: client, "synthetic"), stream


@pytest.mark.parametrize("with_partial", [False, True])
def test_failed_response_preserves_transient_provider_error(with_partial):
    events = []
    if with_partial:
        events.append(SimpleNamespace(type="response.output_text.delta", delta="partial"))
    events.append(
        SimpleNamespace(
            type="response.failed",
            response=SimpleNamespace(
                status="failed",
                error=SimpleNamespace(code="server_error", message="private diagnostic"),
                output=[],
            ),
        )
    )
    provider, stream = adapter(events)
    with pytest.raises(ModelError) as caught:
        provider.create(messages=[])
    assert caught.value.kind is ModelErrorKind.TRANSIENT_SERVICE_ERROR
    assert caught.value.code == "server_error"
    assert caught.value.status_code is None
    assert "private diagnostic" not in str(caught.value)
    assert stream.closed


def test_error_event_preserves_rate_limit_category():
    provider, stream = adapter(
        [SimpleNamespace(type="error", code="rate_limit_exceeded", message="private diagnostic")]
    )
    with pytest.raises(ModelError) as caught:
        provider.create(messages=[])
    assert caught.value.kind is ModelErrorKind.RATE_LIMIT_ERROR
    assert caught.value.code == "rate_limit_exceeded"
    assert stream.closed


def test_incomplete_response_does_not_commit_partial_caption():
    provider, stream = adapter(
        [
            SimpleNamespace(type="response.output_text.delta", delta="partial"),
            SimpleNamespace(
                type="response.incomplete",
                response=SimpleNamespace(
                    status="incomplete",
                    incomplete_details=SimpleNamespace(reason="max_output_tokens"),
                    output=[],
                ),
            ),
        ]
    )
    with pytest.raises(ModelError) as caught:
        provider.create(messages=[])
    assert caught.value.code == "max_output_tokens"
    assert stream.closed


@pytest.mark.parametrize("with_partial", [False, True])
def test_unterminated_stream_is_a_retryable_network_error(with_partial):
    events = (
        [SimpleNamespace(type="response.output_text.delta", delta="partial")]
        if with_partial
        else []
    )
    provider, stream = adapter(events)
    with pytest.raises(ModelError) as caught:
        provider.create(messages=[])
    assert caught.value.kind is ModelErrorKind.TRANSIENT_NETWORK_ERROR
    assert caught.value.code == "stream_ended_before_completion"
    assert stream.closed


def test_refusal_is_content_safety_failure():
    provider, stream = adapter(
        [
            SimpleNamespace(
                type="response.completed",
                response=SimpleNamespace(
                    status="completed",
                    output=[
                        SimpleNamespace(
                            type="message",
                            content=[SimpleNamespace(type="refusal", refusal="private refusal")],
                        )
                    ],
                ),
            )
        ]
    )
    with pytest.raises(ModelError) as caught:
        provider.create(messages=[])
    assert caught.value.kind is ModelErrorKind.CONTENT_SAFETY
    assert "private refusal" not in str(caught.value)
    assert stream.closed


def test_completed_stream_with_no_answer_is_retryable_service_error():
    provider, stream = adapter(
        [SimpleNamespace(type="response.completed", response=SimpleNamespace(output=[]))]
    )
    with pytest.raises(ModelError) as caught:
        provider.create(messages=[])
    assert caught.value.kind is ModelErrorKind.TRANSIENT_SERVICE_ERROR
    assert caught.value.code == "empty_model_output"
    assert stream.closed
