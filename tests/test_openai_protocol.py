"""Validate message selection and the connector's OpenAI-compatible wire shapes."""

from __future__ import annotations

import json

import pytest

from dots_bridge.connectors._openai_protocol import (
    ProtocolError,
    chat_chunks,
    chat_completion,
    latest_chat_text,
    latest_response_text,
    response_events,
    response_finish_events,
    response_object,
    response_start_events,
)


def test_chat_selects_only_latest_user_and_ignores_history_and_generation_options():
    payload = {
        "messages": [
            {"role": "system", "content": "Do not forward me"},
            {"role": "user", "content": "A previous question"},
            {"role": "assistant", "content": "An old answer"},
            {"role": "user", "content": "  最新の依頼\n  "},
            {"role": "developer", "content": {"ignored": True}},
            {"role": "assistant", "content": None},
        ],
        "temperature": 1.9,
        "max_tokens": 2,
    }
    assert latest_chat_text(payload) == "  最新の依頼\n  "


@pytest.mark.parametrize(
    "reader,field", [(latest_chat_text, "messages"), (latest_response_text, "input")]
)
def test_latest_message_text_parts_are_joined_in_order(reader, field):
    payload = {
        field: [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "最初"},
                    {"type": "input_text", "text": "次"},
                ],
            }
        ]
    }
    assert reader(payload) == "最初\n次"


def test_responses_accepts_string_or_latest_user_message_without_replaying_items():
    assert latest_response_text({"input": "Just this input"}) == "Just this input"
    payload = {
        "instructions": "Do not forward me",
        "previous_response_id": "resp_old",
        "input": [
            {"type": "message", "role": "system", "content": "Ignore"},
            {"role": "user", "content": "Old"},
            {"type": "function_call_output", "call_id": "old", "output": "Ignore"},
            {"type": "message", "role": "user", "content": "New"},
            {"role": "assistant", "content": "Ignore"},
        ],
    }
    assert latest_response_text(payload) == "New"
    payload["input"][3].pop("type")
    assert latest_response_text(payload) == "New"


@pytest.mark.parametrize(
    "reader,field", [(latest_chat_text, "messages"), (latest_response_text, "input")]
)
@pytest.mark.parametrize(
    "content",
    [
        None,
        "",
        " \n\t",
        [],
        {},
        ["not a content object"],
        [{"type": "text", "text": None}],
        [{"type": "text", "text": 123}],
        [{"type": [], "text": "Invalid type"}],
        [{"type": "input_image", "image_url": "https://example.com/image.png"}],
        [
            {"type": "text", "text": "A mixed image request"},
            {"type": "image_url", "image_url": {"url": "https://example.com/image.png"}},
        ],
        [{"type": "input_audio", "input_audio": {"data": "encoded", "format": "wav"}}],
        [{"type": "input_file", "file_id": "file_123"}],
    ],
)
def test_invalid_latest_user_never_falls_back_to_earlier_message(reader, field, content):
    with pytest.raises(ProtocolError) as failure:
        reader(
            {
                field: [
                    {"role": "user", "content": "Must not be reused"},
                    {"role": "user", "content": content},
                ]
            }
        )
    error = failure.value
    assert error.status_code == 400
    assert error.code == "invalid_request_error"
    assert error.param.startswith(f"{field}[1].content")
    assert "Must not be reused" not in error.message


@pytest.mark.parametrize(
    "reader,field", [(latest_chat_text, "messages"), (latest_response_text, "input")]
)
@pytest.mark.parametrize(
    "value", [None, [], {}, 123, True, [{"role": "assistant", "content": "Hi"}]]
)
def test_missing_or_malformed_message_arrays_return_protocol_error(reader, field, value):
    with pytest.raises(ProtocolError) as failure:
        reader({field: value})
    assert failure.value.param == field


@pytest.mark.parametrize("reader", [latest_chat_text, latest_response_text])
def test_non_object_requests_return_protocol_error(reader):
    with pytest.raises(ProtocolError, match="JSON object"):
        reader([])


def test_responses_rejects_empty_string_and_wrong_latest_user_item_type():
    with pytest.raises(ProtocolError) as failure:
        latest_response_text({"input": "   "})
    assert failure.value.param == "input"
    with pytest.raises(ProtocolError) as failure:
        latest_response_text(
            {
                "input": [
                    {"role": "user", "content": "Old"},
                    {"role": "user", "type": "input_image", "content": "Unsupported"},
                ]
            }
        )
    assert failure.value.param == "input[1].type"


@pytest.mark.parametrize(
    "reader,field", [(latest_chat_text, "messages"), (latest_response_text, "input")]
)
def test_16000_character_limit_applies_to_joined_latest_message(reader, field):
    content = [{"type": "text", "text": "あ" * 7999}, {"type": "text", "text": "い" * 8000}]
    payload = {field: [{"role": "user", "content": content}]}
    assert len(reader(payload)) == 16000
    content[0]["text"] += "う"
    with pytest.raises(ProtocolError, match="16000") as failure:
        reader(payload)
    assert failure.value.param == f"{field}[0].content"
    with pytest.raises(ProtocolError, match="16000"):
        latest_response_text({"input": "x" * 16001})


@pytest.mark.parametrize("invalid", ["\ud800", "\udfff", "Text \ud800 end"])
@pytest.mark.parametrize(
    "reader,field", [(latest_chat_text, "messages"), (latest_response_text, "input")]
)
@pytest.mark.parametrize("as_parts", [False, True])
def test_latest_user_with_surrogate_unicode_returns_safe_400(reader, field, invalid, as_parts):
    content = [{"type": "text", "text": invalid}] if as_parts else invalid
    with pytest.raises(ProtocolError, match="valid Unicode") as failure:
        reader(
            {
                field: [
                    {"role": "user", "content": "Do not reuse this history"},
                    {"role": "user", "content": content},
                ]
            }
        )
    assert failure.value.status_code == 400
    assert failure.value.param == f"{field}[1].content"
    json.dumps(failure.value.as_dict(), ensure_ascii=False).encode("utf-8")


def test_responses_string_input_rejects_surrogates_and_accepts_unicode_scalars():
    with pytest.raises(ProtocolError, match="valid Unicode") as failure:
        latest_response_text({"input": "Text \ud800"})
    assert failure.value.param == "input"
    assert latest_response_text({"input": "絵文字 😀"}) == "絵文字 😀"


@pytest.mark.parametrize(
    "status,error_type",
    [
        (400, "invalid_request_error"),
        (401, "authentication_error"),
        (429, "rate_limit_error"),
        (502, "server_error"),
    ],
)
def test_protocol_error_has_transport_independent_safe_error_object(status, error_type):
    error = ProtocolError("Safe message", param="input", status_code=status, code="example_code")
    assert str(error) == "Safe message"
    assert error.as_dict() == {
        "message": "Safe message",
        "param": "input",
        "code": "example_code",
        "type": error_type,
    }


def test_chat_completion_and_chunks_preserve_reply_identity_without_fabricated_usage():
    identity = {"completion_id": "chatcmpl_test", "created": 123, "model": "dots"}
    text = "返信です。\nSecond line."
    completion = chat_completion(text, **identity)
    assert completion["id"] == "chatcmpl_test"
    assert completion["object"] == "chat.completion"
    assert completion["choices"][0]["message"] == {"role": "assistant", "content": text}
    assert completion["choices"][0]["finish_reason"] == "stop"
    assert "usage" not in completion

    chunks = chat_chunks(text, **identity)
    assert chunks[0]["choices"][0]["delta"]["role"] == "assistant"
    assert "".join(chunk["choices"][0]["delta"].get("content", "") for chunk in chunks) == text
    assert [chunk["choices"][0]["finish_reason"] for chunk in chunks] == [None, None, "stop"]
    for chunk in chunks:
        assert chunk["id"] == completion["id"]
        assert chunk["created"] == completion["created"]
        assert chunk["model"] == completion["model"]
        assert chunk["object"] == "chat.completion.chunk"
        assert "usage" not in chunk
    json.dumps([completion, chunks], allow_nan=False)


def test_response_objects_support_pending_failure_and_independent_metadata():
    identity = {"response_id": "resp_test", "created": 123, "model": "dots"}
    pending = response_object(**identity)
    assert pending["status"] == "in_progress"
    assert pending["output"] == []
    assert pending["error"] is None
    assert pending["usage"] is None
    metadata = {"bridge_request_id": "req_test"}
    error = {"code": "server_error", "message": "Bridge unavailable"}
    failed = response_object(**identity, status="failed", error=error, metadata=metadata)
    assert failed["status"] == "failed"
    assert failed["error"] == error
    assert failed["metadata"] == metadata
    failed["error"]["message"] = "Changed"
    failed["metadata"]["bridge_request_id"] = "Changed"
    assert error["message"] == "Bridge unavailable"
    assert metadata["bridge_request_id"] == "req_test"


def test_response_events_form_consistent_lifecycle_and_can_be_sent_in_two_phases():
    identity = {"response_id": "resp_test", "created": 123, "model": "dots"}
    options = {**identity, "stream_id": "lane_1", "metadata": {"bridge_request_id": "req_test"}}
    events = response_events("Full reply", **options)
    assert events == response_start_events(**options) + response_finish_events(
        "Full reply", **options
    )
    assert [event["type"] for event in events] == [
        "response.created",
        "response.in_progress",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert [event["sequence_number"] for event in events] == list(range(9))
    assert all(event["stream_id"] == "lane_1" for event in events)
    assert all(
        event["response"]["id"] == "resp_test" for event in (events[0], events[1], events[-1])
    )
    assert events[0]["response"]["output"] == []
    assert events[1]["response"]["output"] == []
    assert events[-1]["response"]["status"] == "completed"
    item_id = events[2]["item"]["id"]
    assert all(event["item_id"] == item_id for event in events[3:7])
    assert events[2]["item"]["status"] == "in_progress"
    assert events[2]["item"]["content"] == []
    assert events[3]["part"]["text"] == ""
    assert events[4]["delta"] == events[5]["text"] == events[6]["part"]["text"] == "Full reply"
    assert events[7]["item"] == events[-1]["response"]["output"][0]
    assert events[7]["item"]["id"] == item_id
    assert events[7]["item"]["status"] == "completed"
    assert events[-1]["response"]["metadata"] == options["metadata"]
    assert all("stream_id" not in event for event in response_events("Reply", **identity))
    json.dumps(events, allow_nan=False)


def test_response_events_are_independent_snapshots_and_preserve_empty_reply():
    events = response_events("", response_id="resp_empty", created=123, model="dots")
    assert events[4]["delta"] == ""
    assert events[-1]["response"]["output"][0]["content"][0]["text"] == ""
    events[0]["response"]["metadata"]["changed"] = True
    events[7]["item"]["content"][0]["text"] = "Changed"
    assert events[1]["response"]["metadata"] == {}
    assert events[-1]["response"]["output"][0]["content"][0]["text"] == ""
