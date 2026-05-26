from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models import MessageDb
from aleph.types.content_format import ContentFormat
from aleph.web.controllers.messages import build_headers_content


def test_content_format_values():
    assert ContentFormat.FULL.value == "full"
    assert ContentFormat.HEADERS.value == "headers"
    assert ContentFormat.NONE.value == "none"
    # str-enum: compares equal to its raw value
    assert ContentFormat.HEADERS == "headers"


def _make_message(message_type: MessageType, content: dict) -> MessageDb:
    # MessageDb.__init__ derives the denormalized columns from `content`.
    return MessageDb(
        item_hash="q" * 64,
        type=message_type,
        chain=Chain.ETH,
        sender="0xSENDER",
        signature=None,
        item_type=ItemType.inline,
        item_content=None,
        content=content,
        channel=None,
        size=0,
    )


def test_headers_post_keeps_type_and_ref():
    msg = _make_message(
        MessageType.post,
        {"address": "0xABC", "time": 1.0, "type": "my-type", "ref": "ref123",
         "content": {"big": "x" * 1000}},
    )
    assert build_headers_content(msg) == {
        "address": "0xABC",
        "type": "my-type",
        "ref": "ref123",
    }


def test_headers_post_omits_missing_ref():
    msg = _make_message(
        MessageType.post,
        {"address": "0xABC", "time": 1.0, "type": "my-type"},
    )
    assert build_headers_content(msg) == {"address": "0xABC", "type": "my-type"}


def test_headers_aggregate_keeps_key():
    msg = _make_message(
        MessageType.aggregate,
        {"address": "0xABC", "time": 1.0, "key": "my-key", "content": {"a": 1}},
    )
    assert build_headers_content(msg) == {"address": "0xABC", "key": "my-key"}


def test_headers_store_keeps_item_hash_and_ref():
    msg = _make_message(
        MessageType.store,
        {"address": "0xABC", "time": 1.0, "item_type": "ipfs",
         "item_hash": "Qm123", "ref": "ref456"},
    )
    assert build_headers_content(msg) == {
        "address": "0xABC",
        "item_hash": "Qm123",
        "ref": "ref456",
    }


def test_headers_store_omits_missing_ref():
    msg = _make_message(
        MessageType.store,
        {"address": "0xABC", "time": 1.0, "item_type": "ipfs", "item_hash": "Qm123"},
    )
    assert build_headers_content(msg) == {"address": "0xABC", "item_hash": "Qm123"}


def test_headers_forget_address_only():
    msg = _make_message(
        MessageType.forget,
        {"address": "0xABC", "time": 1.0, "hashes": ["Qm1"], "reason": "spam"},
    )
    assert build_headers_content(msg) == {"address": "0xABC"}


def test_headers_program_address_only():
    msg = _make_message(
        MessageType.program,
        {"address": "0xABC", "time": 1.0, "type": "vm-function"},
    )
    # PROGRAM content has a `type` field, but headers mode does not expose it.
    assert build_headers_content(msg) == {"address": "0xABC"}


def test_headers_instance_address_only():
    msg = _make_message(
        MessageType.instance,
        {"address": "0xABC", "time": 1.0},
    )
    assert build_headers_content(msg) == {"address": "0xABC"}


from aleph.schemas.messages_query_params import MessageQueryParams


def test_content_format_default_is_full():
    params = MessageQueryParams.model_validate({})
    assert params.content_format == ContentFormat.FULL


def test_exclude_content_true_resolves_to_none():
    params = MessageQueryParams.model_validate({"excludeContent": "true"})
    assert params.content_format == ContentFormat.NONE


def test_explicit_content_format_overrides_exclude_content():
    params = MessageQueryParams.model_validate(
        {"excludeContent": "true", "contentFormat": "full"}
    )
    assert params.content_format == ContentFormat.FULL


def test_content_format_headers_parsed():
    params = MessageQueryParams.model_validate({"contentFormat": "headers"})
    assert params.content_format == ContentFormat.HEADERS


def test_content_format_invalid_rejected():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        MessageQueryParams.model_validate({"contentFormat": "bogus"})
