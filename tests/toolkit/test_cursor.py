import datetime as dt

import pytest

from aleph.toolkit.cursor import (
    decode_address_cursor,
    decode_address_stats_cursor,
    decode_aggregate_cursor,
    decode_credit_history_cursor,
    decode_cursor,
    decode_message_cursor,
    encode_address_cursor,
    encode_address_stats_cursor,
    encode_aggregate_cursor,
    encode_credit_history_cursor,
    encode_cursor,
    encode_message_cursor,
)


class TestGenericCursor:
    def test_roundtrip_string_values(self):
        values = {"t": "2025-01-01T00:00:00+00:00", "h": "abc123"}
        encoded = encode_cursor(values)
        assert isinstance(encoded, str)
        decoded = decode_cursor(encoded)
        assert decoded == values

    def test_roundtrip_numeric_values(self):
        values = {"v": 42, "a": "0xabc"}
        encoded = encode_cursor(values)
        decoded = decode_cursor(encoded)
        assert decoded == values

    def test_invalid_cursor_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor("not-a-valid-cursor!!!")

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor("")

    def test_cursor_is_url_safe(self):
        values = {"t": "2025-01-01T00:00:00+00:00", "h": "abc/def+ghi"}
        encoded = encode_cursor(values)
        assert "+" not in encoded
        assert "/" not in encoded


class TestMessageCursor:
    def test_roundtrip(self):
        time = dt.datetime(2025, 1, 15, 12, 30, 45, 123456, tzinfo=dt.timezone.utc)
        item_hash = "abcdef1234567890"
        encoded = encode_message_cursor(time, item_hash)
        decoded_time, decoded_hash = decode_message_cursor(encoded)
        assert decoded_time == time
        assert decoded_hash == item_hash

    def test_preserves_microseconds(self):
        time = dt.datetime(2025, 6, 15, 0, 0, 0, 999999, tzinfo=dt.timezone.utc)
        encoded = encode_message_cursor(time, "hash")
        decoded_time, _ = decode_message_cursor(encoded)
        assert decoded_time.microsecond == 999999


class TestAddressCursor:
    def test_roundtrip(self):
        address = "0x1234567890abcdef"
        encoded = encode_address_cursor(address)
        decoded_address = decode_address_cursor(encoded)
        assert decoded_address == address


class TestAggregateCursor:
    def test_roundtrip(self):
        time = dt.datetime(2025, 3, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        key = "my-aggregate-key"
        owner = "0xowner123"
        encoded = encode_aggregate_cursor(time, key, owner)
        decoded_time, decoded_key, decoded_owner = decode_aggregate_cursor(encoded)
        assert decoded_time == time
        assert decoded_key == key
        assert decoded_owner == owner


class TestCreditHistoryCursor:
    def test_roundtrip(self):
        time = dt.datetime(2025, 2, 1, 10, 0, 0, tzinfo=dt.timezone.utc)
        credit_ref = "ref123"
        credit_index = 42
        encoded = encode_credit_history_cursor(time, credit_ref, credit_index)
        decoded_time, decoded_ref, decoded_index = decode_credit_history_cursor(encoded)
        assert decoded_time == time
        assert decoded_ref == credit_ref
        assert decoded_index == credit_index


class TestAddressStatsCursor:
    def test_roundtrip_with_int_sort_value(self):
        sort_value = 150
        address = "0xstats_addr"
        encoded = encode_address_stats_cursor(sort_value, address)
        decoded_value, decoded_address = decode_address_stats_cursor(encoded)
        assert decoded_value == sort_value
        assert decoded_address == address
