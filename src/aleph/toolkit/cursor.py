import base64
import datetime as dt
import json
from typing import Any, Dict, Tuple


def encode_cursor(values: Dict[str, Any]) -> str:
    """Encode an arbitrary dict into an opaque base64url cursor string."""
    payload = json.dumps(values, separators=(",", ":"), sort_keys=True)
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> Dict[str, Any]:
    """Decode a cursor string back to a dict.
    Raises ValueError if malformed.
    """
    if not cursor:
        raise ValueError("Invalid cursor: empty string")
    try:
        padding = 4 - len(cursor) % 4
        if padding != 4:
            cursor += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(cursor))
        if not isinstance(payload, dict):
            raise ValueError("Invalid cursor: payload is not a dict")
        return payload
    except UnicodeDecodeError as e:
        raise ValueError(f"Invalid cursor: {e}") from e
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Invalid cursor: {e}") from e


def encode_message_cursor(time: dt.datetime, item_hash: str) -> str:
    """Encode a message/post/file cursor: (time, item_hash)."""
    return encode_cursor({"t": time.isoformat(), "h": item_hash})


def decode_message_cursor(cursor: str) -> Tuple[dt.datetime, str]:
    """Decode a message/post/file cursor back to (time, item_hash)."""
    try:
        d = decode_cursor(cursor)
        return dt.datetime.fromisoformat(d["t"]), str(d["h"])
    except KeyError:
        raise ValueError("Invalid cursor: missing required fields")


def encode_aggregate_cursor(time: dt.datetime, key: str, owner: str) -> str:
    """Encode an aggregate cursor: (time, key, owner)."""
    return encode_cursor({"t": time.isoformat(), "k": key, "o": owner})


def decode_aggregate_cursor(cursor: str) -> Tuple[dt.datetime, str, str]:
    """Decode an aggregate cursor back to (time, key, owner)."""
    try:
        d = decode_cursor(cursor)
        return dt.datetime.fromisoformat(d["t"]), str(d["k"]), str(d["o"])
    except KeyError:
        raise ValueError("Invalid cursor: missing required fields")


def encode_address_cursor(address: str) -> str:
    """Encode an address-based cursor: (address,)."""
    return encode_cursor({"a": address})


def decode_address_cursor(cursor: str) -> str:
    """Decode an address-based cursor back to address."""
    try:
        d = decode_cursor(cursor)
        return str(d["a"])
    except KeyError:
        raise ValueError("Invalid cursor: missing required fields")


def encode_credit_history_cursor(
    time: dt.datetime, credit_ref: str, credit_index: int
) -> str:
    """Encode a credit history cursor: (message_timestamp, credit_ref, credit_index)."""
    return encode_cursor({"t": time.isoformat(), "r": credit_ref, "i": credit_index})


def decode_credit_history_cursor(cursor: str) -> Tuple[dt.datetime, str, int]:
    """Decode a credit history cursor back to (message_timestamp, credit_ref, credit_index)."""
    try:
        d = decode_cursor(cursor)
        return dt.datetime.fromisoformat(d["t"]), str(d["r"]), int(d["i"])
    except KeyError:
        raise ValueError("Invalid cursor: missing required fields")


def encode_credit_history_sort_cursor(
    sort_by: str,
    sort_value: Any,
    credit_ref: str,
    credit_index: int,
) -> str:
    """Encode a credit history cursor with sort field info."""
    value = (
        sort_value.isoformat() if isinstance(sort_value, dt.datetime) else sort_value
    )
    return encode_cursor({"s": sort_by, "v": value, "r": credit_ref, "i": credit_index})


def decode_credit_history_sort_cursor(
    cursor: str,
) -> Tuple[str, Any, str, int]:
    """Decode a credit history sort cursor.

    Returns (sort_by, sort_value, credit_ref, credit_index).
    For backward compat, if 's' key is missing, assumes 'message_timestamp' sort
    and uses the 't' key as the sort value.
    """
    try:
        d = decode_cursor(cursor)
        if "s" in d:
            return str(d["s"]), d["v"], str(d["r"]), int(d["i"])
        # Backward compat: old cursor format with (t, r, i)
        return "message_timestamp", d["t"], str(d["r"]), int(d["i"])
    except KeyError:
        raise ValueError("Invalid cursor: missing required fields")


def encode_address_stats_cursor(sort_value: Any, address: str) -> str:
    """Encode an address stats cursor: (sort_value, address)."""
    return encode_cursor({"v": sort_value, "a": address})


def decode_address_stats_cursor(cursor: str) -> Tuple[Any, str]:
    """Decode an address stats cursor back to (sort_value, address)."""
    try:
        d = decode_cursor(cursor)
        return d["v"], str(d["a"])
    except KeyError:
        raise ValueError("Invalid cursor: missing required fields")
