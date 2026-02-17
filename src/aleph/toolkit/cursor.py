import base64
import json
from typing import Tuple


def encode_cursor(time: float, item_hash: str) -> str:
    """Encode a (time, item_hash) pair into an opaque cursor string."""
    payload = json.dumps({"t": time, "h": item_hash}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> Tuple[float, str]:
    """Decode an opaque cursor string into a (time, item_hash) pair."""
    # Re-add padding
    padding = 4 - len(cursor) % 4
    if padding != 4:
        cursor += "=" * padding
    payload = json.loads(base64.urlsafe_b64decode(cursor))
    return payload["t"], payload["h"]
