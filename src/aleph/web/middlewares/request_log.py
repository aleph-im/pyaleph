"""
Structured JSON request log: one line per HTTP request on the ``aleph.http``
logger, written by the analytics middleware once the request is over.

Each line is a single JSON object with these keys: ``ts``, ``req_id``,
``method``, ``route``, ``path``, ``query``, ``query_keys``, ``status``,
``dur_ms``, ``bytes_in``, ``bytes_out``, ``client``, ``ua``, ``sender`` and
``auth``. ``route`` is the matched route template (``unmatched`` for 404/405),
``query`` is the raw query string cut to 512 characters, ``ua`` the
User-Agent cut to 256.

``sender`` is the address a handler established from a signed message: a
handler that validated one sets ``request["aleph_sender"]``. ``auth`` is
``signed`` when it did, ``anonymous`` otherwise.

``setup_request_logging()`` sends the lines to stdout with no prefix and stops
them from reaching the root logger, whose formatter would wrap them.
"""

import logging
import sys
from datetime import datetime, timezone

import orjson
from aiohttp import web

REQUEST_LOGGER_NAME = "aleph.http"
SENDER_KEY = "aleph_sender"

MAX_QUERY_LENGTH = 512
MAX_USER_AGENT_LENGTH = 256

_HANDLER_MARKER = "_aleph_request_log_handler"


def setup_request_logging() -> None:
    """
    Write the request log to stdout, one bare JSON object per line.

    Idempotent: a second call does not add a second handler.
    """
    logger = logging.getLogger(REQUEST_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if any(getattr(handler, _HANDLER_MARKER, False) for handler in logger.handlers):
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    setattr(handler, _HANDLER_MARKER, True)
    logger.addHandler(handler)


def _timestamp() -> str:
    """The current UTC time in RFC 3339 with milliseconds, e.g. 2026-11-06T15:04:05.123Z."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _client_address(request: web.Request) -> str:
    """The first X-Forwarded-For address if the header is set, else the peer."""
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for.strip():
        return forwarded_for.split(",", 1)[0].strip()
    return request.remote or ""


def format_request_log(
    request: web.Request,
    status: int,
    dur_ms: float,
    bytes_in: int,
    bytes_out: int,
) -> bytes:
    """Serialise the request log line for a finished request."""
    sender = request.get(SENDER_KEY)
    line = {
        "ts": _timestamp(),
        "req_id": request.get("req_id", ""),
        "method": request.method,
        "route": request.get("analytics_route", ""),
        "path": request.rel_url.path,
        "query": request.query_string[:MAX_QUERY_LENGTH],
        "query_keys": list(dict.fromkeys(request.query.keys())),
        "status": status,
        "dur_ms": round(dur_ms, 1),
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "client": _client_address(request),
        "ua": request.headers.get("User-Agent", "")[:MAX_USER_AGENT_LENGTH],
        "sender": sender,
        "auth": "signed" if sender is not None else "anonymous",
    }
    return orjson.dumps(line)
