"""
Structured JSON request log: one line per HTTP request on the ``aleph.http``
logger, written by the analytics middleware once the request is over.

Each line is a single JSON object with these keys: ``ts``, ``req_id``,
``method``, ``route``, ``path``, ``query``, ``query_keys``, ``status``,
``dur_ms``, ``bytes_in``, ``bytes_out``, ``client``, ``ua``, ``sender`` and
``auth``. ``route`` is the matched route template (``unmatched`` for 404/405),
``query`` is the raw query string cut to 512 characters, ``ua`` the
User-Agent cut to 256.

``sender`` is the address named by a well-formed message the request posted:
a handler that validated one sets ``request["aleph_sender"]``. ``auth`` is
``claimed`` when it did, ``anonymous`` otherwise. It is "claimed", not
"signed": the handlers only check the message's structure; its signature is
verified later in the processing pipeline (asynchronously for async posts).

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


def _clean(value: str) -> str:
    """
    Make a string from the request safe to serialise. aiohttp decodes header
    values with surrogateescape, so bytes that are not UTF-8 arrive as lone
    surrogates, which orjson refuses: they become U+FFFD instead.
    """
    return value.encode("utf-8", "surrogateescape").decode("utf-8", "replace")


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
        "method": _clean(request.method),
        "route": request.get("analytics_route", ""),
        "path": _clean(request.rel_url.path),
        "query": _clean(request.rel_url.raw_query_string[:MAX_QUERY_LENGTH]),
        "query_keys": [_clean(key) for key in dict.fromkeys(request.query.keys())],
        "status": status,
        "dur_ms": round(dur_ms, 1),
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "client": _clean(_client_address(request)),
        "ua": _clean(request.headers.get("User-Agent", "")[:MAX_USER_AGENT_LENGTH]),
        "sender": sender,
        # "claimed", not "signed": the signature is verified later, in the pipeline.
        "auth": "claimed" if sender is not None else "anonymous",
    }
    return orjson.dumps(line)
