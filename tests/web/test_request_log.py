"""
Tests for the structured JSON request log: one line per request on the
``aleph.http`` logger, emitted by the analytics middleware.
"""

import asyncio
import json
import logging
import re
import sys
from typing import Any, Dict, List

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from aleph.web.middlewares.analytics import analytics_middleware, set_request_id_header
from aleph.web.middlewares.request_log import (
    REQUEST_LOGGER_NAME,
    format_request_log,
    setup_request_logging,
)

LOG_KEYS = {
    "ts",
    "req_id",
    "method",
    "route",
    "path",
    "query",
    "query_keys",
    "status",
    "dur_ms",
    "bytes_in",
    "bytes_out",
    "client",
    "ua",
    "sender",
    "auth",
}
TS_RE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")


class ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def records():
    logger = logging.getLogger(REQUEST_LOGGER_NAME)
    handler = ListHandler()
    previous_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


def lines(records: List[logging.LogRecord]) -> List[Dict[str, Any]]:
    return [json.loads(record.getMessage()) for record in records]


async def items_handler(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def boom_handler(request: web.Request) -> web.Response:
    raise web.HTTPUnprocessableEntity()


async def crash_handler(request: web.Request) -> web.Response:
    raise RuntimeError("crash")


async def claimed_sender_handler(request: web.Request) -> web.Response:
    request["aleph_sender"] = "0xabc"
    return web.json_response({"ok": True})


async def slow_handler(request: web.Request) -> web.Response:
    await asyncio.sleep(30)
    return web.json_response({"ok": True})


async def timeout_handler(request: web.Request) -> web.Response:
    raise asyncio.TimeoutError()


@pytest.fixture
def app() -> web.Application:
    app = web.Application(middlewares=[analytics_middleware])
    app.on_response_prepare.append(set_request_id_header)
    app.router.add_get("/items/{item_id}", items_handler)
    app.router.add_post("/items/{item_id}", items_handler)
    app.router.add_get("/boom", boom_handler)
    app.router.add_get("/crash", crash_handler)
    app.router.add_get("/claimed", claimed_sender_handler)
    app.router.add_get("/slow", slow_handler)
    app.router.add_get("/timeout", timeout_handler)
    return app


@pytest.mark.asyncio
async def test_one_json_line_per_request_with_schema(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    response = await client.get("/items/7")
    body = await response.read()

    assert len(records) == 1
    record = records[0]
    assert record.name == "aleph.http"
    assert record.levelno == logging.INFO
    assert "\n" not in record.getMessage()
    line = json.loads(record.getMessage())
    assert set(line) == LOG_KEYS
    assert line["route"] == "/items/{item_id}"
    assert line["path"] == "/items/7"
    assert line["status"] == 200
    assert line["method"] == "GET"
    assert line["auth"] == "anonymous" and line["sender"] is None
    assert TS_RE.match(line["ts"])
    assert line["req_id"] == response.headers["X-Request-Id"]
    assert line["query"] == "" and line["query_keys"] == []
    assert isinstance(line["dur_ms"], float) and line["dur_ms"] >= 0
    assert line["dur_ms"] == round(line["dur_ms"], 1)
    assert line["bytes_in"] == 0
    assert line["bytes_out"] == len(body)
    assert line["client"] == "127.0.0.1"
    assert isinstance(line["ua"], str)


@pytest.mark.asyncio
async def test_bytes_in_counts_the_body(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.post("/items/1", data=b"0123456789")

    assert lines(records)[-1]["bytes_in"] == 10


@pytest.mark.asyncio
async def test_query_is_truncated_and_keys_deduplicated(aiohttp_client, app, records):
    client = await aiohttp_client(app)
    query = "a=1&b=2&a=3&c=" + "x" * 600

    await client.get(f"/items/1?{query}")

    line = lines(records)[-1]
    assert line["query_keys"] == ["a", "b", "c"]
    assert len(line["query"]) == 512
    assert line["query"] == query[:512]


@pytest.mark.asyncio
async def test_query_is_logged_raw(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.get(URL("/items/1?q=100%25&r=a%20b", encoded=True))

    line = lines(records)[-1]
    assert line["query"] == "q=100%25&r=a%20b"
    assert line["query_keys"] == ["q", "r"]


def test_invalid_utf8_headers_are_replaced(mocker):
    # aiohttp decodes header bytes with surrogateescape. make_mocked_request
    # re-encodes headers strictly, so the decoded headers are patched in.
    request = make_mocked_request("GET", "/items/1")
    mocker.patch.object(
        web.Request,
        "headers",
        new_callable=mocker.PropertyMock,
        return_value=CIMultiDictProxy(
            CIMultiDict(
                {"User-Agent": "a\udcff", "X-Forwarded-For": "\udcc3(, 1.2.3.4"}
            )
        ),
    )

    line = json.loads(
        format_request_log(request, status=200, dur_ms=1.0, bytes_in=0, bytes_out=0)
    )

    assert line["ua"] == "a\ufffd"
    assert line["client"] == "\ufffd("


@pytest.mark.asyncio
async def test_invalid_utf8_request_still_logs_one_line(aiohttp_client, app, records):
    client = await aiohttp_client(app)
    reader, writer = await asyncio.open_connection(
        client.server.host, client.server.port
    )
    writer.write(
        b"GET /items/1?k=v HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"User-Agent: bad\xff\xfeagent\r\n"
        b"X-Forwarded-For: \xc3(, 1.2.3.4\r\n"
        b"Connection: close\r\n"
        b"\r\n"
    )
    await writer.drain()
    status_line = await asyncio.wait_for(reader.readline(), 5)
    await asyncio.wait_for(reader.read(), 5)
    writer.close()
    await writer.wait_closed()

    assert status_line.startswith(b"HTTP/1.1 200")
    for _ in range(100):
        if records:
            break
        await asyncio.sleep(0.02)
    assert len(records) == 1
    line = lines(records)[0]
    assert line["status"] == 200
    assert line["ua"] == "bad\ufffd\ufffdagent"
    assert line["client"] == "\ufffd("


@pytest.mark.asyncio
async def test_bytes_in_is_the_body_received_not_content_length(
    aiohttp_client, app, records
):
    client = await aiohttp_client(app)
    reader, writer = await asyncio.open_connection(
        client.server.host, client.server.port
    )
    writer.write(
        b"GET /items/1 HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: 9223372036854775807\r\n"
        b"\r\n"
        b"ab"
    )
    await writer.drain()
    status_line = await asyncio.wait_for(reader.readline(), 5)
    writer.close()
    await writer.wait_closed()

    assert status_line.startswith(b"HTTP/1.1 200")
    for _ in range(100):
        if records:
            break
        await asyncio.sleep(0.02)
    assert lines(records)[0]["bytes_in"] == 2


def test_path_and_client_are_capped():
    request = make_mocked_request(
        "GET",
        "/items/" + "p" * 3000,
        headers={"X-Forwarded-For": "c" * 300 + ", 1.2.3.4"},
    )

    line = json.loads(
        format_request_log(request, status=404, dur_ms=1.0, bytes_in=0, bytes_out=0)
    )

    assert line["path"] == ("/items/" + "p" * 3000)[:2048]
    assert line["client"] == "c" * 256


@pytest.mark.asyncio
async def test_query_keys_in_order_of_first_appearance(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.get("/items/1?b=1&a=2&b=3")

    assert lines(records)[-1]["query_keys"] == ["b", "a"]


@pytest.mark.asyncio
async def test_client_from_x_forwarded_for_first_address(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.get("/items/1", headers={"X-Forwarded-For": " 203.0.113.9 , 10.0.0.1"})
    await client.get("/items/1")

    forwarded, direct = lines(records)
    assert forwarded["client"] == "203.0.113.9"
    assert direct["client"] == "127.0.0.1"


@pytest.mark.asyncio
async def test_ua_truncated_to_256(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.get("/items/1", headers={"User-Agent": "u" * 300})

    assert lines(records)[-1]["ua"] == "u" * 256


@pytest.mark.asyncio
async def test_sender_and_auth_when_handler_sets_it(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.get("/claimed")

    line = lines(records)[-1]
    assert line["sender"] == "0xabc"
    assert line["auth"] == "claimed"


@pytest.mark.asyncio
async def test_http_exception_logged_with_its_status(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    response = await client.get("/boom")

    assert response.status == 422
    assert len(records) == 1
    line = lines(records)[0]
    assert line["status"] == 422
    assert line["route"] == "/boom"
    assert line["bytes_out"] == response.content_length


@pytest.mark.asyncio
async def test_unmatched_route_logged(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    await client.get("/nope/abc")

    line = lines(records)[-1]
    assert line["status"] == 404
    assert line["route"] == "unmatched"
    assert line["path"] == "/nope/abc"


@pytest.mark.asyncio
async def test_unhandled_exception_logged_as_500(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    response = await client.get("/crash")

    assert response.status == 500
    assert len(records) == 1
    line = lines(records)[0]
    assert line["status"] == 500
    assert line["bytes_out"] == 0


@pytest.mark.asyncio
async def test_handler_timeout_logged_as_504(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    response = await client.get("/timeout")

    assert response.status == 504
    assert [line["status"] for line in lines(records)] == [504]


@pytest.mark.asyncio
async def test_cancelled_request_logged_as_499(aiohttp_client, app, records):
    client = await aiohttp_client(app)

    # aiohttp's TestServer runs with handler_cancellation=True, so the client
    # giving up cancels the handler. gunicorn's aiohttp worker leaves it False:
    # in production a disconnect does not cancel the handler, a shutdown does.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.get("/slow"), 0.2)

    for _ in range(100):
        if records:
            break
        await asyncio.sleep(0.02)

    assert [line["status"] for line in lines(records)] == [499]


def test_setup_request_logging_is_idempotent_and_does_not_propagate():
    logger = logging.getLogger(REQUEST_LOGGER_NAME)
    saved = (list(logger.handlers), logger.level, logger.propagate)
    try:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

        setup_request_logging()
        setup_request_logging()

        assert len(logger.handlers) == 1
        handler = logger.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is sys.stdout
        assert handler.formatter is not None
        assert handler.formatter._fmt == "%(message)s"
        assert logger.level == logging.INFO
        assert logger.propagate is False
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
        handlers, level, propagate = saved
        for handler in handlers:
            logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = propagate


def test_setup_request_logging_writes_bare_json_line(capsys):
    logger = logging.getLogger(REQUEST_LOGGER_NAME)
    saved = (list(logger.handlers), logger.level, logger.propagate)
    try:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
        setup_request_logging()

        logger.info('{"k":1}')

        assert capsys.readouterr().out == '{"k":1}\n'
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
        handlers, level, propagate = saved
        for handler in handlers:
            logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = propagate
