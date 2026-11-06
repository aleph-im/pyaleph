"""
Tests for the HTTP analytics middleware: per-route Prometheus metrics and the
X-Request-Id contract.

The metrics live on prometheus_client's process-global default registry, so
every assertion compares a value after the requests with the value before.
"""

import asyncio
import re
from typing import Dict, Optional

import pytest
from aiohttp import web
from prometheus_client import REGISTRY as DEFAULT_REGISTRY

from aleph.web.middlewares.analytics import (
    HTTP_REQUESTS,
    REQUEST_ID_HEADER,
    analytics_middleware,
    set_request_id_header,
)

ITEMS_ROUTE = "/items/{item_id}"
REQUEST_ID_RE = re.compile(r"[0-9a-f]{32}")


def sample_value(name: str, **labels: str) -> float:
    value: Optional[float] = DEFAULT_REGISTRY.get_sample_value(name, labels)
    return value or 0.0


def counter_value(**labels: str) -> float:
    return sample_value("aleph_http_requests_total", **labels)


def in_flight_value(route: str) -> float:
    return sample_value("aleph_http_requests_in_flight", route=route)


def route_labels_of_requests_counter() -> set:
    return {
        sample.labels["route"]
        for metric in HTTP_REQUESTS.collect()
        for sample in metric.samples
        if "route" in sample.labels
    }


async def items_handler(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def boom_handler(request: web.Request) -> web.Response:
    raise web.HTTPUnprocessableEntity()


async def crash_handler(request: web.Request) -> web.Response:
    raise RuntimeError("crash")


async def stream_handler(request: web.Request) -> web.StreamResponse:
    response = web.StreamResponse()
    await response.prepare(request)
    await response.write(b"first chunk ")
    await response.write(b"second chunk")
    await response.write_eof()
    return response


async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await ws.close()
    return ws


async def slow_handler(request: web.Request) -> web.Response:
    await asyncio.sleep(30)
    return web.json_response({"ok": True})


async def timeout_handler(request: web.Request) -> web.Response:
    raise asyncio.TimeoutError()


async def req_id_handler(request: web.Request) -> web.Response:
    return web.json_response({"req_id": request["req_id"]})


async def in_flight_handler(request: web.Request) -> web.Response:
    return web.json_response({"in_flight": in_flight_value("/in-flight")})


@pytest.fixture
def app() -> web.Application:
    app = web.Application(middlewares=[analytics_middleware])
    app.on_response_prepare.append(set_request_id_header)
    app.router.add_get(ITEMS_ROUTE, items_handler)
    app.router.add_get("/boom", boom_handler)
    app.router.add_get("/crash", crash_handler)
    app.router.add_get("/stream", stream_handler)
    app.router.add_get("/req-id", req_id_handler)
    app.router.add_get("/in-flight", in_flight_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/slow", slow_handler)
    app.router.add_get("/timeout", timeout_handler)
    return app


@pytest.mark.asyncio
async def test_route_label_is_the_template(aiohttp_client, app):
    client = await aiohttp_client(app)
    labels: Dict[str, str] = dict(route=ITEMS_ROUTE, method="GET", status="200")
    before = counter_value(**labels)

    assert (await client.get("/items/1")).status == 200
    assert (await client.get("/items/2")).status == 200

    assert counter_value(**labels) - before == 2
    assert not any(
        label.startswith("/items/") and label != ITEMS_ROUTE
        for label in route_labels_of_requests_counter()
    )


@pytest.mark.asyncio
async def test_unmatched_route_is_bounded(aiohttp_client, app):
    client = await aiohttp_client(app)
    not_found = dict(route="unmatched", method="GET", status="404")
    not_allowed = dict(route="unmatched", method="POST", status="405")
    before_404 = counter_value(**not_found)
    before_405 = counter_value(**not_allowed)

    assert (await client.get("/nope/abc")).status == 404
    assert (await client.post("/items/1")).status == 405

    assert counter_value(**not_found) - before_404 == 1
    assert counter_value(**not_allowed) - before_405 == 1
    assert not any("nope" in label for label in route_labels_of_requests_counter())


@pytest.mark.asyncio
async def test_http_exception_is_counted_and_propagated(aiohttp_client, app):
    client = await aiohttp_client(app)
    labels = dict(route="/boom", method="GET", status="422")
    before = counter_value(**labels)

    response = await client.get("/boom")

    assert response.status == 422
    assert re.fullmatch(r"[0-9a-f]{32}", response.headers[REQUEST_ID_HEADER])
    assert counter_value(**labels) - before == 1


@pytest.mark.asyncio
async def test_unhandled_exception_counts_500_and_reraises(aiohttp_client, app):
    client = await aiohttp_client(app)
    labels = dict(route="/crash", method="GET", status="500")
    before = counter_value(**labels)
    in_flight_before = in_flight_value("/crash")

    response = await client.get("/crash")

    assert response.status == 500
    assert REQUEST_ID_RE.fullmatch(response.headers[REQUEST_ID_HEADER])
    assert counter_value(**labels) - before == 1
    assert in_flight_value("/crash") == in_flight_before


@pytest.mark.asyncio
async def test_stream_response_counts_zero_bytes_without_error(aiohttp_client, app):
    client = await aiohttp_client(app)
    labels = dict(route="/stream", method="GET", status="200")
    before = counter_value(**labels)
    bytes_before = sample_value("aleph_http_response_bytes_total", route="/stream")

    response = await client.get("/stream")

    assert response.status == 200
    assert await response.read() == b"first chunk second chunk"
    assert REQUEST_ID_RE.fullmatch(response.headers[REQUEST_ID_HEADER])
    assert counter_value(**labels) - before == 1
    # A streamed response has no Content-Length: nothing is added.
    assert (
        sample_value("aleph_http_response_bytes_total", route="/stream") == bytes_before
    )
    assert in_flight_value("/stream") == 0


@pytest.mark.asyncio
async def test_response_and_request_bytes_are_counted(aiohttp_client, app):
    client = await aiohttp_client(app)
    out_before = sample_value("aleph_http_response_bytes_total", route=ITEMS_ROUTE)
    in_before = sample_value("aleph_http_request_bytes_total", route="unmatched")

    response = await client.get("/items/1")
    body = await response.read()
    await client.post("/items/1", data=b"0123456789")

    assert sample_value(
        "aleph_http_response_bytes_total", route=ITEMS_ROUTE
    ) - out_before == len(body)
    assert (
        sample_value("aleph_http_request_bytes_total", route="unmatched") - in_before
        == 10
    )


@pytest.mark.asyncio
async def test_in_flight_returns_to_zero(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/in-flight")

    assert (await response.json())["in_flight"] == 1
    assert in_flight_value("/in-flight") == 0


@pytest.mark.asyncio
async def test_request_id_generated_and_returned(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/req-id")

    req_id = response.headers[REQUEST_ID_HEADER]
    assert re.fullmatch(r"[0-9a-f]{32}", req_id)
    assert (await response.json())["req_id"] == req_id


@pytest.mark.asyncio
async def test_valid_client_request_id_is_echoed(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/req-id", headers={REQUEST_ID_HEADER: "abc-123_x.y"})

    assert response.headers[REQUEST_ID_HEADER] == "abc-123_x.y"
    assert (await response.json())["req_id"] == "abc-123_x.y"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["", "x" * 65, "has space", "ünïcode", "a;b"])
async def test_invalid_client_request_id_is_replaced(aiohttp_client, app, bad):
    client = await aiohttp_client(app)

    response = await client.get("/req-id", headers={REQUEST_ID_HEADER: bad})

    req_id = response.headers[REQUEST_ID_HEADER]
    assert req_id != bad
    assert re.fullmatch(r"[0-9a-f]{32}", req_id)
    assert (await response.json())["req_id"] == req_id


@pytest.mark.asyncio
async def test_duration_histogram_observed(aiohttp_client, app):
    client = await aiohttp_client(app)
    labels = dict(route=ITEMS_ROUTE, method="GET")
    before = sample_value("aleph_http_request_duration_seconds_count", **labels)

    for item_id in range(3):
        await client.get(f"/items/{item_id}")

    after = sample_value("aleph_http_request_duration_seconds_count", **labels)
    assert after - before == 3
    assert (
        sample_value("aleph_http_request_duration_seconds_bucket", le="30.0", **labels)
        >= 3
    )


@pytest.mark.asyncio
async def test_websocket_handshake_carries_request_id(aiohttp_client, app):
    client = await aiohttp_client(app)
    headers = {
        "Connection": "Upgrade",
        "Upgrade": "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
        REQUEST_ID_HEADER: "ws-req-1",
    }

    response = await asyncio.wait_for(client.get("/ws", headers=headers), 5)

    assert response.status == 101
    assert response.headers[REQUEST_ID_HEADER] == "ws-req-1"
    response.close()


@pytest.mark.asyncio
async def test_cancelled_request_counts_499_not_500(aiohttp_client, app):
    client = await aiohttp_client(app)
    cancelled = dict(route="/slow", method="GET", status="499")
    errored = dict(route="/slow", method="GET", status="500")
    before_499 = counter_value(**cancelled)
    before_500 = counter_value(**errored)

    # The client gives up; the test server cancels the handler on disconnect.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.get("/slow"), 0.2)

    for _ in range(100):
        if counter_value(**cancelled) - before_499 >= 1:
            break
        await asyncio.sleep(0.02)

    assert counter_value(**cancelled) - before_499 == 1
    assert counter_value(**errored) == before_500
    assert in_flight_value("/slow") == 0


@pytest.mark.asyncio
async def test_handler_timeout_counts_504(aiohttp_client, app):
    client = await aiohttp_client(app)
    labels = dict(route="/timeout", method="GET", status="504")
    before = counter_value(**labels)

    response = await client.get("/timeout")

    assert response.status == 504
    assert counter_value(**labels) - before == 1
    assert counter_value(route="/timeout", method="GET", status="500") == 0
