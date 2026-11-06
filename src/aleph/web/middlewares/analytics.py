"""
HTTP API analytics: per-route Prometheus metrics and the X-Request-Id contract.

Every request is labelled with the template of the route it matched
(``/api/v0/messages/{item_hash}``), never with its raw path, so the label
cardinality stays bounded by the number of registered routes. Requests that
match no route (404, 405) share the ``unmatched`` label. The ``method`` label
is the request method if it is a standard one, else ``OTHER``: the HTTP parser
accepts any token as a method, so the label would otherwise be unbounded.

The metrics are created on prometheus_client's default registry, as its
multiprocess mode requires. ``make_registry()`` returns the registry to expose
on ``/metrics``: the default registry in a single process, or a fresh registry
aggregating the files of every worker when ``PROMETHEUS_MULTIPROC_DIR`` is set.

``PROMETHEUS_MULTIPROC_DIR`` must be in the environment before anything
imports prometheus_client: the library picks its value class (in-memory or
mmap file) when it is first imported. This module also cannot be reloaded:
re-creating the metrics on the default registry raises a duplicate timeseries
error. Test multiprocess mode in a fresh interpreter, not with a reload.

The middleware sets X-Request-Id on the responses it sees before they are
sent. Responses prepared elsewhere (a handler that streams or opens a
WebSocket, the 500 aiohttp builds for an unhandled exception) need
``set_request_id_header`` registered on ``app.on_response_prepare``.

Once a request is over, the middleware also writes one JSON line on the
``aleph.http`` logger (see ``request_log``), whatever the outcome.
"""

import asyncio
import logging
import os
import re
import time
import uuid
from typing import Awaitable, Callable, Optional

from aiohttp import web
from prometheus_client import REGISTRY as DEFAULT_REGISTRY
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, multiprocess

from aleph.web.middlewares.request_log import REQUEST_LOGGER_NAME, format_request_log

REQUEST_ID_HEADER = "X-Request-Id"
UNMATCHED_ROUTE = "unmatched"
OTHER_METHOD = "OTHER"
KNOWN_METHODS = frozenset(("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"))

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,64}")

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]

LOGGER = logging.getLogger(__name__)
REQUEST_LOGGER = logging.getLogger(REQUEST_LOGGER_NAME)


def make_registry() -> CollectorRegistry:
    """
    Return the registry to serve on /metrics.

    In multiprocess mode (PROMETHEUS_MULTIPROC_DIR set), this is a fresh
    registry with a MultiProcessCollector that sums the metric files of all
    worker processes. Otherwise, it is the default registry on which the
    metrics of this module are created.
    """
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return registry
    return DEFAULT_REGISTRY


HTTP_REQUESTS = Counter(
    "aleph_http_requests_total",
    "HTTP requests handled, by route template, method and status code.",
    labelnames=("route", "method", "status"),
    registry=DEFAULT_REGISTRY,
)
HTTP_REQUEST_DURATION = Histogram(
    "aleph_http_request_duration_seconds",
    "Time spent handling HTTP requests, by route template and method.",
    labelnames=("route", "method"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
    registry=DEFAULT_REGISTRY,
)
HTTP_IN_FLIGHT = Gauge(
    "aleph_http_requests_in_flight",
    "HTTP requests currently being handled, by route template.",
    labelnames=("route",),
    multiprocess_mode="livesum",
    registry=DEFAULT_REGISTRY,
)
HTTP_RESPONSE_BYTES = Counter(
    "aleph_http_response_bytes_total",
    "Bytes sent in HTTP response bodies with a known length, by route template.",
    labelnames=("route",),
    registry=DEFAULT_REGISTRY,
)
HTTP_REQUEST_BYTES = Counter(
    "aleph_http_request_bytes_total",
    "Bytes received in HTTP request bodies, by route template.",
    labelnames=("route",),
    registry=DEFAULT_REGISTRY,
)


def route_label(request: web.Request) -> str:
    """The canonical template of the matched route, or "unmatched"."""
    resource = request.match_info.route.resource
    if resource is None:
        return UNMATCHED_ROUTE
    return resource.canonical


def method_label(method: str) -> str:
    """The method for the metric labels: standard methods as is, else "OTHER"."""
    return method if method in KNOWN_METHODS else OTHER_METHOD


def request_id_from(request: web.Request) -> str:
    """The client's X-Request-Id if well-formed, else a new random id."""
    candidate = request.headers.get(REQUEST_ID_HEADER, "")
    if _REQUEST_ID_PATTERN.fullmatch(candidate):
        return candidate
    return uuid.uuid4().hex


async def set_request_id_header(
    request: web.Request, response: web.StreamResponse
) -> None:
    """
    on_response_prepare signal handler: add X-Request-Id to every response
    whose headers are about to be sent, including streamed responses,
    WebSocket handshakes and the 500 aiohttp builds for unhandled errors.
    """
    req_id = request.get("req_id")
    if req_id:
        response.headers.setdefault(REQUEST_ID_HEADER, req_id)


@web.middleware
async def analytics_middleware(
    request: web.Request, handler: Handler
) -> web.StreamResponse:
    req_id = request_id_from(request)
    route = route_label(request)
    method = method_label(request.method)
    request["req_id"] = req_id
    request["analytics_route"] = route

    response: Optional[web.StreamResponse] = None
    status = 500
    start = time.monotonic()
    HTTP_IN_FLIGHT.labels(route).inc()
    try:
        response = await handler(request)
    except web.HTTPException as exc:
        # aiohttp HTTP exceptions are responses: they carry the status, the
        # body length and the headers sent to the client.
        response = exc
        status = exc.status
        raise
    except asyncio.CancelledError:
        # The handler was cancelled: at shutdown, or on client disconnect when
        # the server runs with handler_cancellation=True (aiohttp's test
        # server does; gunicorn's aiohttp worker does not). Use the nginx
        # "client closed request" code so it never counts as a 5xx.
        status = 499
        raise
    except asyncio.TimeoutError:
        # aiohttp answers a handler's TimeoutError with a 504.
        status = 504
        raise
    except Exception:
        status = 500
        raise
    else:
        status = response.status
        return response
    finally:
        duration = time.monotonic() - start
        request["analytics_status"] = status
        request["analytics_dur_ms"] = duration * 1000

        HTTP_IN_FLIGHT.labels(route).dec()
        HTTP_REQUEST_DURATION.labels(route, method).observe(duration)
        HTTP_REQUESTS.labels(route, method, str(status)).inc()
        # The body bytes actually received, not the client-declared
        # Content-Length, which can be anything.
        bytes_in = request.content.total_bytes
        bytes_out = 0
        HTTP_REQUEST_BYTES.labels(route).inc(bytes_in)
        if response is not None:
            bytes_out = response.content_length or 0
            HTTP_RESPONSE_BYTES.labels(route).inc(bytes_out)
            # A prepared (streamed) response has already sent its headers.
            if not response.prepared:
                response.headers.setdefault(REQUEST_ID_HEADER, req_id)

        # An error here must not replace the handler's response or exception.
        try:
            REQUEST_LOGGER.info(
                format_request_log(
                    request,
                    status=status,
                    dur_ms=duration * 1000,
                    bytes_in=bytes_in,
                    bytes_out=bytes_out,
                ).decode()
            )
        except Exception:
            LOGGER.exception("Could not write the request log line")
