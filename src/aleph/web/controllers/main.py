import asyncio
import logging
from dataclasses import asdict
from typing import Dict, List, Optional, Set

import aiohttp_jinja2
from aiohttp import WSCloseCode, WSMsgType, web
from pydantic import BaseModel, ValidationError

from aleph.db.accessors.metrics import query_metric_ccn, query_metric_crn
from aleph.services.cache.node_cache import NodeCache
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrderForMetrics
from aleph.version import __version__
from aleph.web.controllers.app_state_getters import (
    APP_STATE_MESSAGE_BROADCASTER,
    APP_STATE_STATUS_BROADCASTER,
    get_config_from_request,
    get_node_cache_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.metrics import (
    format_dataclass_for_prometheus,
    get_metrics,
    get_metrics_with_ws,
)

logger = logging.getLogger(__name__)

_STATUS_SEND_BATCH_SIZE = 100

# Redis keys for WS status metrics. Shared across gunicorn workers so
# Prometheus sees cluster-wide state regardless of which worker serves /metrics.
WS_STATUS_CONNECTIONS_ACTIVE_KEY = "pyaleph_ws_status_connections_active"
WS_STATUS_CONNECTIONS_REJECTED_KEY = "pyaleph_ws_status_connections_rejected_total"


class StatusBroadcaster:
    """Single polling loop that broadcasts status to all connected WS clients."""

    def __init__(
        self,
        session_factory: DbSessionFactory,
        node_cache: NodeCache,
        max_connections: int = 1000,
        poll_interval: float = 10.0,
    ):
        self._session_factory = session_factory
        self._node_cache = node_cache
        self._clients: Set[web.WebSocketResponse] = set()
        self._task: Optional[asyncio.Task] = None
        self._poll_interval = poll_interval

        # Connection limit (same on every worker — from config).
        self.max_connections: int = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)
        # Note: counter state lives in Redis (see WS_*_KEY constants). This
        # class never holds it locally so that all gunicorn workers share
        # the same observed values.

    @property
    def is_at_capacity(self) -> bool:
        return self._semaphore.locked()

    def acquire_slot(self) -> asyncio.Semaphore:
        return self._semaphore

    async def record_rejection(self) -> None:
        """Increment the shared 'connection rejected' counter."""
        await self._node_cache.incr(WS_STATUS_CONNECTIONS_REJECTED_KEY)

    async def add(self, ws: web.WebSocketResponse):
        if ws in self._clients:
            return
        self._clients.add(ws)
        await self._node_cache.incr(WS_STATUS_CONNECTIONS_ACTIVE_KEY)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll_loop())

    async def remove(self, ws: web.WebSocketResponse):
        # Guard against double-remove: Redis DECR must be idempotent-safe.
        if ws not in self._clients:
            return
        self._clients.discard(ws)
        await self._node_cache.decr(WS_STATUS_CONNECTIONS_ACTIVE_KEY)

    async def shutdown(self):
        """Graceful shutdown — called from aiohttp on_cleanup."""
        if self._task and not self._task.done():
            self._task.cancel()
            self._task = None
        # Decrement the shared active counter once per client this worker
        # owned, so the Redis value reflects only clients still connected
        # through other workers.
        if self._clients:
            await self._node_cache.decrby(
                WS_STATUS_CONNECTIONS_ACTIVE_KEY, len(self._clients)
            )
            self._clients.clear()

    async def _poll_loop(self):
        previous_status = None
        while self._clients:
            try:
                status = await get_metrics(
                    session_factory=self._session_factory,
                    node_cache=self._node_cache,
                )
            except Exception:
                logger.exception("Failed to get metrics for status broadcast")
                await asyncio.sleep(self._poll_interval)
                continue

            if status != previous_status:
                payload = asdict(status)
                clients = list(self._clients)
                dead: List[web.WebSocketResponse] = []

                for i in range(0, len(clients), _STATUS_SEND_BATCH_SIZE):
                    batch = clients[i : i + _STATUS_SEND_BATCH_SIZE]
                    results = await asyncio.gather(
                        *[self._send(ws, payload) for ws in batch],
                        return_exceptions=True,
                    )
                    for ws, ok in zip(batch, results):
                        if ok is not True:
                            dead.append(ws)

                for ws in dead:
                    await self.remove(ws)
                previous_status = status

            await asyncio.sleep(self._poll_interval)

    @staticmethod
    async def _send(ws: web.WebSocketResponse, payload: dict) -> bool:
        if ws.closed:
            return False
        try:
            await ws.send_json(payload)
            return True
        except (ConnectionResetError, ConnectionError):
            return False


async def _get_full_metrics(request: web.Request):
    """Fetch metrics including live WS stats from broadcasters."""
    session_factory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)
    return await get_metrics_with_ws(
        session_factory=session_factory,
        node_cache=node_cache,
        message_broadcaster=request.app.get(APP_STATE_MESSAGE_BROADCASTER),
        status_broadcaster=request.app.get(APP_STATE_STATUS_BROADCASTER),
    )


@aiohttp_jinja2.template("index.html")
async def index(request: web.Request) -> Dict:
    """Index of aleph."""

    model = asdict(await _get_full_metrics(request))
    model["version"] = __version__
    return model


async def status_ws(request: web.Request) -> web.WebSocketResponse:
    config = get_config_from_request(request)
    heartbeat = config.websocket.heartbeat.value

    ws = web.WebSocketResponse(heartbeat=float(heartbeat))
    await ws.prepare(request)

    broadcaster: StatusBroadcaster = request.app[APP_STATE_STATUS_BROADCASTER]

    if broadcaster.is_at_capacity:
        await broadcaster.record_rejection()
        logger.warning(
            "Status WS connection limit reached (%d)", broadcaster.max_connections
        )
        await ws.close(
            code=WSCloseCode.TRY_AGAIN_LATER,
            message=b"Too many connections",
        )
        return ws

    async with broadcaster.acquire_slot():
        await broadcaster.add(ws)

        try:
            while not ws.closed:
                ws_msg = await ws.receive()
                if ws_msg.type in (
                    WSMsgType.CLOSE,
                    WSMsgType.ERROR,
                    WSMsgType.CLOSING,
                ):
                    break
        finally:
            await broadcaster.remove(ws)
            if not ws.closed:
                await ws.close()

    return ws


async def metrics(request: web.Request) -> web.Response:
    """
    Prometheus compatible metrics.

    ---
    summary: Prometheus metrics
    tags:
      - Metrics
    responses:
      '200':
        description: Prometheus-formatted metrics
        content:
          text/plain:
            schema:
              type: string
    """
    return web.Response(
        text=format_dataclass_for_prometheus(await _get_full_metrics(request))
    )


async def metrics_json(request: web.Request) -> web.Response:
    """
    JSON version of the Prometheus metrics.

    ---
    summary: Metrics (JSON)
    tags:
      - Metrics
    responses:
      '200':
        description: Metrics in JSON format
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/MetricsResponse'
    """
    return web.Response(
        text=(await _get_full_metrics(request)).to_json(),
        content_type="application/json",
    )


class MetricsQueryParams(BaseModel):
    start_date: Optional[float] = None
    end_date: Optional[float] = None
    sort: Optional[SortOrderForMetrics] = None


def _get_node_id_from_request(request: web.Request) -> str:
    address = request.match_info.get("node_id")
    if address is None:
        raise web.HTTPUnprocessableEntity(body="node_id must be specified.")
    return address


async def ccn_metric(request: web.Request) -> web.Response:
    """
    Fetch metrics for a CCN node.

    ---
    summary: Get CCN node metrics
    tags:
      - Metrics
    parameters:
      - name: node_id
        in: path
        required: true
        schema:
          type: string
      - name: start_date
        in: query
        schema:
          type: number
      - name: end_date
        in: query
        schema:
          type: number
      - name: sort
        in: query
        schema:
          type: string
    responses:
      '200':
        description: CCN node metrics
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/NodeMetricsResponse'
      '404':
        description: Node not found
      '422':
        description: Validation error
    """

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    try:
        query_params = MetricsQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    node_id = _get_node_id_from_request(request)

    with session_factory() as session:
        ccn = query_metric_ccn(
            session,
            node_id=node_id,
            start_date=query_params.start_date,
            end_date=query_params.end_date,
            sort_order=query_params.sort,
        )
        if not ccn:
            raise web.HTTPNotFound()

        if not ccn["item_hash"]:
            raise web.HTTPNotFound()

        result = {"metrics": ccn}
        return web.json_response(result)


async def crn_metric(request: web.Request) -> web.Response:
    """
    Fetch metrics for a CRN node.

    ---
    summary: Get CRN node metrics
    tags:
      - Metrics
    parameters:
      - name: node_id
        in: path
        required: true
        schema:
          type: string
      - name: start_date
        in: query
        schema:
          type: number
      - name: end_date
        in: query
        schema:
          type: number
      - name: sort
        in: query
        schema:
          type: string
    responses:
      '200':
        description: CRN node metrics
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/NodeMetricsResponse'
      '404':
        description: Node not found
      '422':
        description: Validation error
    """

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    try:
        query_params = MetricsQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    node_id = _get_node_id_from_request(request)

    with session_factory() as session:
        crn = query_metric_crn(
            session,
            node_id=node_id,
            start_date=query_params.start_date,
            end_date=query_params.end_date,
            sort_order=query_params.sort,
        )

        if not crn:
            raise web.HTTPNotFound()

        if not crn["item_hash"]:
            raise web.HTTPNotFound()

        result = {"metrics": crn}
        return web.json_response(result)
