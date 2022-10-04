import asyncio
import logging
from dataclasses import asdict
from typing import Dict

import aiohttp_jinja2
from aiohttp import web

from aleph.types.db_session import DbSessionFactory
from aleph.web.controllers.metrics import format_dataclass_for_prometheus, get_metrics

logger = logging.getLogger(__name__)


@aiohttp_jinja2.template("index.html")
async def index(request) -> Dict:
    """Index of aleph."""
    shared_stats = request.config_dict["shared_stats"]
    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        return asdict(await get_metrics(session=session, shared_stats=shared_stats))


async def status_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    session_factory: DbSessionFactory = request.app["session_factory"]

    previous_status = None
    while True:
        shared_stats = request.config_dict["shared_stats"]

        with session_factory() as session:
            status = await get_metrics(session=session, shared_stats=shared_stats)

        if status != previous_status:
            try:
                await ws.send_json(asdict(status))
            except ConnectionResetError:
                logger.warning("Websocket connection reset")
                await ws.close()
                return
            previous_status = status

        await asyncio.sleep(2)


async def metrics(request):
    """Prometheus compatible metrics.

    Naming convention:
    https://prometheus.io/docs/practices/naming/
    """
    shared_stats = request.config_dict["shared_stats"]
    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        return web.Response(
            text=format_dataclass_for_prometheus(
                await get_metrics(session=session, shared_stats=shared_stats)
            )
        )


async def metrics_json(request):
    """JSON version of the Prometheus metrics."""
    shared_stats = request.config_dict["shared_stats"]
    session_factory: DbSessionFactory = request.app["session_factory"]

    with session_factory() as session:
        return web.Response(
            text=(await get_metrics(session=session, shared_stats=shared_stats)).to_json(),
            content_type="application/json",
        )
