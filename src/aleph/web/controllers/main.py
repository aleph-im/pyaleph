import asyncio
import logging
from dataclasses import asdict
from typing import Dict

import aiohttp_jinja2
import pkg_resources
from aiohttp import web

from aleph import __version__
from aleph.web import app
from aleph.web.controllers.metrics import format_dataclass_for_prometheus, get_metrics

logger = logging.getLogger(__name__)


app.router.add_static(
    "/static/",
    path=pkg_resources.resource_filename("aleph.web", "static/"),
    name="static",
)


@aiohttp_jinja2.template("index.html")
async def index(request) -> Dict:
    """Index of aleph."""
    shared_stats = request.config_dict["shared_stats"]
    return asdict(await get_metrics(shared_stats))


app.router.add_get("/", index)


async def version(request):
    """Version endpoint."""

    response = web.json_response({"version": __version__})
    return response


app.router.add_get("/version", version)
app.router.add_get("/api/v0/version", version)


async def status_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    previous_status = None
    while True:
        shared_stats = request.config_dict["shared_stats"]
        status = await get_metrics(shared_stats)

        if status != previous_status:
            try:
                await ws.send_json(asdict(status))
            except ConnectionResetError:
                logger.warning("Websocket connection reset")
                await ws.close()
                return
            previous_status = status

        await asyncio.sleep(2)


app.router.add_get("/api/ws0/status", status_ws)


async def metrics(request):
    """Prometheus compatible metrics.

    Naming convention:
    https://prometheus.io/docs/practices/naming/
    """
    shared_stats = request.config_dict["shared_stats"]
    return web.Response(
        text=format_dataclass_for_prometheus(await get_metrics(shared_stats))
    )


app.router.add_get("/metrics", metrics)


async def metrics_json(request):
    """JSON version of the Prometheus metrics."""
    shared_stats = request.config_dict["shared_stats"]
    return web.Response(
        text=(await get_metrics(shared_stats)).to_json(),
        content_type="application/json",
    )


app.router.add_get("/metrics.json", metrics_json)
