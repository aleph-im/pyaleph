import asyncio
import logging

from aiohttp import web

from aleph.services.ipfs.pubsub import pub as pub_ipfs
from aleph.services.p2p.pubsub import pub as pub_p2p
from aleph.types import Protocol
from aleph.web import app

LOGGER = logging.getLogger("web.controllers.p2p")


async def pub_json(request):
    """Forward the message to P2P host and IPFS server as a pubsub message"""
    data = await request.json()
    failed_publications = []

    try:
        if app["config"].ipfs.enabled.value:
            await asyncio.wait_for(pub_ipfs(data.get("topic"), data.get("data")), 1)
    except Exception:
        LOGGER.exception("Can't publish on ipfs")
        failed_publications.append(Protocol.IPFS)

    try:
        await asyncio.wait_for(pub_p2p(data.get("topic"), data.get("data")), 0.5)
    except Exception:
        LOGGER.exception("Can't publish on p2p")
        failed_publications.append(Protocol.P2P)

    status = {
        0: "success",
        1: "warning",
        2: "error",
    }[len(failed_publications)]

    return web.json_response(
        {"status": status, "failed": failed_publications},
        status=500 if status == "error" else 200,
    )


app.router.add_post("/api/v0/ipfs/pubsub/pub", pub_json)
app.router.add_post("/api/v0/p2p/pubsub/pub", pub_json)
