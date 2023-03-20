import asyncio
import json
import logging
from typing import Dict, cast

from aiohttp import web
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config

from aleph.exceptions import InvalidMessageError
from aleph.schemas.pending_messages import parse_message
from aleph.services.ipfs.pubsub import pub as pub_ipfs
from aleph.services.p2p.pubsub import publish as pub_p2p
from aleph.types.protocol import Protocol

LOGGER = logging.getLogger("web.controllers.p2p")


def validate_request_data(config: Config, request_data: Dict) -> None:
    """
    Validates the content of a JSON pubsub message depending on the channel
    and raises a 422 error if the data does not match the expected format.

    :param config: Application configuration.
    :param request_data: Request JSON data, as a dictionary.
    """

    topic = request_data.get("topic")

    # Currently, we only check validate messages
    message_topic = config.aleph.queue_topic.value
    if topic == message_topic:
        message = json.loads(cast(str, request_data.get("data")))
        try:
            _ = parse_message(message)
        except InvalidMessageError as e:
            raise web.HTTPUnprocessableEntity(body=str(e))


async def pub_json(request: web.Request):
    """Forward the message to P2P host and IPFS server as a pubsub message"""
    request_data = await request.json()
    validate_request_data(config=request.app["config"], request_data=request_data)

    failed_publications = []

    try:
        if request.app["config"].ipfs.enabled.value:
            await asyncio.wait_for(
                pub_ipfs(request_data.get("topic"), request_data.get("data")), 1
            )
    except Exception:
        LOGGER.exception("Can't publish on ipfs")
        failed_publications.append(Protocol.IPFS)

    try:
        p2p_client: AlephP2PServiceClient = request.app["p2p_client"]
        await asyncio.wait_for(
            pub_p2p(
                p2p_client,
                request_data.get("topic"),
                request_data.get("data"),
                loopback=True,
            ),
            10,
        )
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
