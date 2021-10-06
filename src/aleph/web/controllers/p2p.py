import asyncio
import logging
from typing import List

import aiohttp
from aiohttp import web

from aleph.model.messages import Message
from aleph.services.ipfs.pubsub import pub as pub_ipfs
from aleph.services.p2p import pub as pub_p2p
from aleph.types import Protocol
from aleph.web import app

LOGGER = logging.getLogger("web.controllers.p2p")


async def get_user_usage(address: str):
    # ETH
    messages = [
        msg async for msg
        in Message.collection.find({
            'content.address': address,
            'type': 'STORE',
        },
            limit=10000,
            # skip=pagination_skip,
            sort=[('time', -1)]
        )
    ]

    # TODO: Check 'sender' is allowed (VM runner, ...)
    pass


async def get_user_balance(address: str) -> float:
    # ETH
    API_url = f"https://api.ethplorer.io/getAddressInfo/{address}?apiKey=freekey"
    async with aiohttp.ClientSession() as client:
        async with client.get(API_url) as resp:
            resp.raise_for_status()
            result = resp.json()
            for token in result["tokens"]:
                if token["tokenInfo"]["address"] == "0x27702a26126e0b3702af63ee09ac4d1a084ef628":
                    assert token["tokenInfo"]["symbol"] == "ALEPH"
                    balance = int(token["rawBalance"])
                    decimals = token["tokenInfo"]["decimals"]
                    return balance / 10 ** decimals


async def get_user_quota(address: str) -> float:
    balance = get_user_balance(address)
    mb_per_aleph = 3
    return balance * mb_per_aleph


async def pub_json(request):
    """Forward the message to P2P host and IPFS server as a pubsub message"""
    data = await request.json()
    topic: str = data["topic"]
    message = data["data"]
    failed_publications: List[str] = []

    # Check that emitter has enough credit
    address: str = message["content"]["address"]
    size: int = message["content"]["size"]
    usage = await get_user_usage(address)
    quota = await get_user_quota(address)
    if (size + usage) > quota:
        return web.HTTPPaymentRequired(reason="Not hold enough tokens",
                                       text="This address does not hold enough Aleph tokens.\n"
                                            f"Usage: {usage} / {quota}\nNew file: {size}")

    try:
        if app["config"].ipfs.enabled.value:
            await asyncio.wait_for(pub_ipfs(topic, message), timeout=0.2)
    except Exception:
        LOGGER.exception("Can't publish on ipfs")
        failed_publications.append(Protocol.IPFS)

    try:
        await asyncio.wait_for(pub_p2p(topic, message), timeout=0.5)
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
