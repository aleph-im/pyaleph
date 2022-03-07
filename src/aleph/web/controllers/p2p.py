import asyncio
import json
import logging
from typing import List

import aiohttp
from aiohttp import web
from aleph.services.p2p.singleton import get_p2p_client

from aleph.model.messages import Message
from aleph.services.ipfs.common import get_ipfs_api
from aleph.services.ipfs.pubsub import pub as pub_ipfs
from aleph.services.p2p.pubsub import publish as pub_p2p
from aleph.types import Protocol
from aleph.chains.balance.ethplorer import EthplorerBalanceChecker
from aleph.web import app

LOGGER = logging.getLogger("web.controllers.p2p")


async def get_user_usage(address: str) -> float:
    # ETH
    LOGGER.info("get_user_usage")
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
    LOGGER.info("MESSAGES count {}".format(len(messages)))

    # TODO: Check 'sender' is allowed (VM runner, ...)

    usage = sum(
        max(message['content'].get('size', 0), 0)
        for message in messages
    )
    assert usage >= 0
    return usage


async def get_user_balance(address: str) -> float:
    # TODO: Support ALEPH on chains other than ETH
    balance_checker = EthplorerBalanceChecker(api_key="freekey")
    return await balance_checker.get_balance(address)


def balance_to_quota(balance: float) -> float:
    mb_per_aleph = 3
    return balance * mb_per_aleph


async def get_user_quota(address: str) -> float:
    balance = await get_user_balance(address)
    return balance_to_quota(balance)


async def view_usage(request: web.Request):
    address = request.query.get('address')
    if not address:
        raise web.HTTPBadRequest(reason="Address required")
    balance = await get_user_balance(address)
    return web.json_response({
        'usage': await get_user_usage(address),
        'balance': balance,
        'quota': balance_to_quota(balance),
    })

app.router.add_get('/api/v0/address/usage', view_usage)


async def pub_json(request):
    """Forward the message to P2P host and IPFS server as a pubsub message"""
    data = await request.json()
    topic: str = data["topic"]
    message = data["data"]

    print(message)
    LOGGER.info(message)
    msg = json.loads(message)
    msg['content'] = json.loads(msg['item_content'])
    failed_publications: List[str] = []

    # Check that emitter has enough credit
    address: str = msg["content"]["address"]
    item_hash = msg["content"]['item_hash']  # TODO: Use get_message_content(message) ?
    api = await get_ipfs_api(timeout=5)
    stats = await asyncio.wait_for(
        api.files.stat(f"/ipfs/{item_hash}"), 5)
    size = stats['CumulativeSize']

    # size: int = msg["content"]["size"]
    usage = await get_user_usage(address)
    quota = await get_user_quota(address)
    assert usage >= 0
    assert quota >= 0
    if (size + usage) > quota:
        return web.HTTPPaymentRequired(reason="Not enough tokens",
                                       text="This address does not hold enough Aleph tokens.\n"
                                            f"Usage: {usage} / {quota}\nNew file: {size}")

    try:
        if app["config"].ipfs.enabled.value:
            await asyncio.wait_for(pub_ipfs(topic, message), timeout=0.2)
    except Exception:
        LOGGER.exception("Can't publish on ipfs")
        failed_publications.append(Protocol.IPFS)

    try:
        p2p_client = get_p2p_client()
        await asyncio.wait_for(pub_p2p(p2p_client, topic, message), timeout=0.5)
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
