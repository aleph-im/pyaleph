import aioipfs
import aiohttp
import asyncio
import orjson as json
import aiohttp
import concurrent
import logging
import base64
import base58

from .common import get_base_url
LOGGER = logging.getLogger("IPFS.PUBSUB")


async def decode_msg(msg):
    return {
        'from': base58.b58encode(
            base64.b64decode(msg['from'])),
        'data': base64.b64decode(msg['data']),
        'seqno': base58.b58encode(base64.b64decode(msg['seqno'])),
        'topicIDs': msg['topicIDs']
    }


async def sub(topic, base_url=None):
    from aleph.network import incoming_check
    if base_url is None:
        from aleph.web import app
        base_url = await get_base_url(app['config'])

    async with aiohttp.ClientSession(read_timeout=0) as session:
        async with session.get('%s/api/v0/pubsub/sub' % base_url,
                               params={'arg': topic,
                                       'discover': 'true'}) as resp:
            rest_value = None
            while True:
                value, is_full = await resp.content.readchunk()
                if rest_value:
                    value = rest_value + value

                if is_full:
                    # yield value
                    rest_value = None
                    try:
                        mvalue = json.loads(value)
                        mvalue = await decode_msg(mvalue)
                        LOGGER.debug("New message received %r" % mvalue)

                        # we should check the sender here to avoid spam
                        # and such things...
                        message = await incoming_check(mvalue)
                        if message is not None:
                            yield message

                    except Exception:
                        LOGGER.exception("Error handling message")
                else:
                    rest_value = value


async def pub(topic, message, base_url=None):
    if base_url is None:
        from aleph.web import app
        base_url = await get_base_url(app['config'])

    async with aiohttp.ClientSession(read_timeout=0) as session:
        async with session.get('%s/api/v0/pubsub/pub' % base_url,
                               params=(('arg', topic),
                                       ('arg', message))) as resp:
            assert resp.status == 200


async def incoming_channel(config, topic):
    from aleph.chains.common import incoming
    loop = asyncio.get_event_loop()
    while True:
        try:
            i = 0
            #seen_ids = []
            tasks = []
            async for message in sub(topic,
                                     base_url=await get_base_url(config)):
                LOGGER.debug("New message %r" % message)
                i += 1
                tasks.append(
                    loop.create_task(incoming(message)))

                # await incoming(message, seen_ids=seen_ids)
                if (i > 1000):
                    # every 1000 message we check that all tasks finished
                    # and we reset the seen_ids list.
                    for task in tasks:
                        await task
                    seen_ids = []
                    tasks = []
                    i = 0

        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting.")