import aiohttp
import base64
import base58
import json
import asyncio
from aleph.storage import get_base_url
import logging
LOGGER = logging.getLogger("NETWORK")


async def decode_msg(msg):
    return {
        'from': base58.b58encode(
            base64.b64decode(msg['from'])),
        'data': base64.b64decode(msg['data']),
        'seqno': base58.b58encode(base64.b64decode(msg['seqno'])),
        'topicIDs': msg['topicIDs']
    }


async def sub(base_url, topic):
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
                        LOGGER.debug("New message received", mvalue)
                        yield mvalue
                    except Exception as exc:
                        LOGGER.exception("Can't decode message JSON")
                else:
                    rest_value = value


async def incoming_channel(config, topic):
    while True:
        try:
            async for message in sub(await get_base_url(config), topic):
                print(message)
        except Exception as exc:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting.")


def setup_listeners(config):
    # for now (1st milestone), we only listen on a single global topic...
    loop = asyncio.get_event_loop()
    loop.create_task(incoming_channel(config, config.aleph.queue_topic.value))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sub('http://localhost:5001', 'blah'))
    loop.close()
