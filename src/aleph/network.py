import aiohttp
import base64
import base58
import json
import asyncio
from aleph.storage import get_base_url
from aleph.chains.register import VERIFIER_REGISTER
import logging
LOGGER = logging.getLogger("NETWORK")

INCOMING_MESSAGE_AUTHORIZED_FIELDS = [
    'item_hash',
    'chain',
    'channel',
    'sender',
    'type',
    'time',
    'signature'
]


async def decode_msg(msg):
    return {
        'from': base58.b58encode(
            base64.b64decode(msg['from'])),
        'data': base64.b64decode(msg['data']),
        'seqno': base58.b58encode(base64.b64decode(msg['seqno'])),
        'topicIDs': msg['topicIDs']
    }


async def incoming_check(ipfs_pubsub_message):
    """ Verifies an incoming message is sane, protecting from spam in the
    meantime.

    TODO: actually implement this, no check done here yet. IMPORTANT.
    """

    try:
        message = json.loads(ipfs_pubsub_message.get('data', ''))
        LOGGER.debug("New message! %r" % message)
        message = await check_message(message, from_network=True)
        return message
    except json.JSONDecodeError:
        LOGGER.exception('Received non-json message %r'
                         % ipfs_pubsub_message.get('data', ''))


async def sub(topic, base_url=None):
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
                        LOGGER.debug("New message received", mvalue)

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
    while True:
        try:
            async for message in sub(topic,
                                     base_url=await get_base_url(config)):
                await incoming(message)

        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting.")


async def check_message(message, from_chain=False, from_network=False,
                        trusted=False):
    """ This function should check the incoming message and verify any
    extraneous or dangerous information for the rest of the process.
    Example of dangerous data: fake confirmations, fake tx_hash, bad times...

    TODO: Implement it fully! Dangerous!
    """
    if trusted:
        # only in the case of a message programmatically built here
        # from legacy native chain signing for example (signing offloaded)
        return message
    else:
        message = {k: v for k, v in message.items()
                   if k in INCOMING_MESSAGE_AUTHORIZED_FIELDS}
        chain = message.get('chain', None)
        signer = VERIFIER_REGISTER.get(chain, None)
        if signer is None:
            LOGGER.warn('Unknown chain for validation %r' % chain)
            return None

        if await signer(message):
            return message


def setup_listeners(config):
    # for now (1st milestone), we only listen on a single global topic...
    loop = asyncio.get_event_loop()
    loop.create_task(incoming_channel(config, config.aleph.queue_topic.value))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sub('blah', base_url='http://localhost:5001'))
    loop.close()
