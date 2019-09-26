import base64
import base58
from . import singleton
import logging
LOGGER = logging.getLogger('P2P.pubsub')

async def decode_msg(msg):
    return {
        'from': base58.b58encode(msg.from_id),
        'data': msg.data,
        'seqno': base58.b58encode(msg.seqno),
        'topicIDs': msg.topicIDs
    }
    

async def sub(topic):
    from aleph.network import incoming_check
    sub = await singleton.pubsub.subscribe(topic)
    while True:
        mvalue = await sub.get()
        mvalue = await decode_msg(mvalue)
        LOGGER.debug("New message received %r" % mvalue)
        
        yield mvalue


async def pub(topic, message):
    await singleton.pubsub.publish(topic, message.encode('utf-8'))
            