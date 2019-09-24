from libp2p import new_node
import multiaddr
from libp2p.peer.peerinfo import info_from_p2p_addr
from libp2p.pubsub import floodsub, gossipsub
from libp2p.pubsub.pubsub import Pubsub
from libp2p import generate_new_rsa_identity, initialize_default_swarm
import libp2p.security.secio.transport as secio
import asyncio

from Crypto.PublicKey.RSA import import_key
from libp2p.crypto.rsa import RSAPrivateKey, KeyPair, create_new_key_pair

import base64
import base58

import logging
LOGGER = logging.getLogger()

FLOODSUB_PROTOCOL_ID = floodsub.PROTOCOL_ID
GOSSIPSUB_PROTOCOL_ID = gossipsub.PROTOCOL_ID

host = None
pubsub = None

async def get_host(host='0.0.0.0', port=4025, key=None, listen=True):
    if key is None:
        keypair = create_new_key_pair()
        LOGGER.info("Generating new key, please save it to keep same host id.")
        LOGGER.info(keypair.private_key.impl.export_key().decode('utf-8'))
    else:
        priv = import_key(key)
        private_key = RSAPrivateKey(priv)
        public_key = private_key.get_public_key()
        keypair = KeyPair(private_key, public_key)
        
    transport_opt = f"/ip4/{host}/tcp/{port}"
    host = await new_node(transport_opt=[transport_opt],
                          key_pair=keypair)
    gossip = gossipsub.GossipSub([GOSSIPSUB_PROTOCOL_ID], 10, 9, 11, 30)
    psub = Pubsub(host, gossip, host.get_id())
    await host.get_network().listen(multiaddr.Multiaddr(transport_opt))
    LOGGER.info("Listening on " + f'{transport_opt}/p2p/{host.get_id()}')
    return (host, psub)

async def init_p2p(config, listen=True, port_id=0):
    global host, pubsub
    pkey = config.aleph.p2p.key.value
    port = config.aleph.p2p.port.value + port_id
    host, pubsub = await get_host(host=config.aleph.p2p.host.value,
                                  port=port, key=pkey, listen=listen)
    

async def decode_msg(msg):
    return {
        'from': base58.b58encode(msg.from_id),
        'data': msg.data,
        'seqno': base58.b58encode(msg.seqno),
        'topicIDs': msg.topicIDs
    }
    
async def sub(topic):
    from aleph.network import incoming_check
    sub = await pubsub.subscribe(topic)
    while True:
        mvalue = await sub.get()
        mvalue = await decode_msg(mvalue)
        LOGGER.debug("New message received %r" % mvalue)

        # we should check the sender here to avoid spam
        # and such things...
        message = await incoming_check(mvalue)
        if message is not None:
            yield message

async def pub(topic, message):
    await pubsub.publish(topic, message.encode('utf-8'))
            
async def incoming_channel(config, topic):
    from aleph.chains.common import incoming
    loop = asyncio.get_event_loop()
    while True:
        try:
            i = 0
            #seen_ids = []
            tasks = []
            async for message in sub(topic):
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
            LOGGER.exception("Exception in pubsub, reconnecting.")
            
async def connect_peer(peer):
    info = info_from_p2p_addr(peer)
    return await host.connect(info)