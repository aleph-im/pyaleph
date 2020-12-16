import asyncio
import json
import logging

import multiaddr
from libp2p.peer.peerinfo import info_from_p2p_addr

from aleph.services.p2p.pubsub import decode_msg
from . import singleton

LOGGER = logging.getLogger('P2P.peers')

ALIVE_TOPIC = 'ALIVE'

async def publish_host(address, psub, topic=ALIVE_TOPIC, interests=None, delay=120, peer_type="P2P"):
    """ Publish our multiaddress regularly, saying we are alive.
    """
    await asyncio.sleep(2)
    from aleph import __version__
    msg = {
        'address': address,
        'interests': interests,
        'peer_type': peer_type,
        'version': __version__
    }
    msg = json.dumps(msg).encode('utf-8')
    while True:
        try:
            LOGGER.debug("Publishing alive message on p2p pubsub")
            await psub.publish(topic, msg)
        except Exception:
            LOGGER.exception("Can't publish alive message")
        await asyncio.sleep(delay)


async def monitor_hosts(psub):
    from aleph.model.p2p import add_peer
    alive_sub = await psub.subscribe(ALIVE_TOPIC)
    while True:
        try:
            message = await alive_sub.get()
            mvalue = await decode_msg(message)

            LOGGER.info("New alive message received %r" % mvalue)

            # TODO: Anti-spam: one emitter should not send us to many peers

            content = json.loads(mvalue['data'])
            peer_type = content.get('peer_type', 'P2P')
            if not isinstance(content['address'], str):
                raise ValueError('Bad address')
            if not isinstance(content['peer_type'], str):
                raise ValueError('Bad peer type')

            # TODO: handle interests and save it (channels)

            if peer_type not in ['P2P', 'HTTP']:
                raise ValueError('Unsupported peer type %r' % peer_type)

            emitter = '/p2p/' + mvalue['from'].decode('ascii')

            await add_peer(address=content['address'], peer_type=peer_type, emitter=emitter)
        except Exception:
            LOGGER.exception("Exception in pubsub peers monitoring")

async def connect_peer(config, peer):
    info = info_from_p2p_addr(multiaddr.Multiaddr(peer))
    LOGGER.debug(f"Attempting p2p connection to {info.peer_id} {info.addrs}")

    if str(info.peer_id) == str(singleton.host.get_id()):
        LOGGER.debug(f"Skipping connection to myself")
        return
    
    if 'streamer' in config.p2p.clients.value:
        if not await singleton.streamer.has_active_streams(info.peer_id):
            # network = singleton.host.get_network()
            # if info.peer_id in network.connections:
            #     await network.close_peer(info.peer_id)
            #     del network[info.peer_id]
                
            await singleton.host.connect(info)
            await singleton.streamer.create_connections(info.peer_id)
    else:
        await singleton.host.connect(info)
            

async def get_peers():
    my_id = singleton.host.get_id()
    peers = [peer for peer
             in singleton.host.get_peerstore().peer_ids()
             if str(peer) != str(my_id)]
    return peers