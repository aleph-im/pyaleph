import asyncio
import json

import multiaddr
from libp2p.peer.peerinfo import info_from_p2p_addr

from aleph.services.p2p.pubsub import decode_msg
from . import singleton


async def connect_peer(config, peer):
    info = info_from_p2p_addr(multiaddr.Multiaddr(peer))
    if str(info.peer_id) == str(singleton.host.get_id()):
        # LOGGER.debug("Can't connect to myself.")
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