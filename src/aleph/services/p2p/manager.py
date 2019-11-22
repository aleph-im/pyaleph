from libp2p import new_node
from libp2p.crypto.rsa import RSAPrivateKey, KeyPair, create_new_key_pair
from Crypto.PublicKey.RSA import import_key
import asyncio
from libp2p.pubsub import floodsub, gossipsub
from libp2p.pubsub.pubsub import Pubsub
import multiaddr
from aleph.services.utils import get_IP

import logging
LOGGER = logging.getLogger('P2P.host')

FLOODSUB_PROTOCOL_ID = floodsub.PROTOCOL_ID
GOSSIPSUB_PROTOCOL_ID = gossipsub.PROTOCOL_ID

async def initialize_host(host='0.0.0.0', port=4025, key=None, listen=True, protocol_active=True):
    from .peers import publish_host, monitor_hosts
    from .protocol import PROTOCOL_ID, AlephProtocol
    from .jobs import reconnect_p2p_job, tidy_http_peers_job
    if key is None:
        keypair = create_new_key_pair()
        if listen:
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
    protocol = None
    # gossip = gossipsub.GossipSub([GOSSIPSUB_PROTOCOL_ID], 10, 9, 11, 30)
    # psub = Pubsub(host, gossip, host.get_id())
    flood = floodsub.FloodSub([FLOODSUB_PROTOCOL_ID, GOSSIPSUB_PROTOCOL_ID])
    psub = Pubsub(host, flood, host.get_id())
    if protocol_active:
        protocol = AlephProtocol(host)
    asyncio.create_task(reconnect_p2p_job())
    asyncio.create_task(tidy_http_peers_job())
    if listen:
        from aleph.web import app
        
        await host.get_network().listen(multiaddr.Multiaddr(transport_opt))
        LOGGER.info("Listening on " + f'{transport_opt}/p2p/{host.get_id()}')
        ip = await get_IP()
        public_address = f'/ip4/{ip}/tcp/{port}/p2p/{host.get_id()}'
        http_port = app['config'].p2p.http_port.value
        public_http_address = f'http://{ip}:{http_port}'
        LOGGER.info("Probable public on " + public_address)
        # TODO: set correct interests and args here
        asyncio.create_task(publish_host(public_address, psub, peer_type="P2P"))
        asyncio.create_task(publish_host(public_http_address, psub, peer_type="HTTP"))
        asyncio.create_task(monitor_hosts(psub))
        # host.set_stream_handler(PROTOCOL_ID, stream_handler)
        
    return (host, psub, protocol)