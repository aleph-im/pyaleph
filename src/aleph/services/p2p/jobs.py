import asyncio
import logging

from aleph.model.p2p import get_peers
from .http import api_get_request
from .peers import connect_peer

LOGGER = logging.getLogger('P2P.jobs')


async def reconnect_p2p_job(config=None):
    from aleph.web import app
    if config is None:
        config = app['config']
    await asyncio.sleep(2)
    while True:
        try:
            peers = set(config.p2p.peers.value + [a async for a in get_peers(peer_type='P2P')])
            for peer in peers:
                try:
                    await connect_peer(config, peer)
                except:
                    LOGGER.debug("Can't reconnect to %s" % peer)
                
        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.p2p.reconnect_delay.value)
        
async def check_peer(peers, peer_uri, timeout=1):
    try:
        version_info = await api_get_request(peer_uri, "version", timeout=timeout)
        if version_info is not None:
            peers.append(peer_uri)
    except Exception:
        LOGGER.exception("Can't contact peer %r" % peer_uri)
    
        
async def tidy_http_peers_job(config=None):
    from aleph.web import app
    from aleph.services.p2p import singleton
    from aleph.services.utils import get_IP
    
    my_ip = await get_IP()
    if config is None:
        config = app['config']
    await asyncio.sleep(2)
    while True:
        try:
            peers = list()
            jobs = list()
            async for peer in get_peers(peer_type='HTTP'):
                if my_ip in peer:
                    continue
                
                jobs.append(check_peer(peers, peer))
            await asyncio.gather(*jobs)
            singleton.api_servers = peers
                
        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.p2p.reconnect_delay.value)