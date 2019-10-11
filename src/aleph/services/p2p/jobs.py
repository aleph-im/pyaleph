import asyncio
import logging
from aleph.model.p2p import get_peers
from .peers import connect_peer
from .http import api_get_request

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
