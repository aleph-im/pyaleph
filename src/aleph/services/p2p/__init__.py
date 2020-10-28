from .manager import initialize_host
from .protocol import incoming_channel
from .pubsub import pub, sub
from .peers import connect_peer
from . import singleton

async def init_p2p(config, listen=True, port_id=0, use_key=True):
    pkey = config.p2p.key.value
    port = config.p2p.port.value + port_id
    singleton.host, singleton.pubsub, singleton.streamer =\
         await initialize_host(key=pkey, host=config.p2p.host.value,
                               port=port, listen=listen,
                               protocol_active=('protocol' in config.p2p.clients.value))
    
async def get_host():
    return singleton.host

async def get_pubsub():
    return singleton.pubsub