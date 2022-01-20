import logging
from typing import Coroutine, List, Tuple, Any

import multiaddr
from Crypto.PublicKey.RSA import import_key
from libp2p import new_node, BasicHost
from libp2p.crypto.rsa import KeyPair, RSAPrivateKey
from libp2p.pubsub import floodsub, gossipsub
from libp2p.pubsub.pubsub import Pubsub

from aleph.services.utils import get_IP
from aleph.services.peers.monitor import monitor_hosts_ipfs, monitor_hosts_p2p
from aleph.services.peers.publish import publish_host
from aleph.services.ipfs.common import get_public_address

LOGGER = logging.getLogger("P2P.host")

FLOODSUB_PROTOCOL_ID = floodsub.PROTOCOL_ID
GOSSIPSUB_PROTOCOL_ID = gossipsub.PROTOCOL_ID


# Save published adress to present them in the web process later
public_adresses = []


async def initialize_host(
    key: str,
    host: str = "0.0.0.0",
    port: int = 4025,
    listen: bool = True,
    protocol_active: bool = True,
) -> Tuple[BasicHost, Pubsub, Any, List]:

    from .protocol import AlephProtocol
    from .jobs import reconnect_p2p_job, tidy_http_peers_job

    assert key, "Host cannot be initialized without a key"

    tasks: List[Coroutine]

    priv = import_key(key)
    private_key = RSAPrivateKey(priv)
    public_key = private_key.get_public_key()
    keypair = KeyPair(private_key, public_key)

    transport_opt = f"/ip4/{host}/tcp/{port}"
    host: BasicHost = await new_node(transport_opt=[transport_opt], key_pair=keypair)
    protocol = None
    # gossip = gossipsub.GossipSub([GOSSIPSUB_PROTOCOL_ID], 10, 9, 11, 30)
    # psub = Pubsub(host, gossip, host.get_id())
    flood = floodsub.FloodSub([FLOODSUB_PROTOCOL_ID, GOSSIPSUB_PROTOCOL_ID])
    psub = Pubsub(host, flood, host.get_id())
    if protocol_active:
        protocol = AlephProtocol(host)
    tasks = [
        reconnect_p2p_job(),
        tidy_http_peers_job(),
    ]
    if listen:
        from aleph.web import app

        await host.get_network().listen(multiaddr.Multiaddr(transport_opt))
        LOGGER.info("Listening on " + f"{transport_opt}/p2p/{host.get_id()}")
        ip = await get_IP()
        public_address = f"/ip4/{ip}/tcp/{port}/p2p/{host.get_id()}"
        http_port = app["config"].p2p.http_port.value
        public_adresses.append(public_address)

        public_http_address = f"http://{ip}:{http_port}"

        LOGGER.info("Probable public on " + public_address)
        # TODO: set correct interests and args here
        tasks += [
            publish_host(
                public_address,
                psub,
                peer_type="P2P",
                use_ipfs=app["config"].ipfs.enabled.value,
            ),
            publish_host(
                public_http_address,
                psub,
                peer_type="HTTP",
                use_ipfs=app["config"].ipfs.enabled.value,
            ),
            monitor_hosts_p2p(psub),
        ]

        if app["config"].ipfs.enabled.value:
            tasks.append(monitor_hosts_ipfs(app["config"]))
            try:
                public_ipfs_address = await get_public_address()
                tasks.append(
                    publish_host(
                        public_ipfs_address, psub, peer_type="IPFS", use_ipfs=True
                    )
                )
            except Exception:
                LOGGER.exception("Can't publish public IPFS address")

        # Enable message exchange using libp2p
        # host.set_stream_handler(PROTOCOL_ID, stream_handler)

    return (host, psub, protocol, tasks)
