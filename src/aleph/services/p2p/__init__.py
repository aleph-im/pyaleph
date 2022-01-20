from typing import Coroutine, List

from multiaddr import Multiaddr
from p2pclient import Client as P2PClient

from . import singleton
from .manager import initialize_host
from .protocol import incoming_channel
from .pubsub import pub
import socket


def init_p2p_client(config) -> P2PClient:
    host = config.p2p.host.value
    host_ip_addr = socket.gethostbyname(host)

    control_port = config.p2p.control_port.value
    listen_port = config.p2p.listen_port.value
    control_maddr = Multiaddr(f"/ip4/{host_ip_addr}/tcp/{control_port}")
    listen_maddr = Multiaddr(f"/ip4/0.0.0.0/tcp/{listen_port}")
    return P2PClient(control_maddr=control_maddr, listen_maddr=listen_maddr)


async def init_p2p(config, listen: bool = True, port_id: int = 0) -> List[Coroutine]:
    singleton.client = init_p2p_client(config)
    port = config.p2p.port.value + port_id
    singleton.streamer, tasks = await initialize_host(
        p2p_client=singleton.client,
        host=config.p2p.host.value,
        port=port,
        listen=listen,
    )

    return tasks
