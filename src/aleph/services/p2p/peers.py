from typing import Optional

from configmanager import Config
from multiaddr import Multiaddr
from p2pclient import Client as P2PClient
from p2pclient.libp2p_stubs.peer.peerinfo import info_from_p2p_addr

from .protocol import AlephProtocol


async def connect_peer(config: Config, p2p_client: P2PClient, streamer: Optional[AlephProtocol], peer: str) -> None:
    peer_info = info_from_p2p_addr(Multiaddr(peer))
    peer_id, _ = await p2p_client.identify()

    if str(peer_info.peer_id) == str(peer_id):
        return

    await p2p_client.connect(peer_info.peer_id, peer_info.addrs)

    if "streamer" in config.p2p.clients.value:
        if streamer is None:
            raise ValueError("Protocol streamer not initialized")
        if not await streamer.has_active_streams(peer_info.peer_id):
            await streamer.create_connections(peer_info.peer_id)
