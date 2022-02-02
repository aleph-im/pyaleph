from configmanager import Config
from multiaddr import Multiaddr
from p2pclient.libp2p_stubs.peer.peerinfo import info_from_p2p_addr

from .singleton import get_p2p_client, streamer


async def connect_peer(config: Config, peer: str) -> None:
    p2p_client = get_p2p_client()
    peer_info = info_from_p2p_addr(Multiaddr(peer))
    peer_id, _ = await p2p_client.identify()

    if str(peer_info.peer_id) == str(peer_id):
        # LOGGER.debug("Can't connect to myself.")
        return

    await p2p_client.connect(peer_info.peer_id, peer_info.addrs)

    if "streamer" in config.p2p.clients.value:
        if streamer is None:
            raise ValueError("Protocol streamer not initialized")
        if not await streamer.has_active_streams(peer_info.peer_id):
            await streamer.create_connections(peer_info.peer_id)
