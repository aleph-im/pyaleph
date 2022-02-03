from typing import Optional

from multiaddr import Multiaddr
from p2pclient import Client as P2PClient
from p2pclient.libp2p_stubs.peer.peerinfo import info_from_p2p_addr

from .protocol import AlephProtocol


async def connect_peer(p2p_client: P2PClient, streamer: Optional[AlephProtocol], peer_maddr: str) -> None:
    """
    Connects to the specified peer.

    :param p2p_client: P2P daemon client.
    :param streamer: Protocol streamer, if configured.
    :param peer_maddr: Fully qualified multi-address of the peer to connect to:
                       /ip4/<ip-address>/tcp/<port>/p2p/<peer-id>
    """
    peer_info = info_from_p2p_addr(Multiaddr(peer_maddr))
    peer_id, _ = await p2p_client.identify()

    if str(peer_info.peer_id) == str(peer_id):
        # LOGGER.debug("Can't connect to myself.")
        return

    await p2p_client.connect(peer_info.peer_id, peer_info.addrs)

    if streamer is not None:
        await streamer.add_peer(peer_info.peer_id)
