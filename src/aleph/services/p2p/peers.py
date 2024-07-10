from aleph_p2p_client import AlephP2PServiceClient
from multiaddr import Multiaddr

from aleph.toolkit.libp2p_stubs.peer.peerinfo import info_from_p2p_addr


async def connect_peer(p2p_client: AlephP2PServiceClient, peer_maddr: str) -> None:
    """
    Connects to the specified peer.

    :param p2p_client: P2P daemon client.
    :param peer_maddr: Fully qualified multi-address of the peer to connect to:
                       /ip4/<ip-address>/tcp/<port>/p2p/<peer-id>
    """
    peer_info = info_from_p2p_addr(Multiaddr(peer_maddr))
    peer_id = (await p2p_client.identify()).peer_id

    # Discard attempts to connect to self.
    if str(peer_info.peer_id) == str(peer_id):
        return

    for multiaddr in peer_info.addrs:
        await p2p_client.dial(peer_id=str(peer_info.peer_id), multiaddr=str(multiaddr))
