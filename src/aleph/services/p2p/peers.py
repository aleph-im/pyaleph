from multiaddr import Multiaddr

from aleph.services.p2p.client import P2PGrpcClient
from aleph.toolkit.libp2p_stubs.peer.peerinfo import info_from_p2p_addr


async def connect_peer(p2p_client: P2PGrpcClient, peer_maddr: str) -> None:
    """
    Connects to the specified peer.

    :param p2p_client: P2P service client.
    :param peer_maddr: Fully qualified multi-address of the peer to connect to:
                       /ip4/<ip-address>/tcp/<port>/p2p/<peer-id>
    """
    peer_info = info_from_p2p_addr(Multiaddr(peer_maddr))

    # Discard attempts to connect to self.
    if str(peer_info.peer_id) == p2p_client.peer_id:
        return

    for multiaddr in peer_info.addrs:
        await p2p_client.dial(peer_id=str(peer_info.peer_id), multiaddr=str(multiaddr))
