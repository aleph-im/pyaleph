"""
Asyncio gRPC client for the aleph.im P2P service (release N+).

Replaces the RabbitMQ-based aleph-p2p-client. The method surface mirrors the
old client so call sites only adapt to the new pubsub message shape.

All public methods raise ``grpc.aio.AioRpcError`` on transport or service
failure unless the error is mapped to a domain exception (``dial`` maps
FAILED_PRECONDITION -> DialWrongPeerException, everything else ->
DialFailedException).
"""

import logging
from dataclasses import dataclass
from typing import AsyncIterator, List, Sequence, Tuple

import grpc
import grpc.aio

from .grpc_generated import aleph_p2p_pb2 as pb
from .grpc_generated import aleph_p2p_pb2_grpc as pb_grpc

LOGGER = logging.getLogger(__name__)


class P2PServiceException(Exception):
    pass


class DialFailedException(P2PServiceException):
    pass


class DialWrongPeerException(P2PServiceException):
    pass


@dataclass(frozen=True)
class NodeInfo:
    peer_id: str
    listen_multiaddrs: List[str]
    external_multiaddrs: List[str]


@dataclass(frozen=True)
class PubsubMessage:
    topic: str
    sender: str
    data: bytes
    received_at_millis: int


@dataclass(frozen=True)
class PeerInfo:
    peer_id: str
    multiaddrs: List[str]
    preferred: bool
    score: float


class P2PGrpcClient:
    def __init__(self, channel: grpc.aio.Channel, peer_id: str):
        self._channel = channel
        self._stub = pb_grpc.AlephP2PStub(channel)
        self.peer_id = peer_id

    @classmethod
    async def connect(
        cls, host: str, port: int, timeout: float = 30.0
    ) -> "P2PGrpcClient":
        """Create a client by connecting to the P2P service at host:port.

        Performs an initial Identify RPC to verify the service is reachable and
        to capture the local peer ID. ``wait_for_ready=True`` is passed so that
        container start-ordering races are absorbed within the timeout window;
        UNAVAILABLE is only surfaced after the deadline is reached.
        """
        channel = grpc.aio.insecure_channel(
            f"{host}:{port}",
            options=[
                ("grpc.keepalive_time_ms", 30_000),
                ("grpc.keepalive_timeout_ms", 10_000),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        )
        stub = pb_grpc.AlephP2PStub(channel)
        try:
            node_info = await stub.Identify(
                pb.IdentifyRequest(), timeout=timeout, wait_for_ready=True
            )
        except BaseException:
            await channel.close()
            raise
        LOGGER.info("Connected to P2P service, peer ID: %s", node_info.peer_id)
        return cls(channel=channel, peer_id=node_info.peer_id)

    async def identify(self) -> NodeInfo:
        node_info = await self._stub.Identify(pb.IdentifyRequest())
        return NodeInfo(
            peer_id=node_info.peer_id,
            listen_multiaddrs=list(node_info.listen_multiaddrs),
            external_multiaddrs=list(node_info.external_multiaddrs),
        )

    async def dial(self, peer_id: str, multiaddr: str) -> None:
        try:
            await self._stub.Dial(pb.DialRequest(peer_id=peer_id, multiaddr=multiaddr))
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                raise DialWrongPeerException(e.details()) from e
            raise DialFailedException(e.details()) from e

    async def publish(self, data: bytes, topic: str, echo: bool = False) -> None:
        """Publish ``data`` on ``topic``.

        ``echo`` (default False): when True the service redelivers this message
        on local Subscribe streams for the same topic, mirroring the old
        client's ``loopback`` parameter.

        Raises ``grpc.aio.AioRpcError`` on transport or service failure.
        """
        await self._stub.Publish(
            pb.PublishRequest(topic=topic, payload=data, echo=echo)
        )

    async def receive_messages(self, topic: str) -> AsyncIterator[PubsubMessage]:
        stream = self._stub.Subscribe(pb.SubscribeRequest(topic=topic))
        try:
            async for envelope in stream:
                yield PubsubMessage(
                    topic=envelope.topic,
                    sender=envelope.source_peer_id,
                    data=envelope.payload,
                    received_at_millis=envelope.received_at_millis,
                )
        finally:
            stream.cancel()

    async def set_preferred_peers(
        self, peers: Sequence[Tuple[str, Sequence[str]]]
    ) -> Tuple[int, int]:
        request = pb.SetPreferredPeersRequest(
            peers=[
                pb.PreferredPeer(peer_id=peer_id, multiaddrs=list(multiaddrs))
                for peer_id, multiaddrs in peers
            ]
        )
        result = await self._stub.SetPreferredPeers(request)
        return result.accepted, result.truncated

    async def get_peers(self) -> List[PeerInfo]:
        result = await self._stub.GetPeers(pb.GetPeersRequest())
        return [
            PeerInfo(
                peer_id=peer.peer_id,
                multiaddrs=list(peer.multiaddrs),
                preferred=peer.preferred,
                score=peer.score,
            )
            for peer in result.peers
        ]

    async def close(self) -> None:
        await self._channel.close()
