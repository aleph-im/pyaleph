"""
Asyncio gRPC client for the aleph.im P2P service (release N+).

Replaces the RabbitMQ-based aleph-p2p-client. The method surface mirrors the
old client so call sites only adapt to the new pubsub message shape.
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


@dataclass
class NodeInfo:
    peer_id: str
    listen_multiaddrs: List[str]
    external_multiaddrs: List[str]


@dataclass
class PubsubMessage:
    topic: str
    sender: str
    data: bytes
    received_at_millis: int


@dataclass
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
        channel = grpc.aio.insecure_channel(f"{host}:{port}")
        stub = pb_grpc.AlephP2PStub(channel)
        try:
            node_info = await stub.Identify(pb.IdentifyRequest(), timeout=timeout)
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
        await self._stub.Publish(
            pb.PublishRequest(topic=topic, payload=data, echo=echo)
        )

    async def subscribe(self, topic: str) -> None:
        # Subscription state lives in the service; opening the Subscribe
        # stream (receive_messages) is what subscribes. Kept for interface
        # compatibility with the old client.
        return None

    async def receive_messages(self, topic: str) -> AsyncIterator[PubsubMessage]:
        stream = self._stub.Subscribe(pb.SubscribeRequest(topic=topic))
        async for envelope in stream:
            yield PubsubMessage(
                topic=envelope.topic,
                sender=envelope.source_peer_id,
                data=envelope.payload,
                received_at_millis=envelope.received_at_millis,
            )

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
