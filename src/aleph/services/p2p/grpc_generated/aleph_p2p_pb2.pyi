from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IdentifyRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IdentifyResponse(_message.Message):
    __slots__ = ("peer_id", "listen_multiaddrs", "external_multiaddrs")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    LISTEN_MULTIADDRS_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_MULTIADDRS_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    listen_multiaddrs: _containers.RepeatedScalarFieldContainer[str]
    external_multiaddrs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, peer_id: _Optional[str] = ..., listen_multiaddrs: _Optional[_Iterable[str]] = ..., external_multiaddrs: _Optional[_Iterable[str]] = ...) -> None: ...

class DialRequest(_message.Message):
    __slots__ = ("peer_id", "multiaddr")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    MULTIADDR_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    multiaddr: str
    def __init__(self, peer_id: _Optional[str] = ..., multiaddr: _Optional[str] = ...) -> None: ...

class DialResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PublishRequest(_message.Message):
    __slots__ = ("topic", "payload", "echo")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    ECHO_FIELD_NUMBER: _ClassVar[int]
    topic: str
    payload: bytes
    echo: bool
    def __init__(self, topic: _Optional[str] = ..., payload: _Optional[bytes] = ..., echo: _Optional[bool] = ...) -> None: ...

class PublishResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SubscribeRequest(_message.Message):
    __slots__ = ("topic",)
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    topic: str
    def __init__(self, topic: _Optional[str] = ...) -> None: ...

class PubsubEnvelope(_message.Message):
    __slots__ = ("topic", "source_peer_id", "payload", "received_at_millis")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PEER_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    RECEIVED_AT_MILLIS_FIELD_NUMBER: _ClassVar[int]
    topic: str
    source_peer_id: str
    payload: bytes
    received_at_millis: int
    def __init__(self, topic: _Optional[str] = ..., source_peer_id: _Optional[str] = ..., payload: _Optional[bytes] = ..., received_at_millis: _Optional[int] = ...) -> None: ...

class PreferredPeer(_message.Message):
    __slots__ = ("peer_id", "multiaddrs")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    MULTIADDRS_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    multiaddrs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, peer_id: _Optional[str] = ..., multiaddrs: _Optional[_Iterable[str]] = ...) -> None: ...

class SetPreferredPeersRequest(_message.Message):
    __slots__ = ("peers",)
    PEERS_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[PreferredPeer]
    def __init__(self, peers: _Optional[_Iterable[_Union[PreferredPeer, _Mapping]]] = ...) -> None: ...

class SetPreferredPeersResponse(_message.Message):
    __slots__ = ("accepted", "truncated")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    accepted: int
    truncated: int
    def __init__(self, accepted: _Optional[int] = ..., truncated: _Optional[int] = ...) -> None: ...

class GetPeersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetPeersResponse(_message.Message):
    __slots__ = ("peers",)
    PEERS_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[PeerInfo]
    def __init__(self, peers: _Optional[_Iterable[_Union[PeerInfo, _Mapping]]] = ...) -> None: ...

class PeerInfo(_message.Message):
    __slots__ = ("peer_id", "multiaddrs", "preferred", "score")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    MULTIADDRS_FIELD_NUMBER: _ClassVar[int]
    PREFERRED_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    multiaddrs: _containers.RepeatedScalarFieldContainer[str]
    preferred: bool
    score: float
    def __init__(self, peer_id: _Optional[str] = ..., multiaddrs: _Optional[_Iterable[str]] = ..., preferred: _Optional[bool] = ..., score: _Optional[float] = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("item_hash", "preferred_peer_ids")
    ITEM_HASH_FIELD_NUMBER: _ClassVar[int]
    PREFERRED_PEER_IDS_FIELD_NUMBER: _ClassVar[int]
    item_hash: str
    preferred_peer_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, item_hash: _Optional[str] = ..., preferred_peer_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class FetchChunk(_message.Message):
    __slots__ = ("data", "total_size")
    DATA_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SIZE_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    total_size: int
    def __init__(self, data: _Optional[bytes] = ..., total_size: _Optional[int] = ...) -> None: ...
