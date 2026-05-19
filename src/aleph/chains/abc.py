import abc
import datetime as dt
from typing import Optional, Protocol

from aleph_message.models import Chain, MessageType
from configmanager import Config


class SignableMessage(Protocol):
    """
    Structural type for messages that carry the fields needed for signature
    verification. Both `BasePendingMessage` (Pydantic) and `PendingMessageDb`
    (SQLAlchemy) satisfy this Protocol without sharing a common base class.
    """

    chain: Chain
    sender: str
    type: MessageType
    item_hash: str
    signature: Optional[str]
    time: dt.datetime


class Verifier(abc.ABC):
    @abc.abstractmethod
    async def verify_signature(self, message: SignableMessage) -> bool: ...


class ChainReader(abc.ABC):
    @abc.abstractmethod
    async def fetcher(self, config: Config): ...


class ChainWriter(ChainReader):
    @abc.abstractmethod
    async def packer(self, config: Config): ...
