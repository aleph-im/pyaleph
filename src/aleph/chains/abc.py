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

    Fields are read-only properties so subclasses can narrow them
    (e.g. `PendingPostMessage.type: Literal[MessageType.post]`).
    """

    @property
    def chain(self) -> Chain: ...
    @property
    def sender(self) -> str: ...
    @property
    def type(self) -> MessageType: ...
    @property
    def item_hash(self) -> str: ...
    @property
    def signature(self) -> Optional[str]: ...
    @property
    def time(self) -> dt.datetime: ...


class Verifier(abc.ABC):
    @abc.abstractmethod
    async def verify_signature(self, message: SignableMessage) -> bool: ...


class ChainReader(abc.ABC):
    @abc.abstractmethod
    async def fetcher(self, config: Config) -> None: ...


class ChainWriter(ChainReader):
    @abc.abstractmethod
    async def packer(self, config: Config) -> None: ...
