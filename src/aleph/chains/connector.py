import abc

from configmanager import Config

from aleph.schemas.pending_messages import BasePendingMessage
from aleph.types.chain_sync import ChainEventType


class ChainConnector:
    ...


class Verifier(abc.ABC, ChainConnector):
    @abc.abstractmethod
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        ...


class ChainReader(abc.ABC, ChainConnector):
    @abc.abstractmethod
    async def fetcher(self, config: Config):
        ...


class ChainWriter(ChainReader):
    @abc.abstractmethod
    async def packer(self, config: Config):
        ...
