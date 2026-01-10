import abc
from typing import AsyncIterable, Optional


class StorageEngine(abc.ABC):
    @abc.abstractmethod
    async def read(self, filename: str) -> Optional[bytes]: ...

    @abc.abstractmethod
    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]: ...

    @abc.abstractmethod
    async def write(self, filename: str, content: bytes): ...

    @abc.abstractmethod
    async def delete(self, filename: str): ...

    @abc.abstractmethod
    async def exists(self, filename: str) -> bool: ...
