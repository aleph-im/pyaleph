import abc
from typing import Optional


class StorageEngine(abc.ABC):
    @abc.abstractmethod
    async def read(self, filename: str) -> Optional[bytes]: ...

    @abc.abstractmethod
    async def write(self, filename: str, content: bytes): ...

    @abc.abstractmethod
    async def delete(self, filename: str): ...

    @abc.abstractmethod
    async def exists(self, filename: str) -> bool: ...
