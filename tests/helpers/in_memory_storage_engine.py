from typing import AsyncIterable, Dict, Optional

from aleph.services.storage.engine import StorageEngine


# TODO: remove duplication between this class and MockStorageEngine
class InMemoryStorageEngine(StorageEngine):
    """
    A storage engine that stores files in a dictionary.
    """

    def __init__(self, files: Dict[str, bytes]):
        self.files = files

    async def read(self, filename: str) -> Optional[bytes]:
        try:
            return self.files[filename]
        except KeyError:
            return None

    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]:
        content = await self.read(filename)
        if content is None:
            return None

        async def _read_iterator():
            for i in range(0, len(content), chunk_size):
                yield content[i : i + chunk_size]

        return _read_iterator()

    async def write(self, filename: str, content: bytes):
        self.files[filename] = content

    async def delete(self, filename: str):
        del self.files[filename]

    async def exists(self, filename: str) -> bool:
        return filename in self.files
