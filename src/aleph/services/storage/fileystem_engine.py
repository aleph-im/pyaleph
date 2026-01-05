from pathlib import Path
from typing import AsyncIterable, Optional, Union

import aiofiles

from .engine import StorageEngine


class FileSystemStorageEngine(StorageEngine):
    def __init__(self, folder: Union[Path, str]):
        self.folder = folder if isinstance(folder, Path) else Path(folder)

        if self.folder.exists() and not self.folder.is_dir():
            raise ValueError(f"'{self.folder}' exists and is not a directory.")

        self.folder.mkdir(parents=True, exist_ok=True)

    async def read(self, filename: str) -> Optional[bytes]:
        file_path = self.folder / filename

        if not file_path.is_file():
            return None

        return file_path.read_bytes()

    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]:
        file_path = self.folder / filename

        if not file_path.is_file():
            return None

        async def _read_iterator():
            async with aiofiles.open(file_path, mode="rb") as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        return _read_iterator()

    async def write(self, filename: str, content: bytes):
        file_path = self.folder / filename
        file_path.write_bytes(content)

    async def delete(self, filename: str):
        file_path = self.folder / filename
        file_path.unlink(missing_ok=True)

    async def exists(self, filename: str) -> bool:
        file_path = self.folder / filename
        return file_path.exists()
