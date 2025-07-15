from pathlib import Path
from typing import Optional, Union

import aiofiles
import aiofiles.ospath

from .engine import StorageEngine


class FileSystemStorageEngine(StorageEngine):
    def __init__(self, folder: Union[Path, str]):
        self.folder = folder if isinstance(folder, Path) else Path(folder)

        if self.folder.exists() and not self.folder.is_dir():
            raise ValueError(f"'{self.folder}' exists and is not a directory.")

        self.folder.mkdir(parents=True, exist_ok=True)

    async def read(self, filename: str) -> Optional[bytes]:
        file_path = self.folder / filename

        if not await aiofiles.ospath.isfile(file_path):
            return None
        async with aiofiles.open(file_path, "rb") as f:
            return await f.read()

    async def write(self, filename: str, content: bytes):
        file_path = self.folder / filename
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(content)

    async def delete(self, filename: str):
        file_path = self.folder / filename
        async_unlink = aiofiles.ospath.wrap(
            Path.unlink
        )  # We manually wrap unlink (not handled by aiofiles)

        await async_unlink(file_path, missing_ok=True)

    async def exists(self, filename: str) -> bool:
        file_path = self.folder / filename
        return await aiofiles.ospath.exists(
            file_path
        )  # This func warp .exist func into async
