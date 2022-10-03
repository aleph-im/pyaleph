from pathlib import Path
from typing import Optional, Union

from .engine import StorageEngine


class FileSystemStorageEngine(StorageEngine):
    def __init__(self, folder: Union[Path, str]):

        self.folder = folder if isinstance(folder, Path) else Path(folder)

        if not self.folder.is_dir():
            raise ValueError(
                f"'{self.folder}' exists and is not a directory."
            )

    async def read(self, filename: str) -> Optional[bytes]:
        file_path = self.folder / filename

        if not file_path.is_file():
            return None

        return file_path.read_bytes()

    async def write(self, filename: str, content: bytes):
        file_path = self.folder / filename
        file_path.write_bytes(content)

    async def delete(self, filename: str):
        file_path = self.folder / filename
        file_path.unlink(missing_ok=True)
