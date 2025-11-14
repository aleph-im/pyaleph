from pathlib import Path
from typing import Optional, Union

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

    async def write(self, filename: str, content: bytes):
        file_path = self.folder / filename
        temp_path = self.folder / f"{filename}.tmp"

        try:
            # Write to temporary file first
            temp_path.write_bytes(content)

            # Atomic rename - this operation is atomic on POSIX systems
            # If crash happens before this, temp file exists but target doesn't
            temp_path.replace(file_path)

        except Exception:
            # Clean up temp file if write failed
            temp_path.unlink(missing_ok=True)
            raise

    async def delete(self, filename: str):
        file_path = self.folder / filename
        file_path.unlink(missing_ok=True)

    async def exists(self, filename: str) -> bool:
        file_path = self.folder / filename
        return file_path.exists()
