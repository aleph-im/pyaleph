import asyncio
import os
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
        temp_path = self.folder / f"{filename}.tmp"

        await asyncio.to_thread(self._write_durably, temp_path, file_path, content)

    @staticmethod
    def _write_durably(temp_path: Path, file_path: Path, content: bytes) -> None:
        """Atomically and durably write ``content`` to ``file_path``.

        Steps:
          1. Write bytes to ``temp_path`` (same directory as ``file_path``).
          2. fsync the file descriptor so data and file metadata hit the disk.
          3. Atomically rename via ``os.replace`` (POSIX-atomic on same FS).
          4. Best-effort fsync of the parent directory so the rename is durable
             across kernel crashes (POSIX-only; silently skipped on Windows).

        On any exception, the temp file is removed (best-effort) and the
        exception is re-raised. The target file is never touched until the
        rename succeeds, so crashes leave either the old content or none.
        """
        fd = os.open(
            str(temp_path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o644,
        )
        try:
            try:
                view = memoryview(content)
                written = 0
                while written < len(view):
                    written += os.write(fd, view[written:])
                os.fsync(fd)
            finally:
                os.close(fd)

            os.replace(str(temp_path), str(file_path))

            # Best-effort directory fsync — makes the rename durable.
            # os.O_DIRECTORY is POSIX-only (AttributeError on Windows);
            # some filesystems/VMs also raise OSError — both are silently skipped.
            try:
                dir_fd = os.open(str(file_path.parent), os.O_DIRECTORY)
            except (AttributeError, OSError):
                return
            try:
                os.fsync(dir_fd)
            except OSError:
                pass
            finally:
                os.close(dir_fd)

        except Exception:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    async def delete(self, filename: str):
        file_path = self.folder / filename
        file_path.unlink(missing_ok=True)

    async def exists(self, filename: str) -> bool:
        file_path = self.folder / filename
        return file_path.exists()
