from typing import Optional

from gridfs import NoFile
from motor.motor_asyncio import AsyncIOMotorGridFSBucket

from .engine import StorageEngine


class GridFsStorageEngine(StorageEngine):
    def __init__(self, gridfs_client: AsyncIOMotorGridFSBucket):
        self.gridfs_client = gridfs_client

    async def read(self, filename: str) -> Optional[bytes]:
        try:
            gridout = await self.gridfs_client.open_download_stream_by_name(filename)
            return await gridout.read()
        except NoFile:
            return None

    async def write(self, filename: str, content: bytes):
        _file_id = await self.gridfs_client.upload_from_stream(filename, content)

    async def delete(self, filename: str):
        async for gridfs_file in self.gridfs_client.find({"filename": filename}):
            await self.gridfs_client.delete(gridfs_file._id)
