"""
"""

from gridfs.errors import NoFile


async def get_value(key: str):
    from aleph.model import fs
    if fs is None:
        raise ValueError("MongoDB fs not initialized")

    try:
        gridout = await fs.open_download_stream_by_name(key)
        return await gridout.read()
    except NoFile:
        return None


async def set_value(key: str, value: bytes):
    from aleph.model import fs
    if fs is None:
        raise ValueError("MongoDB fs not initialized")

    file_id = await fs.upload_from_stream(key, value)
    return file_id


async def delete_value(key: str):
    from aleph.model import fs
    if fs is None:
        raise ValueError("MongoDB fs not initialized")

    async for gridfs_file in fs.find({"filename": key}):
        await fs.delete(gridfs_file._id)
