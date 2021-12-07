"""
"""

# from aleph.model.base import BaseClass
# from pymongo import ASCENDING, IndexModel
from gridfs.errors import NoFile

# class Hash(BaseClass):
#     """Holds information about the chains state."""
#     COLLECTION = "hashes"

#     INDEXES = [IndexModel([("key", ASCENDING)], unique=True)]

#     @classmethod
#     async def get(cls, key):
#         obj = await cls.collection.find_one(
#             {'key': key})
#         if obj is None:
#             return None

#         return obj.get('value', None)

#     @classmethod
#     async def set(cls, key, value):
#         await cls.collection.update_one({'key': key},
#                                         {'$currentDate': {
#                                             'last_update': True
#                                          },
#                                          '$set': {"value": value}
#                                         },
#                                         upsert=True)


async def get_value(key: str):
    from aleph.model import fs

    try:
        gridout = await fs.open_download_stream_by_name(key)
        return await gridout.read()
    except NoFile:
        return None


async def set_value(key: str, value: bytes):
    from aleph.model import fs

    file_id = await fs.upload_from_stream(key, value)
    return file_id


async def delete_value(key: str):
    from aleph.model import fs

    async for gridfs_file in fs.find({"filename": key}):
        await fs.delete(gridfs_file._id)
