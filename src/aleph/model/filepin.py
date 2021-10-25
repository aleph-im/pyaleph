from typing import Dict

from pymongo import IndexModel, HASHED

from aleph.model.base import BaseClass


class PermanentPin(BaseClass):
    """Hold information about pinned files."""

    COLLECTION = "filepins"

    INDEXES = [
        IndexModel([("multihash", HASHED)])
    ]


    @classmethod
    async def register(cls, multihash: str, reason: Dict):
        assert reason, "A permanent pin requires a reason"
        await cls.collection.update_many(
            filter={"multihash": multihash},
            update={"$push": {"reason": reason}},
            upsert=True,
        )
