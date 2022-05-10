import datetime as dt
from dataclasses import asdict, dataclass
from typing import AsyncIterator, Dict, Optional

from bson.objectid import ObjectId
from pymongo import ASCENDING, IndexModel

from aleph.model.base import BaseClass


@dataclass
class ScheduledDeletionInfo:
    filename: str
    delete_by: dt.datetime
    object_id: Optional[ObjectId] = None

    @classmethod
    def from_db(cls, db_value: Dict) -> "ScheduledDeletionInfo":
        return cls(
            filename=db_value["filename"],
            delete_by=db_value["delete_by"],
            object_id=db_value["_id"],
        )

    def to_dict(self):
        return {"filename": self.filename, "delete_by": self.delete_by}


class ScheduledDeletion(BaseClass):
    COLLECTION = "scheduled_deletions"

    IndexModel([("delete_by", ASCENDING)])
    IndexModel([("filename", ASCENDING)])

    @classmethod
    async def insert(cls, scheduled_deletion: ScheduledDeletionInfo):
        await cls.collection.insert_one(scheduled_deletion.to_dict())

    @classmethod
    async def files_to_delete(
        cls, delete_by: dt.datetime
    ) -> AsyncIterator[ScheduledDeletionInfo]:

        query = cls.collection.find(filter={"delete_by": {"$lte": delete_by}}).sort(
            [("delete_by", 1)]
        )

        async for result in query:
            yield ScheduledDeletionInfo.from_db(result)
