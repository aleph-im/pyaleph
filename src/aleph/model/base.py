import logging
from datetime import datetime

import pymongo
from bson.objectid import ObjectId

LOGGER = logging.getLogger(__name__)


class Index(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def ensure(self, collection):
        LOGGER.debug("creating index %r/%r" % (self._args, self._kwargs))
        return collection.ensure_index(*self._args, **self._kwargs)


class classproperty(object):
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


def prepare_value(value):
    if isinstance(value, dict):
        value = prepare_dict(value)

    if isinstance(value, list):
        value = [prepare_value(v) for v in value]

    elif (
        isinstance(value, datetime)
        or isinstance(value, bytes)
        or isinstance(value, ObjectId)
    ):
        value = str(value)

    return value


def prepare_dict(dict_to_prepare):
    out = dict()
    for key, value in dict_to_prepare.items():
        out[key] = prepare_value(value)

    return out


class SerializerObject(object):
    def __init__(self, data):
        self.set_data(data)

    def _setattr(self, attr, value):
        object.__setattr__(self, attr, value)

    def set_data(self, data):
        self._setattr("_data", data)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        return self._data.set(key, value)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]

        if attr in self._data:
            return self._data.get(attr)

        raise AttributeError("%r object has no attribute %r" % (self.__class__, attr))

    def __setitem__(self, attribute, value):
        self._data[attribute] = value

    def __getitem__(self, attribute):
        return self._data[attribute]

    def __setattr__(self, attr, value):
        if attr in self.__dict__:
            self._setattr(attr, value)
        else:
            self.__setitem__(attr, value)

    def serialize(self):
        return prepare_dict(self._data)


class SubObject(SerializerObject):
    def __init__(self, parent, data=None):
        self.parent = parent
        self.set_data(data or dict())


class BaseClass(SerializerObject):
    @classproperty
    def _collection_name(self):
        if getattr(self, "COLLECTION", None) is None:
            raise ValueError(
                "Please specify collection name on classes"
                + " inheriting from BaseClass"
            )

        return self.COLLECTION

    def get_collection(self, db=None):
        if db is None:
            from aleph import model

            db = model.db
        return db[self._collection_name]

    @classproperty
    def collection(self):
        return self.get_collection(self)

    @classmethod
    async def find_one(cls, **kwargs):
        value = await cls.collection.find_one(kwargs)
        if value is not None:
            return cls(value)

    @classmethod
    def _prepare_find(
        cls,
        collection,
        *args,
        sort=None,
        sort_order=pymongo.ASCENDING,
        skip=None,
        limit=None,
        **kwargs
    ):
        values = collection.find(*args, **kwargs)
        if sort is not None:
            if not isinstance(sort, list):
                values = values.sort(sort, sort_order)
            else:
                values = values.sort(sort)

        if skip is not None:
            values = values.skip(skip)

        if limit is not None:
            values = values.limit(limit)

        return values

    @classmethod
    async def find(
        cls,
        *args,
        sort=None,
        sort_order=pymongo.ASCENDING,
        skip=None,
        limit=None,
        **kwargs
    ):
        cursor = cls._prepare_find(
            cls.collection,
            *args,
            sort=sort,
            sort_order=sort_order,
            skip=skip,
            limit=limit,
            **kwargs
        )

        async for value in cursor:
            yield cls(value)

    @classmethod
    async def count(cls, *args, **kwargs):
        return await cls.collection.count_documents(*args, **kwargs)

    async def save(self):
        self._id = await self.collection.save(self._data)
        return self._id

    async def delete(self):
        await self.collection.delete_one({"_id": self._id})

    async def refresh(self):
        if not self._id:
            raise ValueError(
                "You should save a new object before " + "trying to update it."
            )
        value = await self.collection.find_one({"_id": self._id})
        if value is not None:
            self.set_data(value)
        else:
            raise ValueError("Item %r not found" % self._id)

    @classmethod
    def ensure_indexes(cls, db):
        """Ensures indexes. Warning: Takes a non-async db."""
        indexes = getattr(cls, "INDEXES", None)
        if indexes is None:
            return
        if len(indexes) == 0:
            return

        cls.get_collection(cls, db).create_indexes(indexes)
