import logging
from typing import Optional, Union

from bson.objectid import ObjectId

from aleph.model import hashes
from aleph.web import app

LOGGER = logging.getLogger("filestore")


async def get_value(key: str) -> Optional[bytes]:
    engine = app["config"].storage.engine.value

    if engine != "mongodb":
        raise ValueError(f"Unsupported storage engine: '{engine}'.")

    if not isinstance(key, str):
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        else:
            raise ValueError("Bad input key (bytes or string only)")
    return await hashes.get_value(key)


async def set_value(key: Union[bytes, str], value: Union[bytes, str]) -> ObjectId:
    engine = app["config"].storage.engine.value

    if engine != "mongodb":
        raise ValueError(f"Unsupported storage engine: '{engine}'.")

    if not isinstance(value, bytes):
        if isinstance(value, str):
            value = value.encode("utf-8")
        else:
            raise ValueError("Bad input value (bytes or string only)")

    if not isinstance(key, str):
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        else:
            raise ValueError("Bad input key (bytes or string only)")
    return await hashes.set_value(key, value)
