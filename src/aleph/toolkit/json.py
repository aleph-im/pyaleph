"""
An abstraction layer for JSON serialization/deserialization.
Makes swapping between JSON implementations easier.
"""

import json
from datetime import date, datetime, time
from typing import IO, Any, Union

import orjson
import pydantic

# The actual type of serialized JSON as returned by the JSON serializer.
SerializedJson = bytes

# All the possible types for serialized JSON. This type is useful to force functions
# to handle all possible cases when using serialized JSON as input in order to make
# serializer changes easier.
SerializedJsonInput = Union[bytes, str]

# Note: JSONDecodeError is a subclass of ValueError, but the JSON module sometimes throws
#       raw value errors, including on NaN because of our custom parse_constant.
DecodeError = orjson.JSONDecodeError


def load(fp: IO) -> Any:
    raise NotImplementedError("orjson does not provide load")


def loads(s: Union[bytes, str]) -> Any:
    try:
        return orjson.loads(s)
    except TypeError:
        return json.loads(s)


def dump(fp: IO, obj: Any) -> None:
    raise NotImplementedError("orjson does not provide dump")


def extended_json_encoder(obj: Any) -> Any:
    """
    Extended JSON encoder for dumping objects that contain pydantic models and datetime objects.
    """
    if isinstance(obj, datetime):
        return obj.timestamp()
    elif isinstance(obj, date):
        return obj.toordinal()
    elif isinstance(obj, time):
        return obj.hour * 3600 + obj.minute * 60 + obj.second + obj.microsecond / 1e6
    elif isinstance(obj, pydantic.BaseModel):
        return obj.model_dump()
    else:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def dumps(obj: Any, sort_keys: bool = True) -> bytes:
    try:
        opts = orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS if sort_keys else 0
        return orjson.dumps(obj, option=opts)
    except TypeError:
        return json.dumps(
            obj, default=extended_json_encoder, sort_keys=sort_keys
        ).encode()
