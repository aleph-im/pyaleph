"""
An abstraction layer for JSON serialization/deserialization.
Makes swapping between JSON implementations easier.
"""

import json
from typing import IO, Any, Union

import orjson

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


def dumps(obj: Any) -> bytes:
    try:
        return orjson.dumps(obj)
    except TypeError:
        return json.dumps(obj).encode()
