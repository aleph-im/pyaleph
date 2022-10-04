"""
An abstraction layer for JSON serialization/deserialization.
Makes swapping between JSON implementations easier.
"""


import json
from typing import Any, IO, Union

# The actual type of serialized JSON as returned by the JSON serializer.
SerializedJson = str

# All the possible types for serialized JSON. This type is useful to force functions
# to handle all possible cases when using serialized JSON as input in order to make
# serializer changes easier.
SerializedJsonInput = Union[bytes, str]


# Note: JSONDecodeError is a subclass of ValueError, but the JSON module sometimes throws
#       raw value errors, including on NaN because of our custom parse_constant.
DecodeError = ValueError


def _parse_constant(c: str) -> float:
    if c == "NaN":
        raise ValueError("NaN is not valid JSON")
    return float(c)


def load(fp: IO) -> Any:
    return json.load(fp)


def loads(s: Union[bytes, str]) -> Any:
    return json.loads(s, parse_constant=_parse_constant)


def dump(fp: IO, obj: Any) -> None:
    return json.dump(fp, obj)


def dumps(obj: Any) -> SerializedJson:
    return json.dumps(obj)
