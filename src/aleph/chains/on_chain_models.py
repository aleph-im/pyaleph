"""
Functions and classes to read/write data stored on-chain by CCNs.
"""

import json
from enum import Enum
from typing import Dict, List, Literal, TypeVar, Union

from pydantic import BaseModel, parse_obj_as


class OnChainAlephProtocolData(BaseModel):
    protocol: Literal["aleph"]
    version: int
    content: List[Dict]


class OnChainAlephOffChainProtocolData(BaseModel):
    protocol: Literal["aleph-offchain"]
    version: int
    content: str


OnChainData = Union[OnChainAlephProtocolData, OnChainAlephOffChainProtocolData]


class OnChainProtocol(str, Enum):
    # Messages are stored on-chain
    ALEPH = "aleph"
    # Messages are stored off-chain, on Aleph nodes or IPFS
    ALEPH_OFF_CHAIN = "aleph-offchain"

    def __int__(self):
        if self == self.ALEPH:
            return 1
        if self == self.ALEPH_OFF_CHAIN:
            return 2

        raise ValueError(f"Unsupported protocol: {self}.")

    @classmethod
    def from_int(cls, value: int) -> "OnChainProtocol":
        if value == 1:
            return cls.ALEPH
        if value == 2:
            return cls.ALEPH_OFF_CHAIN

        raise ValueError(f"Unsupported integer protocol value: {value}.")


def parse_on_chain_data(on_chain_data: str) -> OnChainData:
    """
    Deserializes on-chain data into an object.

    This function takes data stored by a CCN on-chain in any of the supported formats
    (JSON, compact string) and deserializes it into an object.

    :param on_chain_data: Binary data stored on-chain.
    :raises: A ValueError if it cannot decode the format, or an UnknownHashError if
             the hash is invalid.
    """

    try:
        json_data = json.loads(on_chain_data)
        # Ignoring type checking because of this bug in mypy/pydantic:
        # https://github.com/samuelcolvin/pydantic/issues/1847
        return parse_obj_as(OnChainData, json_data)  # type: ignore
    except json.JSONDecodeError:
        pass

    return OnChainAlephOffChainProtocolData(
        protocol=OnChainProtocol.from_int(int(on_chain_data[0])),
        version=int(on_chain_data[1]),
        content=on_chain_data[2:],
    )
