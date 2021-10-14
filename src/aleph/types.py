from __future__ import annotations
from enum import Enum


class ItemType(str, Enum):
    """Item storage options"""
    Inline = "inline"
    IPFS = "ipfs"
    Storage = "storage"

    @classmethod
    def from_hash(cls, hash: str) -> ItemType:
        assert isinstance(hash, str)
        if len(hash) == 46:
            return cls.IPFS
        elif len(hash) == 64:
            return cls.Storage
        else:
            raise ValueError("Unknown hash")


class Protocol(str, Enum):
    """P2P Protocol"""
    IPFS = "ipfs"
    P2P = "p2p"
