from __future__ import annotations
from enum import Enum

from aleph.exceptions import UnknownHashError


class ItemType(str, Enum):
    """Item storage options"""
    Inline = "inline"
    IPFS = "ipfs"
    Storage = "storage"

    @classmethod
    def from_hash(cls, hash: str) -> ItemType:
        assert isinstance(hash, str)
        # https://docs.ipfs.io/concepts/content-addressing/#identifier-formats
        if hash.startswith("Qm") and 44 <= len(hash) <= 46: # CIDv0
            return cls.IPFS
        elif hash.startswith("bafy") and len(hash) == 59:  # CIDv1
            return cls.IPFS
        elif len(hash) == 64:
            return cls.Storage
        else:
            raise UnknownHashError(f"Unknown hash {len(hash)} {hash}")


class Protocol(str, Enum):
    """P2P Protocol"""
    IPFS = "ipfs"
    P2P = "p2p"
