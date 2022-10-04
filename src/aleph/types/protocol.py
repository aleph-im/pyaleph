from __future__ import annotations

from enum import Enum


class Protocol(str, Enum):
    """P2P Protocol"""
    IPFS = "ipfs"
    P2P = "p2p"
