from enum import Enum


class ChainSyncProtocol(str, Enum):
    ON_CHAIN = "aleph"
    OFF_CHAIN = "aleph-offchain"
