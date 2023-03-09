from enum import Enum


class ChainSyncProtocol(str, Enum):
    # Message sync tx where the messages are in the tx data
    ON_CHAIN_SYNC = "aleph"
    # Message sync tx where the messages to fetch are in an IPFS hash
    OFF_CHAIN_SYNC = "aleph-offchain"
    # Messages sent by a smart contract
    SMART_CONTRACT = "smart-contract"


class ChainEventType(str, Enum):
    # Messages sent on-chain using the Aleph smart contract.
    MESSAGE = "message"
    # Synchronisation messages sent by a CCN to the Aleph smart contract.
    SYNC = "sync"
