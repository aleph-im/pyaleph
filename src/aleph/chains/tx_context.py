from dataclasses import dataclass


@dataclass
class TxContext:
    chain_name: str
    tx_hash: str
    height: int
    time: int
    publisher: str
