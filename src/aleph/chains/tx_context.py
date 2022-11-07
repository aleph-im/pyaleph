from dataclasses import dataclass


@dataclass
class TxContext:
    chain_name: str
    tx_hash: str
    height: int
    # Transaction timestamp, in Unix time (number of seconds since epoch).
    time: float
    publisher: str
