from dataclasses import dataclass


@dataclass
class TxContext:
    chain: str
    hash: str
    height: int
    # Transaction timestamp, in Unix time (number of seconds since epoch).
    time: int
    publisher: str
