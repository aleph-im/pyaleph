"""Ledger form of addresses, shared by the accessors and the fold."""


def normalize_address(address: str) -> str:
    """Ledger form of an address: EVM (0x) addresses lowercased, others as is."""
    return address.lower() if address[:2].lower() == "0x" else address
