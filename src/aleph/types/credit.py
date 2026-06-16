from enum import Enum


class CreditFlow(str, Enum):
    """Sign-based classification of a credit history entry.

    INCOMING: amount > 0 (distributions, received transfers).
    OUTGOING: amount < 0 (expenses, sent transfers).
    Zero-amount entries match neither flow.
    """

    INCOMING = "incoming"
    OUTGOING = "outgoing"
