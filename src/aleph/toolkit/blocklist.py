from typing import Iterable


def is_blocked_sender(sender: str, blocked_senders: Iterable[str]) -> bool:
    """
    Check whether a message sender is in the node's blocklist.

    Addresses are compared case-insensitively and surrounding whitespace is
    ignored, so configuration values may be checksummed or lowercase.
    """
    normalized_sender = sender.strip().lower()
    if isinstance(blocked_senders, str):
        blocked_senders = [blocked_senders]
    return any(
        blocked_sender.strip().lower() == normalized_sender
        for blocked_sender in blocked_senders or ()
    )
