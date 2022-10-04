from typing import Dict, Iterable, List, Callable


def get_messages_by_keys(messages: Iterable[Dict], **keys) -> List[Dict]:
    """
    Filters messages based on user-provided keys.

    Example:
    >>> filtered_messages = get_messages_by_keys(
    >>>     message_list, item_hash="some-hash", channel="MY-CHANNEL"
    >>> )

    """
    return list(
        filter(
            lambda msg: all(msg[k] == v for k, v in keys.items()),
            messages,
        )
    )
