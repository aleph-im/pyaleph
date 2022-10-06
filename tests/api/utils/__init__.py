from typing import Dict, Iterable, List, Callable


def get_messages_by_predicate(
    messages: Iterable[Dict], predicate: Callable[[Dict], bool]
) -> List[Dict]:
    """
    Filters messages based on a user-provided predicate
    (=a function/lambda operating on a single message).
    """

    return [msg for msg in messages if predicate(msg)]


def get_messages_by_keys(messages: Iterable[Dict], **keys) -> List[Dict]:
    """
    Filters messages based on user-provided keys.

    Example:
    >>> filtered_messages = get_messages_by_keys(
    >>>     message_list, item_hash="some-hash", channel="MY-CHANNEL"
    >>> )

    """
    return get_messages_by_predicate(
        messages, lambda msg: all(msg[k] == v for k, v in keys.items())
    )
