from typing import Dict, Iterable, List, Callable


def get_messages_by_predicate(
    messages: Iterable[Dict], predicate: Callable[[Dict], bool]
) -> List[Dict]:
    return [msg for msg in messages if predicate(msg)]


def get_messages_by_keys(messages: Iterable[Dict], **keys) -> List[Dict]:
    return get_messages_by_predicate(
        messages, lambda msg: all(msg[k] == v for k, v in keys.items())
    )
