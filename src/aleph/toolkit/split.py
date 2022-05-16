from typing import Callable, Iterable, List, Tuple, TypeVar


T = TypeVar("T")


def split_iterable(iterable: Iterable[T], cond: Callable[[T], bool]) -> Tuple[List[T], List[T]]:
    matches = []
    others = []

    for x in iterable:
        if cond(x):
            matches.append(x)
        else:
            others.append(x)

    return matches, others
