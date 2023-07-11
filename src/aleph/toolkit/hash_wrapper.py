from typing import TypeVar, Callable

T = TypeVar("T")


class HashWrapper:
    """
    A wrapper class to use set operations on objects with an alternate implementation
    of __hash__ without modifying the original class.
    """

    def __init__(self, obj: T, hash_func: Callable[[T], int]):
        self.obj = obj
        self.hash_func = hash_func

    def __hash__(self):
        return self.hash_func(self.obj)
