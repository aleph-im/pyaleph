from typing import Any, Callable, Generic, List, Protocol, TypeVar


class Comparable(Protocol):
    def __lt__(self, other):
        pass

    def __le__(self, other):
        pass

    def __eq__(self, other):
        pass


T = TypeVar("T", bound=Comparable)


class Range(Generic[T]):
    def __init__(
        self,
        lower: T,
        upper: T,
        lower_inc: bool = True,
        upper_inc: bool = False,
    ):
        if upper < lower:
            raise ValueError(
                f"Range start ({lower}) must be lower than range end ({upper})"
            )

        self.lower = lower
        self.upper = upper
        self.lower_inc = lower_inc
        self.upper_inc = upper_inc

    @classmethod
    def from_str(cls, range_str: str, parser: Callable[[str], T]) -> "Range[T]":
        left = range_str[0]
        right = range_str[-1]
        lower, upper = range_str[1:-1].split(",")
        lower_inc = True if left == "[" else False
        upper_inc = True if right == "]" else False
        return cls(parser(lower), parser(upper), lower_inc, upper_inc)

    def _check_types(self, other: Any):
        if isinstance(other, Range):
            if not isinstance(other.lower, type(self.lower)):
                raise TypeError(
                    f"Cannot subtract Range[{type(other.lower)}] from Range[{type(self.lower)}]"
                )
        elif not isinstance(other, MultiRange):
            raise TypeError(f"Cannot use {type(other)} with Range operations")

    def __repr__(self):
        return f"Range {str(self)}"

    def __str__(self):
        left = "[" if self.lower_inc else "("
        right = "]" if self.upper_inc else ")"
        return f"{left}{self.lower},{self.upper}{right}"

    def is_strictly_left_of(self, other: "Range[T]"):
        if self.upper_inc or other.lower_inc:
            return self.upper < other.lower

        return self.upper <= other.lower

    def is_strictly_right_of(self, other: "Range[T]"):
        return other.is_strictly_left_of(self)

    def __eq__(self, other):
        self._check_types(other)
        return (
            self.lower == other.lower
            and self.upper == other.upper
            and self.lower_inc == other.lower_inc
            and self.upper_inc == other.upper_inc
        )

    def __add__(self, other) -> "Range[T]":
        """
        Combines this range and the other one, if they intersect.

        Ex: [1, 10] + [10, 20] = [1, 20]
            [1, 10] + [5, 15] = [1, 15]
            [1, 10] + [20, 30] = [1, 10]
        """

        self._check_types(other)
        if self.overlaps(other):
            return Range(min(self.lower, other.lower), max(self.upper, other.upper))

        return self

    def remove_multirange(self, multirange: "MultiRange[T]") -> List["Range[T]"]:
        missing_ranges = [self]

        for rng in multirange.ranges:
            _missing_ranges = []
            for missing_range in missing_ranges:
                _missing_ranges += missing_range - rng
            missing_ranges = _missing_ranges

        return missing_ranges

    def __sub__(self, other) -> List["Range[T]"]:
        self._check_types(other)

        if not self.overlaps(other):
            return [self]

        if self.lower < other.lower:
            if self.upper <= other.upper:
                return [
                    Range(self.lower, other.lower, self.lower_inc, not other.lower_inc)
                ]
            else:
                return [
                    Range(self.lower, other.lower, self.lower_inc, not other.lower_inc),
                    Range(other.upper, self.upper, not other.upper_inc, self.upper_inc),
                ]
        else:
            if self.upper > other.upper:
                return [
                    Range(other.upper, self.upper, not other.upper_inc, self.upper_inc)
                ]
            # ex: [1, 10] - [1, 10) = [10, 10]
            elif self.upper == other.upper and (self.upper_inc and not other.upper_inc):
                return [
                    Range(other.upper, self.upper, not other.upper_inc, self.upper_inc)
                ]
            else:
                return []

    def overlaps(self, other: "Range[T]"):
        if self.lower > other.upper or self.upper < other.lower:
            # No intersection in all cases
            return False

        if self.lower < other.lower:
            if self.upper > other.lower:
                return True
            elif self.upper == other.lower and (self.upper_inc or other.lower_inc):
                return True
            else:
                return False
        elif self.lower == other.lower and (self.lower_inc or other.lower_inc):
            return True
        else:
            if self.lower < other.upper:
                return True
            elif self.lower == other.upper and (self.lower_inc or other.upper_inc):
                return True
            else:
                return False


def int_range(range_str: str) -> Range[int]:
    return Range.from_str(range_str, int)


class MultiRange(Generic[T]):
    ranges: List[Range[T]]

    def __init__(self, *ranges: Range[T]):
        self.ranges = sorted(ranges, key=lambda rng: rng.lower)

    def __repr__(self):
        ranges_str = ", ".join(str(r) for r in self.ranges)
        return f"Multirange({ranges_str})"

    def __len__(self):
        return len(self.ranges)

    def __bool__(self):
        return bool(self.ranges)

    def __iter__(self):
        for rng in self.ranges:
            yield rng

    def __str__(self):
        return str(self.ranges)

    def _check_types(self, other: Any):
        if not isinstance(other, MultiRange):
            raise TypeError(f"Cannot subtract {type(other)} from MultiRange")

    def __eq__(self, other: Any):
        self._check_types(other)

        if len(self.ranges) != len(other.ranges):
            return False

        for rng, other_rng in zip(self.ranges, other.ranges):
            if rng != other_rng:
                return False

        return True

    def add_range(self, other: Range[T]):
        left_ranges = []
        right_ranges = []

        for rng in self.ranges:
            if other.overlaps(rng):
                other += rng
            else:
                if rng.is_strictly_left_of(other):
                    left_ranges.append(rng)
                else:
                    right_ranges.append(rng)

        self.ranges = left_ranges + [other] + right_ranges

    def __add__(self, other) -> "MultiRange[T]":
        if isinstance(other, Range):
            self.add_range(other)
            return self

        raise TypeError(f"Unsupported type for multirange addition: {type(other)}")

    def __sub__(self, other) -> "MultiRange[T]":
        self._check_types(other)

        missing_ranges = []
        for rng in self.ranges:
            missing_ranges += rng.remove_multirange(other)

        return MultiRange(*missing_ranges)
