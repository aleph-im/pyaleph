from typing import List

from aleph.toolkit.range import Range, MultiRange, int_range
import pytest


@pytest.mark.parametrize(
    "range_str, expected",
    [
        ("[10, 20)", Range(10, 20, lower_inc=True, upper_inc=False)),
        ("[10, 20]", Range(10, 20, lower_inc=True, upper_inc=True)),
        ("(10, 20]", Range(10, 20, lower_inc=False, upper_inc=True)),
        ("(10, 20)", Range(10, 20, lower_inc=False, upper_inc=False)),
    ],
)
def test_build_int_range(range_str: str, expected: Range[int]):
    rng = int_range(range_str)
    assert rng == expected


@pytest.mark.parametrize(
    "range_1, range_2, is_strictly_left",
    [
        (int_range("[1, 2)"), int_range("[3, 4)"), True),
        (int_range("[1, 2)"), int_range("[2, 3)"), False),
        (int_range("[1, 2)"), int_range("[-1, 0)"), False),
    ],
)
def test_strictly_left(
    range_1: Range[int], range_2: Range[int], is_strictly_left: bool
):
    assert range_1.is_strictly_left_of(range_2) == is_strictly_left


@pytest.mark.parametrize(
    "range_1, range_2, expected",
    [
        (Range(1, 10), Range(2, 3), [Range(1, 2), Range(3, 10)]),
        (Range(1, 10), Range(0, 1), [Range(1, 10)]),
        (Range(1, 10), Range(10, 11), [Range(1, 10)]),
        (Range(1, 10), Range(0, 4), [Range(4, 10)]),
        (Range(1, 10), Range(8, 12), [Range(1, 8)]),
    ],
)
def test_sub_ranges(range_1: Range, range_2: Range, expected: List[Range]):
    assert range_1 - range_2 == expected


@pytest.mark.parametrize(
    "range_1, range_2, expected",
    [
        (
            int_range("[1, 10]"),
            int_range("[1, 10)"),
            [Range(10, 10, upper_inc=True)],
        ),
        (
            int_range("[1, 10)"),
            int_range("[0, 10)"),
            [],
        ),
        (
            int_range("[1, 10)"),
            int_range("[0, 10]"),
            [],
        ),
        (
            int_range("[1, 10)"),
            int_range("[4, 7]"),
            [Range(1, 4), Range(7, 10, lower_inc=False)],
        ),
        (
            int_range("[1, 10]"),
            int_range("(5, 7)"),
            [Range(1, 5, upper_inc=True), Range(7, 10, upper_inc=True)],
        ),
        (
            int_range("(1, 10)"),
            int_range("(5, 8]"),
            [
                Range(1, 5, lower_inc=False, upper_inc=True),
                Range(8, 10, lower_inc=False, upper_inc=False),
            ],
        ),
    ],
)
def test_sub_ranges_bounds_combinations(
    range_1: Range, range_2: Range, expected: List[Range]
):
    assert range_1 - range_2 == expected


@pytest.mark.parametrize(
    "range_1, range_2, expected",
    [
        (Range(1, 10), Range(10, 20), Range(1, 20)),
        (Range(1, 10), Range(5, 15), Range(1, 15)),
        (Range(1, 10), Range(0, 20), Range(0, 20)),
        (Range(1, 10), Range(20, 30), Range(1, 10)),
    ],
)
def test_add_ranges(range_1: Range, range_2: Range, expected: Range):
    assert range_1 + range_2 == expected


@pytest.mark.parametrize(
    "range_1, range_2, expected",
    [
        (Range(1, 10), Range(0, 3), True),
        (Range(1, 10), Range(8, 12), True),
        (Range(1, 10), Range(0, 100), True),
        (Range(1, 10), Range(5, 7), True),
        (Range(1, 3), Range(1, 3), True),
        (Range(0, 3), Range(6, 9), False),
        (Range(0, 3), Range(-4, -2), False),
        (Range(-1, 1), Range(1, 3), True),
        (Range(-1, 1), Range(-3, -1), True),
        (Range(-1, 1, upper_inc=False), Range(1, 3, lower_inc=False), False),
        (Range(-1, 1, lower_inc=False), Range(-3, -1, upper_inc=False), False),
        (Range(-1, 1, upper_inc=True), Range(1, 3, lower_inc=True), True),
    ],
    ids=[
        "overlap-left",
        "overlap-right",
        "included-in-range-2",
        "includes-range-2",
        "equality",
        "disjoint-left",
        "disjoint-right",
        "disjoint-touch-left",
        "disjoint-touch-right",
        "disjoint-touch-left-not-included",
        "disjoint-touch-right-not-included",
        "touch-double-inclusion",
    ],
)
def test_overlaps(range_1: Range[int], range_2: Range[int], expected: bool):
    assert range_1.overlaps(range_2) == expected
    assert range_2.overlaps(range_1) == expected


@pytest.mark.parametrize(
    "rng, multirange, expected",
    [
        (
            Range(1, 100),
            MultiRange(Range(-10, 2), Range(5, 10), Range(20, 90), Range(120, 130)),
            [Range(2, 5), Range(10, 20), Range(90, 100)],
        )
    ],
)
def test_range_remove_multirange(
    rng: Range[int], multirange: MultiRange[int], expected: List[Range[int]]
):
    assert rng.remove_multirange(multirange) == expected


@pytest.mark.parametrize(
    "multirange_1, multirange_2, should_match",
    [
        (MultiRange(Range(1, 10)), MultiRange(Range(1, 10)), True),
        (MultiRange(Range(0, 1)), MultiRange(Range(2, 3)), False),
        (
            MultiRange(Range(1, 10), Range(20, 30)),
            MultiRange(Range(1, 10), Range(20, 30)),
            True,
        ),
        (
            MultiRange(Range(0, 1), Range(20, 30)),
            MultiRange(Range(0, 1), Range(1, 2)),
            False,
        ),
        (
            MultiRange(Range(0, 1), Range(2, 3), Range(4, 5)),
            MultiRange(Range(0, 1), Range(2, 3)),
            False,
        ),
    ],
)
def test_multiranges_eq(
    multirange_1: MultiRange[int], multirange_2: MultiRange[int], should_match: bool
):
    if should_match:
        assert multirange_1 == multirange_2
    else:
        assert multirange_1 != multirange_2


@pytest.mark.parametrize(
    "multirange, rng, expected",
    [
        # Add two simple multiranges
        (
            MultiRange(Range(1, 10)),
            Range(20, 30),
            MultiRange(Range(1, 10), Range(20, 30)),
        ),
        # Insert a range in the middle, no merging
        (
            MultiRange(Range(1, 10), Range(40, 50)),
            Range(20, 30),
            MultiRange(Range(1, 10), Range(20, 30), Range(40, 50)),
        ),
        # Merge left
        (
            MultiRange(Range(1, 10), Range(40, 50)),
            Range(0, 1),
            MultiRange(Range(0, 10), Range(40, 50)),
        ),
        # Merge in the middle
        (
            MultiRange(Range(1, 10), Range(40, 50)),
            Range(10, 20),
            MultiRange(Range(1, 20), Range(40, 50)),
        ),
        # Merge right
        (
            MultiRange(Range(1, 10), Range(40, 50)),
            Range(50, 60),
            MultiRange(Range(1, 10), Range(40, 60)),
        ),
        # Insert merge two
        (
            MultiRange(Range(1, 10), Range(20, 30), Range(40, 50)),
            Range(10, 20),
            MultiRange(Range(1, 30), Range(40, 50)),
        ),
        # Insert merge multiple
        (
            MultiRange(
                Range(-1, 0), Range(1, 10), Range(20, 30), Range(40, 50), Range(90, 100)
            ),
            Range(5, 45),
            MultiRange(Range(-1, 0), Range(1, 50), Range(90, 100)),
        ),
        # Merge all
        (
            MultiRange(
                Range(-1, 0), Range(1, 10), Range(20, 30), Range(40, 50), Range(90, 100)
            ),
            Range(-10, 110),
            MultiRange(Range(-10, 110)),
        ),
    ],
    ids=[
        "add-disjoint-range",
        "insert-no-merge",
        "merge-left",
        "merge-insert",
        "merge-right",
        "insert-merge-two",
        "insert-merge-multiple",
        "merge-all",
    ],
)
def test_multirange_add_range(
    multirange: MultiRange[int], rng: Range[int], expected: MultiRange[int]
):
    multirange.add_range(rng)
    assert multirange == expected


@pytest.mark.parametrize(
    "multirange_1, multirange_2, expected",
    [
        (
            MultiRange(Range(1, 10), Range(40, 50)),
            MultiRange(Range(1, 10), Range(20, 30)),
            MultiRange(Range(40, 50)),
        ),
        (
            MultiRange(Range(0, 100), Range(200, 300)),
            MultiRange(Range(20, 50), Range(80, 220), Range(280, 400)),
            MultiRange(Range(0, 20), Range(50, 80), Range(220, 280)),
        ),
        (
            MultiRange(int_range("[0, 100]"), int_range("[200, 300]")),
            MultiRange(int_range("[20, 100]"), int_range("[200, 250]")),
            MultiRange(int_range("[0, 20)"), int_range("(250, 300]")),
        ),
        (
            MultiRange(int_range("[0, 100]"), int_range("[200, 300]")),
            MultiRange(),
            MultiRange(int_range("[0, 100]"), int_range("[200, 300]")),
        ),
    ],
)
def test_sub_multiranges(
    multirange_1: MultiRange[int],
    multirange_2: MultiRange[int],
    expected: MultiRange[int],
):
    assert multirange_1 - multirange_2 == expected
