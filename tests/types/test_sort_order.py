import pytest
from pydantic import TypeAdapter

from aleph.types.sort_order import SortOrder, SortOrderForMetrics

sort_order_adapter = TypeAdapter(SortOrderForMetrics)


@pytest.mark.parametrize(
    "sort_order,expected",
    [
        ("asc", SortOrder.ASCENDING),
        ("desc", SortOrder.DESCENDING),
        ("ASC", SortOrder.ASCENDING),
        ("DESC", SortOrder.DESCENDING),
        (1, SortOrder.ASCENDING),
        (-1, SortOrder.DESCENDING),
    ],
)
def test_sort_order_for_metrics_deserialization(sort_order, expected: SortOrder):
    result = sort_order_adapter.validate_python(sort_order)
    assert result == expected
