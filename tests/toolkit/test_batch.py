import pytest

from aleph.toolkit.batch import async_batch


async def async_range(*args):
    for i in range(*args):
        yield i


@pytest.mark.asyncio
async def test_async_batch():
    # batch with a remainder
    batches = [b async for b in async_batch(async_range(0, 10), 3)]
    assert batches == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    # iterable divisible by n
    batches = [b async for b in async_batch(async_range(0, 4), 2)]
    assert batches == [[0, 1], [2, 3]]

    # n = 1
    batches = [b async for b in async_batch(async_range(0, 5), 1)]
    assert batches == [[0], [1], [2], [3], [4]]

    # n = len(iterable)
    batches = [b async for b in async_batch(async_range(0, 7), 7)]
    assert batches == [[0, 1, 2, 3, 4, 5, 6]]
