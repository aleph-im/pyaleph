from typing import AsyncIterator, List, TypeVar

T = TypeVar("T")


async def async_batch(
    async_iterable: AsyncIterator[T], n: int
) -> AsyncIterator[List[T]]:
    batch = []
    async for item in async_iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []

    # Yield the last batch
    if batch:
        yield batch
