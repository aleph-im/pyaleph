import asyncio
from functools import wraps


def shielded(func):
    """
    Protects a coroutine from cancellation.
    """
    @wraps(func)
    async def wrapped(*args, **kwargs):
        return await asyncio.shield(func(*args, **kwargs))

    return wrapped
