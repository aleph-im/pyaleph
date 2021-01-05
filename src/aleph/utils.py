import asyncio

from aleph.settings import settings


async def run_in_executor(executor, func, *args):
    if settings.use_executors:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, func, *args)
    else:
        return func(*args)
