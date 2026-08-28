import asyncio
import logging
import signal
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol

LOGGER = logging.getLogger(__name__)


class _AsyncCloseable(Protocol):
    async def close(self) -> Any: ...


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop, callback: Callable[[], object]
) -> None:
    """Register SIGINT and SIGTERM handlers that invoke `callback`.

    Re-registering replaces the previous handler. Silently no-ops on
    platforms that do not support `loop.add_signal_handler` (Windows).
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, callback)
        except NotImplementedError:
            LOGGER.debug("Signal handler for %s not available on this platform", sig)


async def safe_async_cleanup(name: str, awaitable: Awaitable[object]) -> None:
    """Await a cleanup coroutine, logging but never re-raising on failure.

    Cleanup paths run during shutdown and must not mask the shutdown signal.
    """
    try:
        await awaitable
    except Exception:
        LOGGER.exception("Error during cleanup of %s", name)


@asynccontextmanager
async def closing_quietly(
    name: str, closeable: _AsyncCloseable
) -> AsyncIterator[_AsyncCloseable]:
    """Yield ``closeable``, awaiting ``.close()`` on exit via safe_async_cleanup.

    For shutdown paths that want a resource closed without a close() failure
    (e.g. an unreachable broker) masking the original exception.
    """
    try:
        yield closeable
    finally:
        await safe_async_cleanup(name, closeable.close())
