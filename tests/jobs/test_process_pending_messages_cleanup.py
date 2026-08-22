from unittest.mock import AsyncMock, MagicMock

import pytest

from aleph.jobs.process_pending_messages import PendingMessageProcessor


@pytest.mark.asyncio
async def test_processor_closes_its_mq_connection_on_exit():
    """PendingMessageProcessor owns the MQ connection created in new(); its
    __aexit__ must close it (the base MqWatcher.__aexit__ only cancels the
    watcher task, so without this override the connection leaks)."""
    mq_conn = AsyncMock()
    processor = PendingMessageProcessor(
        session_factory=MagicMock(),
        message_handler=MagicMock(),
        max_retries=0,
        mq_conn=mq_conn,
        mq_message_exchange=MagicMock(),
        pending_message_queue=MagicMock(),
    )

    # No __aenter__, so the base watcher task is None and its __aexit__ is a
    # no-op; the override must still close the connection.
    await processor.__aexit__(None, None, None)

    mq_conn.close.assert_awaited_once()
