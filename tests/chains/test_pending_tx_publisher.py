from unittest.mock import AsyncMock, MagicMock

import pytest

from aleph.chains.chain_data_service import PendingTxPublisher


@pytest.mark.asyncio
async def test_pending_tx_publisher_closes_connection_on_exit():
    """The publisher owns its MQ connection and must close it on context exit
    (regression: the connection used to be a local in make_pending_tx_exchange
    and was leaked for the process lifetime)."""
    mq_conn = AsyncMock()
    publisher = PendingTxPublisher(mq_conn=mq_conn, pending_tx_exchange=MagicMock())

    async with publisher as entered:
        assert entered is publisher
        mq_conn.close.assert_not_called()

    mq_conn.close.assert_awaited_once()
