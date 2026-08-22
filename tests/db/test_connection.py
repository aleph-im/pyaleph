from unittest.mock import MagicMock

import pytest

from aleph.db.connection import _timeout_options, disposing_engine


def test_timeout_options_includes_all_nonzero():
    opts = _timeout_options(
        lock_timeout_ms=30000,
        statement_timeout_ms=120000,
        idle_in_transaction_session_timeout_ms=300000,
    )
    assert opts == (
        "-c lock_timeout=30000 "
        "-c statement_timeout=120000 "
        "-c idle_in_transaction_session_timeout=300000"
    )


def test_timeout_options_skips_zero_values():
    opts = _timeout_options(
        lock_timeout_ms=30000,
        statement_timeout_ms=0,
        idle_in_transaction_session_timeout_ms=0,
    )
    assert opts == "-c lock_timeout=30000"


def test_timeout_options_all_zero_is_empty():
    assert (
        _timeout_options(
            lock_timeout_ms=0,
            statement_timeout_ms=0,
            idle_in_transaction_session_timeout_ms=0,
        )
        == ""
    )


@pytest.mark.asyncio
async def test_disposing_engine_disposes_on_exit():
    engine = MagicMock()
    async with disposing_engine(engine) as yielded:
        assert yielded is engine
        engine.dispose.assert_not_called()
    engine.dispose.assert_called_once()


@pytest.mark.asyncio
async def test_disposing_engine_disposes_on_exception():
    engine = MagicMock()
    with pytest.raises(RuntimeError):
        async with disposing_engine(engine):
            raise RuntimeError("boom")
    engine.dispose.assert_called_once()
