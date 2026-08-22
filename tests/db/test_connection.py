from unittest.mock import MagicMock

import pytest

from aleph.db.connection import disposing_engine


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
