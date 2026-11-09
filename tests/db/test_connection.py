from unittest.mock import MagicMock, patch

import pytest

from aleph.db.connection import _timeout_options, disposing_engine, make_engine


def _fake_config(lock=30000, statement=120000, idle=300000):
    config = MagicMock()
    config.postgres.lock_timeout_ms.value = lock
    config.postgres.statement_timeout_ms.value = statement
    config.postgres.idle_in_transaction_session_timeout_ms.value = idle
    config.postgres.pool_size.value = 5
    config.postgres.pool_pre_ping.value = True
    config.postgres.pool_recycle.value = 3600
    return config


@patch("aleph.db.connection.make_db_url", return_value="postgresql://x")
@patch("aleph.db.connection.create_engine")
def test_make_engine_applies_config_timeouts(mock_create_engine, _mock_url):
    make_engine(config=_fake_config())
    options = mock_create_engine.call_args.kwargs["connect_args"]["options"]
    assert options == (
        "-c lock_timeout=30000 "
        "-c statement_timeout=120000 "
        "-c idle_in_transaction_session_timeout=300000"
    )


@patch("aleph.db.connection.make_db_url", return_value="postgresql://x")
@patch("aleph.db.connection.create_engine")
def test_make_engine_no_timeouts_omits_options(mock_create_engine, _mock_url):
    make_engine(config=_fake_config(lock=0, statement=0, idle=0))
    # With every timeout disabled, no libpq options are passed at all.
    assert mock_create_engine.call_args.kwargs["connect_args"] == {}


@patch("aleph.db.connection.make_db_url", return_value="postgresql://x")
@patch("aleph.db.connection.create_engine")
def test_make_engine_statement_timeout_override_disables_it(
    mock_create_engine, _mock_url
):
    # The API passes statement_timeout_ms=0 so slow aggregate reads are not
    # aborted; lock_timeout and idle_in_transaction must still apply.
    make_engine(config=_fake_config(), statement_timeout_ms=0)
    options = mock_create_engine.call_args.kwargs["connect_args"]["options"]
    assert "statement_timeout" not in options
    assert "lock_timeout=30000" in options
    assert "idle_in_transaction_session_timeout=300000" in options


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


def test_make_db_url_round_trips_special_characters():
    from sqlalchemy.engine import make_url

    from aleph.db.connection import make_db_url

    config = MagicMock()
    config.postgres.host.value = "db.example"
    config.postgres.port.value = 5432
    config.postgres.user.value = "us@er"
    config.postgres.password.value = "p@ss:w/rd#1%?"
    config.postgres.database.value = "aleph"

    url = make_url(make_db_url("psycopg2", config, application_name="app"))

    assert url.password == "p@ss:w/rd#1%?"
    assert url.username == "us@er"
    assert url.host == "db.example"
    assert url.port == 5432
    assert url.database == "aleph"
