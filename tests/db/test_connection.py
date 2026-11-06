import contextlib
import socket
import time
from typing import Iterator
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import psycopg2
import pytest
import sqlalchemy.exc
from configmanager import Config
from sqlalchemy import text

import aleph.config
from aleph.db.connection import (
    _timeout_options,
    disposing_engine,
    make_async_engine,
    make_db_url,
    make_engine,
)


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


def _default_config() -> Config:
    return Config(aleph.config.get_defaults())


def _query(url: str) -> dict:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def test_psycopg2_url_carries_connection_settings():
    url = make_db_url("psycopg2", _default_config(), application_name="aleph-test")
    assert _query(url) == {
        "application_name": "aleph-test",
        "connect_timeout": "10",
        "keepalives": "1",
        "keepalives_idle": "60",
        "keepalives_interval": "10",
        "keepalives_count": "5",
    }


def test_psycopg2_url_omits_zero_keepalive_settings():
    config = _default_config()
    config.postgres.keepalives_interval_s.value = 0
    url = make_db_url("psycopg2", config)
    query = _query(url)
    assert "keepalives_interval" not in query
    assert query["keepalives"] == "1"
    assert query["keepalives_idle"] == "60"
    assert query["keepalives_count"] == "5"


def test_psycopg2_url_omits_keepalives_when_all_zero():
    config = _default_config()
    config.postgres.keepalives_idle_s.value = 0
    config.postgres.keepalives_interval_s.value = 0
    config.postgres.keepalives_count.value = 0
    config.postgres.connect_timeout_s.value = 0
    query = _query(make_db_url("psycopg2", config))
    assert not any(key.startswith("keepalives") for key in query)
    # 0 = no limit, which is libpq's behaviour without the parameter.
    assert "connect_timeout" not in query


def test_url_omits_sslmode_when_unset():
    # Unset must leave PGSSLMODE in charge, so the URL must not pin a mode.
    config = _default_config()
    assert "sslmode" not in _query(make_db_url("psycopg2", config))
    assert _query(make_db_url("asyncpg", config)) == {}


def test_psycopg2_url_carries_sslmode_when_set():
    config = _default_config()
    config.postgres.sslmode.value = "verify-full"
    assert _query(make_db_url("psycopg2", config))["sslmode"] == "verify-full"


def test_asyncpg_url_carries_only_parameters_asyncpg_accepts():
    config = _default_config()
    config.postgres.sslmode.value = "require"
    url = make_db_url("asyncpg", config, application_name="aleph-test")
    # Every URL query key becomes an asyncpg.connect() keyword argument (as a
    # string): asyncpg has no application_name/keepalive arguments and needs
    # a numeric timeout, so only `ssl` can travel in the URL.
    assert _query(url) == {"ssl": "require"}


@contextlib.contextmanager
def silent_listener() -> Iterator[int]:
    """A local port that accepts TCP connections and never answers.

    This is what a router with no backend does (the incident this setting
    fixes): the TCP handshake succeeds, then nothing. Without a connect
    timeout the client waits forever.
    """
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        yield sock.getsockname()[1]
    finally:
        sock.close()


def test_psycopg2_connect_timeout_bounds_silent_server(mock_config):
    with silent_listener() as port:
        mock_config.postgres.port.value = port
        mock_config.postgres.connect_timeout_s.value = 2
        engine = make_engine(config=mock_config)
        start = time.monotonic()
        try:
            with pytest.raises(sqlalchemy.exc.OperationalError, match="timeout"):
                with engine.connect():
                    pass
        finally:
            engine.dispose()
        assert time.monotonic() - start < 6


def _ssl_in_use(engine) -> bool:
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        ).scalar_one()


def test_psycopg2_sslmode_reaches_libpq(mock_config):
    mock_config.postgres.sslmode.value = "disable"
    engine = make_engine(config=mock_config)
    try:
        assert _ssl_in_use(engine) is False
    finally:
        engine.dispose()

    # Depending on the server, `require` either gets TLS or is refused by
    # libpq before authenticating; both prove the mode reached libpq (the
    # default `prefer` would silently fall back to plaintext).
    mock_config.postgres.sslmode.value = "require"
    engine = make_engine(config=mock_config)
    try:
        assert _ssl_in_use(engine) is True
    except sqlalchemy.exc.OperationalError as e:
        assert "server does not support SSL" in str(e)
    finally:
        engine.dispose()


@pytest.mark.skipif(
    not hasattr(socket, "TCP_KEEPIDLE"), reason="Linux TCP keepalive options"
)
def test_psycopg2_keepalives_reach_the_socket(mock_config):
    mock_config.postgres.keepalives_idle_s.value = 42
    mock_config.postgres.keepalives_interval_s.value = 7
    mock_config.postgres.keepalives_count.value = 3
    engine = make_engine(config=mock_config)
    try:
        raw = engine.raw_connection()
        try:
            pg_conn = raw.driver_connection
            assert isinstance(pg_conn, psycopg2.extensions.connection)
            sock = socket.fromfd(pg_conn.fileno(), socket.AF_INET, socket.SOCK_STREAM)
            try:
                assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == 42
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) == 7
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 3
            finally:
                sock.close()
        finally:
            raw.close()
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_async_engine_connects_with_default_settings(mock_config):
    engine = make_async_engine(config=mock_config, application_name="aleph-test")
    try:
        async with engine.connect() as conn:
            name = (
                await conn.execute(text("SELECT current_setting('application_name')"))
            ).scalar_one()
        assert name == "aleph-test"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_async_engine_connect_timeout_bounds_silent_server(mock_config):
    with silent_listener() as port:
        mock_config.postgres.port.value = port
        mock_config.postgres.connect_timeout_s.value = 2
        engine = make_async_engine(config=mock_config)
        start = time.monotonic()
        try:
            with pytest.raises(TimeoutError):
                async with engine.connect():
                    pass
        finally:
            await engine.dispose()
        assert time.monotonic() - start < 6
