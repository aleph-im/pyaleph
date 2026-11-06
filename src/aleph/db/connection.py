from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional
from urllib.parse import quote, urlencode

from configmanager import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker

from aleph.config import get_config
from aleph.toolkit.credentials import (
    PasswordSource,
    install_password_hook,
    read_credential,
)
from aleph.types.db_session import DbSessionFactory


@asynccontextmanager
async def disposing_engine(engine: Engine) -> AsyncIterator[Engine]:
    """Yield ``engine``, disposing its connection pool on exit.

    Lets a long-running subprocess close its DB connections gracefully on
    shutdown instead of dropping them (which leaves the Postgres backends to
    time out and logs unexpected-EOF on the server).
    """
    try:
        yield engine
    finally:
        engine.dispose()


def _connection_query_params(
    driver: str, config: Config, application_name: Optional[str] = None
) -> Dict[str, str]:
    """Connection settings to carry in the URL query string for ``driver``.

    Putting them in the URL (rather than in connect_args) means every consumer
    of make_db_url gets them, including the migrations engine.
    """
    postgres = config.postgres

    if driver == "asyncpg":
        # SQLAlchemy passes each query key to asyncpg.connect() as a string
        # keyword argument. asyncpg has no application_name or keepalive
        # arguments and needs a numeric timeout, so only the TLS mode fits
        # here; make_async_engine passes the rest through connect_args.
        if postgres.sslmode.value:
            return {"ssl": postgres.sslmode.value}
        return {}

    params: Dict[str, str] = {}
    if application_name:
        params["application_name"] = application_name
    if postgres.connect_timeout_s.value:
        params["connect_timeout"] = str(postgres.connect_timeout_s.value)
    # Unset leaves libpq to PGSSLMODE (or its "prefer" default).
    if postgres.sslmode.value:
        params["sslmode"] = postgres.sslmode.value

    keepalives = {
        "keepalives_idle": postgres.keepalives_idle_s.value,
        "keepalives_interval": postgres.keepalives_interval_s.value,
        "keepalives_count": postgres.keepalives_count.value,
    }
    if any(keepalives.values()):
        params["keepalives"] = "1"
        params.update({key: str(value) for key, value in keepalives.items() if value})

    return params


_PASSWORD_WHAT = "postgres.password_file"


def make_password_source(config: Config) -> Optional[PasswordSource]:
    """The per-connection password source, or None without a password file."""
    password_file = config.postgres.password_file.value
    if not password_file:
        return None
    return PasswordSource(
        Path(password_file),
        fallback=config.postgres.password.value,
        what=_PASSWORD_WHAT,
    )


def install_password_file_hook(engine: Engine, config: Config) -> None:
    """Re-read ``postgres.password_file`` for every new psycopg2 connection.

    Does nothing when no password file is configured.
    """
    source = make_password_source(config)
    if source is not None:
        install_password_hook(engine, source)


def make_db_url(
    driver: str, config: Config, application_name: Optional[str] = None
) -> str:
    """
    Returns the database connection string from configuration values.

    :param driver: Driver name. Ex: psycopg2, asyncpg.
    :param config: Configuration. If not specified, the global configuration object is used.
    :param application_name: Application name (psycopg2 only, asyncpg takes it
                             from make_async_engine).
    :returns: The database connection string.
    """

    host = config.postgres.host.value
    port = config.postgres.port.value
    user = config.postgres.user.value
    # The file, if set, is read now: a missing or empty one fails here, at
    # startup. Engines from make_engine / make_async_engine read it again for
    # each new connection.
    password = read_credential(
        config.postgres.password.value,
        None,
        config.postgres.password_file.value,
        what=_PASSWORD_WHAT,
    )
    database = config.postgres.database.value

    # Percent-encode credentials so characters such as '@', '/', ':' or '#' in a
    # password cannot corrupt the URL.
    connection_string = f"postgresql+{driver}://{quote(str(user), safe='')}:"

    if password is not None:
        connection_string += quote(str(password), safe="")

    connection_string += "@"

    if host is not None:
        connection_string += f"{host}:{port}"

    connection_string += f"/{database}"

    query = _connection_query_params(driver, config, application_name)
    if query:
        connection_string += f"?{urlencode(query)}"

    return connection_string


def _timeout_options(
    lock_timeout_ms: int,
    statement_timeout_ms: int,
    idle_in_transaction_session_timeout_ms: int,
) -> str:
    """Build the libpq ``options`` string for the per-session timeouts.

    Each timeout is included only when non-zero (0 = disabled, Postgres default
    of no limit).
    """
    parts = []
    if lock_timeout_ms:
        parts.append(f"-c lock_timeout={lock_timeout_ms}")
    if statement_timeout_ms:
        parts.append(f"-c statement_timeout={statement_timeout_ms}")
    if idle_in_transaction_session_timeout_ms:
        parts.append(
            "-c idle_in_transaction_session_timeout="
            f"{idle_in_transaction_session_timeout_ms}"
        )
    return " ".join(parts)


def make_engine(
    config: Optional[Config] = None,
    echo: bool = False,
    application_name: Optional[str] = None,
    statement_timeout_ms: Optional[int] = None,
) -> Engine:
    if config is None:
        config = get_config()

    # Bound lock waits / statement runtime / idle transactions so a single
    # blocked query cannot freeze the synchronous worker event loop. Migrations
    # use their own engine (deployment/migrations/env.py) and are unaffected.
    #
    # statement_timeout_ms can be overridden per engine (e.g. the API passes 0
    # to disable it, because some aggregate reads legitimately run longer than
    # the worker default); None means use the config value.
    if statement_timeout_ms is None:
        statement_timeout_ms = config.postgres.statement_timeout_ms.value
    options = _timeout_options(
        config.postgres.lock_timeout_ms.value,
        statement_timeout_ms,
        config.postgres.idle_in_transaction_session_timeout_ms.value,
    )
    connect_args = {"options": options} if options else {}

    engine = create_engine(
        make_db_url(
            driver="psycopg2", config=config, application_name=application_name
        ),
        echo=echo,
        pool_size=config.postgres.pool_size.value,
        pool_pre_ping=config.postgres.pool_pre_ping.value,
        pool_recycle=config.postgres.pool_recycle.value,
        connect_args=connect_args,
    )
    install_password_file_hook(engine, config)
    return engine


def make_async_engine(
    config: Optional[Config] = None,
    echo: bool = False,
    application_name: Optional[str] = None,
) -> AsyncEngine:
    if config is None:
        config = get_config()

    # asyncpg needs these as typed keyword arguments, which a URL query string
    # cannot express (see _connection_query_params).
    connect_args: Dict[str, Any] = {}
    if config.postgres.connect_timeout_s.value:
        connect_args["timeout"] = config.postgres.connect_timeout_s.value
    if application_name:
        connect_args["server_settings"] = {"application_name": application_name}
    # asyncpg calls a callable password for each new connection, so a rotated
    # password file is picked up without a restart. No last-known-good retry
    # here: the do_connect hook wraps a synchronous connect, which asyncpg is
    # not. No node process builds this engine today (the worker loops, the
    # API and migrations all use psycopg2).
    source = make_password_source(config)
    if source is not None:
        connect_args["password"] = source.current

    return create_async_engine(
        make_db_url(driver="asyncpg", config=config),
        future=True,
        echo=echo,
        connect_args=connect_args,
    )


def make_session_factory(engine: Engine) -> DbSessionFactory:
    return sessionmaker(engine, expire_on_commit=False)
