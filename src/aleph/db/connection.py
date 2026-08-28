from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from configmanager import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker

from aleph.config import get_config
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


def make_db_url(
    driver: str, config: Config, application_name: Optional[str] = None
) -> str:
    """
    Returns the database connection string from configuration values.

    :param driver: Driver name. Ex: psycopg2, asyncpg.
    :param config: Configuration. If not specified, the global configuration object is used.
    :param application_name: Application name.
    :returns: The database connection string.
    """

    host = config.postgres.host.value
    port = config.postgres.port.value
    user = config.postgres.user.value
    password = config.postgres.password.value
    database = config.postgres.database.value

    connection_string = f"postgresql+{driver}://{user}:"

    if password is not None:
        connection_string += f"{password}"

    connection_string += "@"

    if host is not None:
        connection_string += f"{host}:{port}"

    connection_string += f"/{database}"

    if application_name:
        connection_string += f"?application_name={application_name}"

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

    return create_engine(
        make_db_url(
            driver="psycopg2", config=config, application_name=application_name
        ),
        echo=echo,
        pool_size=config.postgres.pool_size.value,
        pool_pre_ping=config.postgres.pool_pre_ping.value,
        pool_recycle=config.postgres.pool_recycle.value,
        connect_args=connect_args,
    )


def make_async_engine(
    config: Optional[Config] = None,
    echo: bool = False,
    application_name: Optional[str] = None,
) -> AsyncEngine:
    return create_async_engine(
        make_db_url(driver="asyncpg", config=config, application_name=application_name),
        future=True,
        echo=echo,
    )


def make_session_factory(engine: Engine) -> DbSessionFactory:
    return sessionmaker(engine, expire_on_commit=False)
