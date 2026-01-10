from typing import Optional

from configmanager import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker

from aleph.config import get_config
from aleph.types.db_session import DbSessionFactory


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


def make_engine(
    config: Optional[Config] = None,
    echo: bool = False,
    application_name: Optional[str] = None,
) -> Engine:
    if config is None:
        config = get_config()

    return create_engine(
        make_db_url(
            driver="psycopg2", config=config, application_name=application_name
        ),
        echo=echo,
        pool_size=config.postgres.pool_size.value,
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
