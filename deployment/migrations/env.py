from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
from aleph.config import get_config
from aleph.db.connection import make_db_url

config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.attributes.get('configure_logger', True):
    fileConfig(config.config_file_name)

# Auto-generate migrations
from aleph.db.models import Base

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_db_url():
    cli_args = context.get_x_argument(as_dictionary=True)
    db_url = cli_args.get("db_url")

    if db_url:
        return db_url

    config = get_config()
    default_config_file = Path.cwd() / "config.yml"
    config_file = cli_args.get("config_file") or default_config_file
    with config_file.open() as f:
        user_config = f.read()

    # Little trick to allow empty config files
    if user_config:
        config.yaml.loads(user_config)

    return make_db_url(driver="psycopg2", config=config)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """

    db_url = get_db_url()
    context.configure(
        url=db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    db_url = get_db_url()
    connectable = create_engine(db_url, echo=False)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
