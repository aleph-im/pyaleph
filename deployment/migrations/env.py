from logging.config import fileConfig
from pathlib import Path

from alembic import context
# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
from aleph.config import get_config
from aleph.db.connection import make_db_url
from sqlalchemy import create_engine

config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.attributes.get('configure_logger', True):
    fileConfig(config.config_file_name)

# Auto-generate migrations
from aleph.db.models import Base  # noqa

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_db_url() -> str:
    cli_args = context.get_x_argument(as_dictionary=True)
    db_url = cli_args.get("db_url")

    if db_url:
        return db_url

    config = get_config()

    config_file_path: Path

    # intermediat variable to please mypy
    cli_args_config_file = cli_args.get("config_file")
    if cli_args_config_file:
        config_file_path = Path(cli_args_config_file)
    else:
        config_file_path = Path.cwd() / "config.yml"

    if config_file_path.exists():
        user_config_raw: str = config_file_path.read_text()

        # Little trick to allow empty config files
        if user_config_raw:
            config.yaml.loads(user_config_raw)

    return make_db_url(driver="psycopg2", config=config)


def include_object(obj, name, type_, reflected, compare_to):
    """Exclude views from Alembic auto-generation."""
    if type_ == "table" and obj.info.get("is_view"):
        return False
    return True


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
        include_object=include_object,
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
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
