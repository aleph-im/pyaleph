"""Migrations must not depend on the database being named `aleph`."""

import argparse
import logging
from pathlib import Path

import alembic.command
import alembic.config
import pytest
from sqlalchemy import create_engine, text

import aleph.config
from aleph.db.connection import make_db_url

SCRATCH_DB = "aleph_migration_probe"


def _terminate_other_sessions(conn, dbname: str) -> None:
    """Terminate any other backends connected to `dbname`.

    Alembic's `env.py` is exec'd as a module and keeps a reference cycle
    (its globals reference functions whose __globals__ point back to the
    module dict), so the engine it creates is not reclaimed by CPython's
    refcounting alone; it lingers until a cyclic GC pass runs. Without
    terminating it explicitly, DROP DATABASE can fail with
    "database is being accessed by other users".
    """
    conn.execute(
        text(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = :dbname AND pid <> pg_backend_pid()"
        ),
        {"dbname": dbname},
    )


@pytest.fixture
def scratch_db_url(mock_config):
    """Create an empty database with a non-default name and return its URL."""
    admin_url = make_db_url(driver="psycopg2", config=aleph.config.app_config)
    admin_engine = create_engine(
        admin_url.rsplit("/", 1)[0] + "/postgres", isolation_level="AUTOCOMMIT"
    )
    with admin_engine.connect() as conn:
        _terminate_other_sessions(conn, SCRATCH_DB)
        conn.execute(text(f"DROP DATABASE IF EXISTS {SCRATCH_DB}"))
        conn.execute(text(f"CREATE DATABASE {SCRATCH_DB}"))
    yield admin_url.rsplit("/", 1)[0] + f"/{SCRATCH_DB}"
    with admin_engine.connect() as conn:
        _terminate_other_sessions(conn, SCRATCH_DB)
        conn.execute(text(f"DROP DATABASE IF EXISTS {SCRATCH_DB}"))
    admin_engine.dispose()


def test_migrations_apply_to_any_database_name(scratch_db_url):
    project_dir = Path(__file__).parent.parent.parent
    alembic_cfg = alembic.config.Config(str(project_dir / "alembic.ini"))
    alembic_cfg.attributes["configure_logger"] = False
    alembic_cfg.cmd_opts = argparse.Namespace(x=[f"db_url={scratch_db_url}"])
    logging.getLogger("alembic").setLevel(logging.CRITICAL)

    alembic.command.upgrade(alembic_cfg, "head")

    engine = create_engine(scratch_db_url)
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        assert version is not None
        assert conn.execute(text("SELECT count(*) FROM error_codes")).scalar() == 25
    engine.dispose()
