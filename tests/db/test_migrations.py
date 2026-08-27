"""Migrations must not depend on the database being named `aleph`.

Requires the test database role to be a superuser: the scratch database is
created and dropped with CREATE DATABASE / DROP DATABASE and any leftover
backend on it is closed with pg_terminate_backend. CI runs postgres with
`POSTGRES_USER: aleph`, which is the superuser of that instance.
"""

import argparse
import contextlib
import logging
import os
from pathlib import Path

import alembic.command
import alembic.config
import pytest
from db_seeds import EXPECTED_ERROR_CODE_ROWS
from sqlalchemy import create_engine, text

import aleph.config
from aleph.db.connection import make_db_url

SCRATCH_DB = "aleph_migration_probe"


@contextlib.contextmanager
def change_dir(directory: Path):
    """Run the block with `directory` as the working directory.

    Copied from `tests/conftest.py` rather than imported: conftest is not an
    importable module for tests.
    """
    current_directory = Path.cwd()
    try:
        os.chdir(directory)
        yield
    finally:
        os.chdir(current_directory)


def _terminate_other_sessions(conn, dbname: str) -> None:
    """Terminate any other backends connected to `dbname`.

    `deployment/migrations/env.py` never disposes the engine its
    `run_migrations_online` creates, and SQLAlchemy's pool keeps the physical
    connection open after the `connect()` block exits. The pooled connection is
    only released when the engine is garbage collected, and the `QueuePool` /
    `_ConnectionRecord` reference cycle defers that to a cyclic GC pass. So the
    scratch database may still have a live backend when we DROP it, which fails
    with "database is being accessed by other users" unless we close it here.
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

    # alembic.ini's `script_location` is relative to the working directory.
    with change_dir(project_dir):
        alembic.command.upgrade(alembic_cfg, "head")

    engine = create_engine(scratch_db_url)
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        assert version is not None
        assert (
            conn.execute(text("SELECT count(*) FROM error_codes")).scalar()
            == EXPECTED_ERROR_CODE_ROWS
        )
    engine.dispose()
