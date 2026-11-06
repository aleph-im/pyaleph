"""PostgreSQL password read from a file at connect time, against a real server.

A dedicated role (`rot_test`) has its password changed with ALTER ROLE while
the engine is alive, to reproduce both orders of a rotation: the Secret (file)
updated before the role, and after it.

Requires the test database role to be a superuser (CREATE ROLE, CREATE
DATABASE), as tests/db/test_migrations.py does.
"""

import argparse
import contextlib
import logging
import os
from pathlib import Path

import alembic.command
import alembic.config
import pytest
from configmanager import Config
from sqlalchemy import create_engine, text

import aleph.config
from aleph.db.connection import make_async_engine, make_db_url, make_engine
from aleph.toolkit.credentials import LOGGER as CREDENTIALS_LOGGER

ROLE = "rot_test"
PASSWORD_A = "rot-pass-A"
PASSWORD_B = "rot-pass-B"
SCRATCH_DB = "aleph_password_file_probe"


def _admin_engine():
    admin_url = make_db_url(driver="psycopg2", config=aleph.config.app_config)
    return create_engine(admin_url, isolation_level="AUTOCOMMIT")


def _set_role_password(admin, password: str) -> None:
    with admin.connect() as conn:
        conn.execute(text(f"ALTER ROLE {ROLE} PASSWORD '{password}'"))


@pytest.fixture
def admin(mock_config):
    engine = _admin_engine()
    yield engine
    engine.dispose()


@pytest.fixture
def rot_role(admin):
    """A login role with password A, dropped (with its sessions) afterwards."""
    with admin.connect() as conn:
        conn.execute(text(f"DROP ROLE IF EXISTS {ROLE}"))
        conn.execute(text(f"CREATE ROLE {ROLE} LOGIN PASSWORD '{PASSWORD_A}'"))
    yield ROLE
    with admin.connect() as conn:
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE usename = :role"
            ),
            {"role": ROLE},
        )
        conn.execute(text(f"DROP ROLE IF EXISTS {ROLE}"))


@pytest.fixture
def password_file(tmp_path) -> Path:
    path = tmp_path / "postgres-password"
    path.write_text(PASSWORD_A + "\n")
    return path


def _role_config(password_file: Path) -> Config:
    config = Config(aleph.config.get_defaults())
    config.postgres.host.value = aleph.config.app_config.postgres.host.value
    config.postgres.port.value = aleph.config.app_config.postgres.port.value
    config.postgres.database.value = aleph.config.app_config.postgres.database.value
    config.postgres.user.value = ROLE
    # Never used: the file wins.
    config.postgres.password.value = "inline-is-ignored"
    config.postgres.password_file.value = str(password_file)
    return config


def _current_user(engine) -> str:
    with engine.connect() as conn:
        return conn.execute(text("SELECT current_user")).scalar_one()


def _rotation_warnings(caplog):
    return [
        r
        for r in caplog.records
        if r.name == CREDENTIALS_LOGGER.name and r.levelno == logging.WARNING
    ]


def test_engine_reads_password_file_on_each_connect(admin, rot_role, password_file):
    engine = make_engine(config=_role_config(password_file))
    try:
        assert _current_user(engine) == ROLE

        _set_role_password(admin, PASSWORD_B)
        password_file.write_text(PASSWORD_B + "\n")
        engine.dispose()

        # Same engine, new connection: the new file content is used.
        assert _current_user(engine) == ROLE
    finally:
        engine.dispose()


def test_retry_with_last_good_when_file_is_stale(
    admin, rot_role, password_file, caplog
):
    """Secret updated first: the file holds B while the role still has A."""
    engine = make_engine(config=_role_config(password_file))
    try:
        assert _current_user(engine) == ROLE  # A works, remembered
        engine.dispose()

        password_file.write_text(PASSWORD_B + "\n")
        with caplog.at_level(logging.WARNING, logger=CREDENTIALS_LOGGER.name):
            assert _current_user(engine) == ROLE

        warnings = _rotation_warnings(caplog)
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert "last known good" in message
        assert PASSWORD_A not in message and PASSWORD_B not in message
    finally:
        engine.dispose()


def test_retry_with_file_when_last_good_is_stale(
    admin, rot_role, password_file, caplog
):
    """The role then moves to B: the stale last good A no longer matters."""
    engine = make_engine(config=_role_config(password_file))
    try:
        assert _current_user(engine) == ROLE  # last good = A
        engine.dispose()
        password_file.write_text(PASSWORD_B + "\n")
        assert _current_user(engine) == ROLE  # file B rejected, A retried
        engine.dispose()

        _set_role_password(admin, PASSWORD_B)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=CREDENTIALS_LOGGER.name):
            assert _current_user(engine) == ROLE  # file B accepted
            engine.dispose()
            # B is now the last good: rolling the file back to A (wrong) is
            # still covered by the retry, with B.
            password_file.write_text(PASSWORD_A + "\n")
            assert _current_user(engine) == ROLE

        warnings = _rotation_warnings(caplog)
        assert len(warnings) == 1
    finally:
        engine.dispose()


def test_missing_password_file_fails_at_engine_creation(tmp_path):
    from aleph.toolkit.credentials import CredentialFileError

    missing = tmp_path / "absent"
    with pytest.raises(CredentialFileError, match=str(missing)):
        make_engine(config=_role_config(missing))


@pytest.mark.asyncio
async def test_async_engine_reads_password_file_on_each_connect(
    admin, rot_role, password_file
):
    engine = make_async_engine(config=_role_config(password_file))
    try:
        async with engine.connect() as conn:
            user = (await conn.execute(text("SELECT current_user"))).scalar_one()
        assert user == ROLE

        _set_role_password(admin, PASSWORD_B)
        password_file.write_text(PASSWORD_B + "\n")
        await engine.dispose()

        async with engine.connect() as conn:
            user = (await conn.execute(text("SELECT current_user"))).scalar_one()
        assert user == ROLE
    finally:
        await engine.dispose()


@contextlib.contextmanager
def _change_dir(directory: Path):
    current_directory = Path.cwd()
    try:
        os.chdir(directory)
        yield
    finally:
        os.chdir(current_directory)


def _terminate_sessions(conn, dbname: str) -> None:
    conn.execute(
        text(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = :dbname AND pid <> pg_backend_pid()"
        ),
        {"dbname": dbname},
    )


@pytest.fixture
def scratch_db(admin):
    with admin.connect() as conn:
        _terminate_sessions(conn, SCRATCH_DB)
        conn.execute(text(f"DROP DATABASE IF EXISTS {SCRATCH_DB}"))
        conn.execute(text(f"CREATE DATABASE {SCRATCH_DB}"))
    yield SCRATCH_DB
    with admin.connect() as conn:
        _terminate_sessions(conn, SCRATCH_DB)
        conn.execute(text(f"DROP DATABASE IF EXISTS {SCRATCH_DB}"))


def test_migrations_use_password_file(mock_config, scratch_db, tmp_path):
    """env.py, run from the node config, connects with postgres.password_file."""
    postgres = aleph.config.app_config.postgres
    real_password = postgres.password.value
    real_database = postgres.database.value
    password_file = tmp_path / "postgres-password"
    password_file.write_text(real_password + "\n")

    postgres.database.value = scratch_db
    postgres.password.value = "inline-is-wrong"
    postgres.password_file.value = str(password_file)

    project_dir = Path(__file__).parent.parent.parent
    alembic_cfg = alembic.config.Config(str(project_dir / "alembic.ini"))
    alembic_cfg.attributes["configure_logger"] = False
    # No db_url: env.py builds the engine from the node config. Point it at a
    # config file that does not exist so a local config.yml cannot interfere.
    alembic_cfg.cmd_opts = argparse.Namespace(
        x=[f"config_file={tmp_path / 'no-config.yml'}"]
    )
    logging.getLogger("alembic").setLevel(logging.CRITICAL)

    try:
        with _change_dir(project_dir):
            alembic.command.upgrade(alembic_cfg, "head")

        engine = make_engine(config=aleph.config.app_config)
        try:
            with engine.connect() as conn:
                version = conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar()
            assert version is not None
        finally:
            engine.dispose()
    finally:
        postgres.password_file.value = None
        postgres.password.value = real_password
        postgres.database.value = real_database
