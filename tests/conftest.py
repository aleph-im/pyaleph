import argparse
import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import List, Protocol

import alembic.command
import alembic.config
import pytest
import pytest_asyncio
import pytz
from aleph_message.models import (
    Chain,
    ExecutableContent,
    InstanceContent,
    ItemType,
    MessageType,
    ProgramContent,
)
from aleph_message.models.execution.volume import ImmutableVolume
from configmanager import Config
from sqlalchemy import text
from sqlalchemy.engine import Engine

import aleph.config
from aleph.db.accessors.files import insert_message_file_pin, upsert_file_tag
from aleph.db.connection import make_db_url, make_engine, make_session_factory
from aleph.db.models import (
    AlephBalanceDb,
    MessageStatusDb,
    PendingMessageDb,
    StoredFileDb,
)
from aleph.db.models.aggregates import AggregateDb, AggregateElementDb
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.constants import (
    DEFAULT_PRICE_AGGREGATE,
    DEFAULT_SETTINGS_AGGREGATE,
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    SETTINGS_AGGREGATE_KEY,
    SETTINGS_AGGREGATE_OWNER,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import MessageStatus
from aleph.web import create_aiohttp_app
from aleph.web.controllers.app_state_getters import (
    APP_STATE_CONFIG,
    APP_STATE_NODE_CACHE,
    APP_STATE_P2P_CLIENT,
    APP_STATE_SESSION_FACTORY,
    APP_STATE_STORAGE_SERVICE,
)

# Add the helpers to the PYTHONPATH.
# Note: mark the "helpers" directory as a source directory to tell PyCharm
# about this trick and avoid IDE errors.
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@contextlib.contextmanager
def change_dir(directory: Path):
    current_directory = Path.cwd()
    try:
        os.chdir(directory)
        yield
    finally:
        os.chdir(current_directory)


def run_db_migrations(config: Config):
    logging.basicConfig(level=logging.DEBUG)

    project_dir = Path(__file__).parent.parent

    db_url = make_db_url(driver="psycopg2", config=config)
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic_cfg.attributes["configure_logger"] = False
    # env.py reads the target URL from `-x db_url=...`; alembic's `tag`
    # argument is ignored there, so pass the URL the way env.py expects.
    alembic_cfg.cmd_opts = argparse.Namespace(x=[f"db_url={db_url}"])
    logging.getLogger("alembic").setLevel(logging.CRITICAL)

    with change_dir(project_dir):
        alembic.command.upgrade(alembic_cfg, "head")


@dataclass
class MigratedDb:
    """A freshly migrated schema plus a snapshot of its seeded rows.

    `tables` lists every table in `public`; `seed_tables` the subset that
    migrations populate (copied to `seed.<table>` so per-test resets can
    restore them without re-running migrations).
    """

    engine: Engine
    tables: List[str]
    seed_tables: List[str]


SEED_SCHEMA = "seed"


def _public_tables(conn) -> List[str]:
    rows = conn.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
    )
    return [row[0] for row in rows]


def snapshot_seed_tables(engine: Engine) -> tuple[List[str], List[str]]:
    """Copy every non-empty public table into the `seed` schema.

    Returns (all public tables, seeded tables). Discovery is by row count so
    a new migration that seeds another table is picked up automatically.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SEED_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {SEED_SCHEMA}"))
        tables = _public_tables(conn)
        seed_tables = []
        for table in tables:
            count = conn.execute(
                text(f'SELECT count(*) FROM public."{table}"')
            ).scalar()
            if count:
                conn.execute(
                    text(
                        f'CREATE TABLE {SEED_SCHEMA}."{table}" AS TABLE public."{table}"'
                    )
                )
                seed_tables.append(table)
    return tables, seed_tables


def rebuild_schema(engine: Engine, config: Config) -> tuple[List[str], List[str]]:
    """Drop and re-migrate the public schema, then snapshot the seeds."""
    with engine.begin() as conn:
        conn.execute(text("drop schema public cascade"))
        conn.execute(text("create schema public"))
    run_db_migrations(config=config)
    return snapshot_seed_tables(engine)


USER_TRIGGER_TABLES = ("messages", "message_confirmations")


def _non_empty_tables(conn, tables: List[str]) -> List[str]:
    """Return the subset of `tables` that currently holds at least one row.

    A single statement with one EXISTS probe per table: a few milliseconds for
    this schema's ~70 tables, which keeps the reset proportional to what the
    test actually wrote instead of to the size of the schema.
    """
    if not tables:
        return []
    probes = " UNION ALL ".join(
        f"SELECT '{table}' AS table_name WHERE EXISTS (SELECT 1 FROM public.\"{table}\")"
        for table in tables
    )
    return list(conn.execute(text(probes)).scalars().all())


def reset_database(migrated_db: MigratedDb) -> bool:
    """Return the schema to its freshly migrated state without migrating.

    Empties every public table except alembic_version, restores the seeded rows
    from the `seed` schema, rewinds any sequence a test advanced, and re-enables
    user triggers a previous test may have left disabled.

    DELETE rather than TRUNCATE: TRUNCATE gives each table a new relfilenode and
    the commit fsyncs all of them, which costs ~4.5 s for this schema's 68 tables
    (~112 ms even for a single table). Deleting only the handful of tables a test
    touched takes ~10 ms. `session_replication_role = replica` suppresses foreign
    key triggers so the tables can be emptied in any order, which is what CASCADE
    bought us before.

    Returns False if the schema itself drifted beyond what deleting rows can
    repair, i.e. a test dropped a table (the metrics partition cron job does):
    the caller then has to fall back to a full rebuild. Tables a test *created*
    are dropped here, so that case does not need a rebuild.
    """
    with migrated_db.engine.begin() as conn:
        conn.execute(text("SET LOCAL session_replication_role = replica"))
        # The set of tables is re-read every time rather than taken from the
        # session-scoped snapshot: DDL in a test (partition create/drop) would
        # otherwise leave the snapshot stale and the probes below would fail.
        current = set(_public_tables(conn))
        expected = set(migrated_db.tables)
        for table in sorted(current - expected):
            conn.execute(text(f'DROP TABLE IF EXISTS public."{table}" CASCADE'))
        if expected - current:
            return False
        targets = sorted(expected - {"alembic_version"})
        for table in _non_empty_tables(conn, targets):
            conn.execute(text(f'DELETE FROM public."{table}"'))
        for table in migrated_db.seed_tables:
            if table == "alembic_version":
                continue
            conn.execute(
                text(
                    f'INSERT INTO public."{table}" SELECT * FROM {SEED_SCHEMA}."{table}"'
                )
            )
        # A test can advance a sequence without leaving a row behind, so rewind
        # every sequence that has been used. No migration seeds a serial column
        # (the seeded tables carry explicit keys), so "unused" is the freshly
        # migrated state for all of them.
        dirty_sequences = (
            conn.execute(
                text(
                    "SELECT sequencename FROM pg_sequences "
                    "WHERE schemaname = 'public' AND last_value IS NOT NULL"
                )
            )
            .scalars()
            .all()
        )
        for sequence in dirty_sequences:
            conn.execute(text(f'ALTER SEQUENCE public."{sequence}" RESTART'))
        for table in USER_TRIGGER_TABLES:
            conn.execute(text(f'ALTER TABLE public."{table}" ENABLE TRIGGER ALL'))
    return True


@pytest.fixture(scope="session")
def migrated_db():
    # Session-scoped, so it cannot depend on the function-scoped mock_config:
    # build the same test config it would install.
    config = _create_test_config()
    # Tests must not inherit the production statement/lock/idle timeouts: they
    # can cause spurious failures during schema setup/teardown under load.
    config.postgres.lock_timeout_ms.value = 0
    config.postgres.statement_timeout_ms.value = 0
    config.postgres.idle_in_transaction_session_timeout_ms.value = 0
    engine = make_engine(config=config, echo=False, application_name="aleph-tests")

    tables, seed_tables = rebuild_schema(engine, config)

    # Running migrations pollutes aleph.config.app_config by loading config.yml.
    # Replace the global with a completely fresh test config object.
    aleph.config.app_config = _create_test_config()

    yield MigratedDb(engine=engine, tables=tables, seed_tables=seed_tables)
    engine.dispose()


@pytest.fixture
def session_factory(request, mock_config, migrated_db: MigratedDb):
    """A session factory on a database in its freshly migrated state.

    Fast path: delete the rows a test wrote and restore the seeds (tens of
    milliseconds). Tests marked `fresh_schema` get the slow path: drop the
    schema and re-run migrations. A test that drops a table forces the slow
    path for the test after it, since only migrations can bring the table back.
    """
    if request.node.get_closest_marker("fresh_schema") or not reset_database(
        migrated_db
    ):
        config = aleph.config.app_config
        config.postgres.lock_timeout_ms.value = 0
        config.postgres.statement_timeout_ms.value = 0
        config.postgres.idle_in_transaction_session_timeout_ms.value = 0
        migrated_db.tables, migrated_db.seed_tables = rebuild_schema(
            migrated_db.engine, config
        )
        # Running migrations pollutes aleph.config.app_config by loading
        # config.yml; restore the fresh test config as before.
        aleph.config.app_config = _create_test_config()
    return make_session_factory(migrated_db.engine)


def _create_test_config() -> Config:
    """Create a fresh config with test-specific values."""
    config: Config = Config(aleph.config.get_defaults())

    # The anvil/postgres/redis hosts use Docker network names in the default config.
    # We always use localhost for tests.
    config.postgres.host.value = "127.0.0.1"
    config.redis.host.value = "127.0.0.1"
    config.ethereum.api_url.value = "http://127.0.0.1:8545"
    config.ethereum.chain_id.value = 31337

    # To test handle_new_storage
    config.storage.store_files.value = True

    # Disable IPFS fetch jitter in tests so fetch_related_content does not sleep.
    config.ipfs.fetch_jitter_seconds.value = 0

    return config


class _ConfigProxy:
    """
    A proxy that always delegates to aleph.config.app_config.

    Running migrations for tests updates the global config values by loading config.yml.
    The session factory fixture does replace the global object with a fresh config for tests afterward,
    but if mock_config() returns a Config object directly it will be the one that has been modified.
    To avoid this, we use a proxy pattern to always return the current global config in mock_config().
    """

    def __getattr__(self, name):
        return getattr(aleph.config.app_config, name)

    def __setattr__(self, name, value):
        setattr(aleph.config.app_config, name, value)


# Singleton proxy instance
_config_proxy = _ConfigProxy()


@pytest.fixture
def mock_config() -> Config:
    """
    Returns a proxy to the current global app_config.

    This ensures all tests see the same config, and any updates
    to aleph.config.app_config (e.g., after migrations) are reflected.
    """
    # Ensure we start with a clean test config
    config = _create_test_config()
    aleph.config.app_config = config
    return _config_proxy


@pytest_asyncio.fixture
async def node_cache(mock_config: Config):
    async with NodeCache(
        redis_host=mock_config.redis.host.value,
        redis_port=mock_config.redis.port.value,
        message_count_cache_ttl=mock_config.perf.message_count_cache_ttl.value,
    ) as node_cache:
        yield node_cache


@pytest_asyncio.fixture
async def test_storage_service(mock_config: Config, mocker) -> StorageService:
    data_folder = Path("./data")

    # Delete files from previous runs
    if data_folder.is_dir():
        shutil.rmtree(data_folder)
    data_folder.mkdir(parents=True)

    storage_engine = FileSystemStorageEngine(folder=data_folder)
    async with IpfsService.new(mock_config) as ipfs_service:
        storage_service = StorageService(
            storage_engine=storage_engine,
            ipfs_service=ipfs_service,
            node_cache=mocker.AsyncMock(),
        )

        yield storage_service


@pytest.fixture
def ccn_test_aiohttp_app(mocker, mock_config, session_factory, node_cache: NodeCache):
    # Make aiohttp return the stack trace on 500 errors
    event_loop = asyncio.get_event_loop()
    event_loop.set_debug(True)

    app = create_aiohttp_app(with_swagger=False)
    app[APP_STATE_CONFIG] = mock_config
    app[APP_STATE_NODE_CACHE] = node_cache
    app[APP_STATE_P2P_CLIENT] = mocker.AsyncMock()
    app[APP_STATE_STORAGE_SERVICE] = mocker.AsyncMock()
    app[APP_STATE_SESSION_FACTORY] = session_factory

    return app


@pytest_asyncio.fixture
async def ccn_api_client(
    aiohttp_client,
    ccn_test_aiohttp_app,
):
    client = await aiohttp_client(ccn_test_aiohttp_app)
    return client


@pytest.fixture
def fixture_instance_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    content = {
        "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        "allow_amend": False,
        "variables": {
            "VM_CUSTOM_VARIABLE": "SOMETHING",
            "VM_CUSTOM_VARIABLE_2": "32",
        },
        "environment": {
            "reproducible": True,
            "internet": False,
            "aleph_api": False,
            "shared_cache": False,
        },
        "resources": {"vcpus": 1, "memory": 128, "seconds": 30},
        "requirements": {"cpu": {"architecture": "x86_64"}},
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": True,
            },
            "persistence": "host",
            "name": "test-rootfs",
            "size_mib": 20 * 1024,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGULT6A41Msmw2KEu0R9MvUjhuWNAsbdeZ0DOwYbt4Qt user@example",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIH0jqdc5dmt75QhTrWqeHDV9xN8vxbgFyOYs2fuQl7CI",
        ],
        "volumes": [
            {
                "comment": "Python libraries. Read-only since a 'ref' is specified.",
                "mount": "/opt/venv",
                "ref": "5f31b0706f59404fad3d0bff97ef89ddf24da4761608ea0646329362c662ba51",
                "use_latest": False,
            },
            {
                "comment": "Ephemeral storage, read-write but will not persist after the VM stops",
                "mount": "/var/cache",
                "ephemeral": True,
                "size_mib": 5,
            },
            {
                "comment": "Working data persisted on the VM supervisor, not available on other nodes",
                "mount": "/var/lib/sqlite",
                "name": "sqlite-data",
                "persistence": "host",
                "size_mib": 10,
            },
            {
                "comment": "Working data persisted on the Aleph network. "
                "New VMs will try to use the latest version of this volume, "
                "with no guarantee against conflicts",
                "mount": "/var/lib/statistics",
                "name": "statistics",
                "persistence": "store",
                "size_mib": 10,
            },
            {
                "comment": "Raw drive to use by a process, do not mount it",
                "name": "raw-data",
                "persistence": "host",
                "size_mib": 10,
            },
        ],
        "time": 1619017773.8950517,
    }

    pending_message = PendingMessageDb(
        item_hash="734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        type=MessageType.instance.value,
        chain=Chain.ETH,
        sender="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(content),
        time=timestamp_to_datetime(1619017773.8950577),
        channel=None,
        reception_time=timestamp_to_datetime(1619017774),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()

    return pending_message


@pytest.fixture
def instance_message_with_volumes_in_db(
    session_factory: DbSessionFactory, fixture_instance_message: PendingMessageDb
) -> None:
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()


class Volume(Protocol):
    ref: str
    use_latest: bool


def get_volume_refs(content: ExecutableContent) -> List[Volume]:
    volumes = []

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            volumes.append(volume)

    if isinstance(content, ProgramContent):
        volumes += [content.code, content.runtime]
        if content.data:
            volumes.append(content.data)

    elif isinstance(content, InstanceContent):
        if parent := content.rootfs.parent:
            volumes.append(parent)

    return volumes


def insert_volume_refs(session: DbSession, message: PendingMessageDb):
    """
    Insert volume references in the DB to make the program processable.
    """

    content = InstanceContent.model_validate_json(message.item_content)
    volumes = get_volume_refs(content)

    created = pytz.utc.localize(dt.datetime(2023, 1, 1))

    for volume in volumes:
        # Note: we use the reversed ref to generate the file hash for style points,
        # but it could be set to any valid hash.
        file_hash = volume.ref[::-1]

        session.add(StoredFileDb(hash=file_hash, size=1024 * 1024, type=FileType.FILE))
        session.flush()
        insert_message_file_pin(
            session=session,
            file_hash=volume.ref[::-1],
            owner=content.address,
            item_hash=volume.ref,
            ref=None,
            created=created,
        )
        upsert_file_tag(
            session=session,
            tag=FileTag(volume.ref),
            owner=content.address,
            file_hash=volume.ref[::-1],
            last_updated=created,
        )


@pytest.fixture
def user_balance(session_factory: DbSessionFactory) -> AlephBalanceDb:
    balance = AlephBalanceDb(
        address="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        chain=Chain.ETH,
        balance=Decimal(22_192),
        eth_height=0,
    )

    with session_factory() as session:
        session.add(balance)
        session.commit()
    return balance


@pytest.fixture
def user_balance_eth_avax(session_factory: DbSessionFactory) -> AlephBalanceDb:
    balance_eth = AlephBalanceDb(
        address="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        chain=Chain.ETH,
        balance=Decimal(22_192),
        eth_height=0,
    )

    balance_avax = AlephBalanceDb(
        address="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        chain=Chain.AVAX,
        balance=Decimal(22_192),
        eth_height=0,
    )

    with session_factory() as session:
        session.add(balance_eth)
        session.add(balance_avax)

        session.commit()
    return balance_avax


@pytest.fixture
def fixture_product_prices_aggregate_in_db(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        item_hash = "7b74b9c5f73e7a0713dbe83a377b1d321ffb4a5411ea3df49790a9720b93a5bF"
        content = DEFAULT_PRICE_AGGREGATE
        session.add(
            AggregateElementDb(
                item_hash=item_hash,
                key=PRICE_AGGREGATE_KEY,
                owner=PRICE_AGGREGATE_OWNER,
                content=content,
                creation_datetime=dt.datetime(2025, 1, 31),
            )
        )

        session.add(
            AggregateDb(
                key=PRICE_AGGREGATE_KEY,
                owner=PRICE_AGGREGATE_OWNER,
                content=content,
                creation_datetime=dt.datetime(2025, 1, 31),
                last_revision_hash=item_hash,
                dirty=False,
            )
        )

        session.commit()


@pytest.fixture
def fixture_settings_aggregate_in_db(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        item_hash = "a319a7216d39032212c2f11028a21efaac4e5f78254baa34001483c7af22b7a4"
        content = DEFAULT_SETTINGS_AGGREGATE

        session.add(
            AggregateElementDb(
                item_hash=item_hash,
                key=SETTINGS_AGGREGATE_KEY,
                owner=SETTINGS_AGGREGATE_OWNER,
                content=content,
                creation_datetime=dt.datetime(2025, 1, 31),
            )
        )

        session.add(
            AggregateDb(
                key=SETTINGS_AGGREGATE_KEY,
                owner=SETTINGS_AGGREGATE_OWNER,
                content=content,
                creation_datetime=dt.datetime(2025, 1, 31),
                last_revision_hash=item_hash,
                dirty=False,
            )
        )

        session.commit()
