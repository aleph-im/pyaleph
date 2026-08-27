"""The DB fixtures: one migration per session, delete + seed restore per test.

The `test_reset_step_N` tests are a deliberate ordered sequence: each one sets
up the state the next one asserts on. pytest collects tests in definition order
and the project uses neither pytest-randomly nor pytest-xdist, so the order
holds for a normal run. Running a subset (`-k`, `--lf`) skips the steps whose
predecessor did not run rather than failing.
"""

import datetime as dt

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from db_seeds import EXPECTED_CRON_JOB_ROWS, EXPECTED_ERROR_CODE_ROWS
from sqlalchemy import select, text

from aleph.db.models import ErrorCodeDb, PendingMessageDb, StoredFileDb
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType

SEED_COUNTS = {
    "alembic_version": 1,
    "cron_jobs": EXPECTED_CRON_JOB_ROWS,
    "error_codes": EXPECTED_ERROR_CODE_ROWS,
}

# Filled in by the step tests below so that a later step can tell whether the
# schema was rebuilt between the two: re-running the migrations recreates every
# table with a fresh OID, while the fast reset leaves the tables in place.
OBSERVED_OIDS: dict[str, int] = {}


def _messages_oid(session) -> int:
    return session.execute(text("SELECT 'public.messages'::regclass::oid")).scalar()


def _observed_oid(key: str, step: str) -> int:
    """Read an OID recorded by an earlier step, or skip if that step did not run."""
    if key not in OBSERVED_OIDS:
        pytest.skip(f"runs only in sequence after {step}")
    return OBSERVED_OIDS[key]


def test_migrated_db_snapshots_seed_tables(migrated_db):
    assert set(migrated_db.seed_tables) == set(SEED_COUNTS)
    assert "messages" in migrated_db.tables
    assert len(migrated_db.tables) >= 60
    with migrated_db.engine.connect() as conn:
        for table, count in SEED_COUNTS.items():
            assert (
                conn.execute(text(f"SELECT count(*) FROM seed.{table}")).scalar()
                == count
            )


def _insert_residue(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        session.add(StoredFileDb(hash="ab" * 32, size=1, type=FileType.FILE))
        session.add(
            PendingMessageDb(
                item_hash="cd" * 32,
                type=MessageType.post,
                chain=Chain.ETH,
                sender="0x" + "1" * 40,
                signature=None,
                item_type=ItemType.inline,
                item_content="{}",
                time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                channel=None,
                reception_time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                fetched=True,
                check_message=False,
                retries=0,
                next_attempt=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        # Burn a sequence value so the next test can see it was restarted.
        session.execute(
            text("SELECT nextval(pg_get_serial_sequence('file_pins', 'id'))")
        )
        session.commit()


def test_reset_step_1_leaves_residue(session_factory: DbSessionFactory):
    _insert_residue(session_factory)
    with session_factory() as session:
        assert session.execute(text("SELECT count(*) FROM files")).scalar() == 1


def test_reset_step_2_starts_from_a_clean_seeded_schema(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        assert session.execute(text("SELECT count(*) FROM files")).scalar() == 0
        assert (
            session.execute(text("SELECT count(*) FROM pending_messages")).scalar() == 0
        )
        for table, count in SEED_COUNTS.items():
            assert (
                session.execute(text(f"SELECT count(*) FROM {table}")).scalar() == count
            )
        assert (
            len(session.execute(select(ErrorCodeDb)).scalars().all())
            == EXPECTED_ERROR_CODE_ROWS
        )
        # RESTART IDENTITY: the burnt sequence value is back to 1.
        assert (
            session.execute(
                text("SELECT nextval(pg_get_serial_sequence('file_pins', 'id'))")
            ).scalar()
            == 1
        )
        # Triggers are enabled again even if a previous test disabled one.
        enabled = session.execute(
            text(
                "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled = 'O'"
            )
        ).scalar()
        assert enabled == 2


def test_reset_step_3_trigger_left_disabled_is_re_enabled(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        session.execute(text("ALTER TABLE messages DISABLE TRIGGER trg_message_counts"))
        session.commit()


def test_reset_step_4_sees_trigger_enabled(session_factory: DbSessionFactory):
    with session_factory() as session:
        state = session.execute(
            text("SELECT tgenabled FROM pg_trigger WHERE tgname = 'trg_message_counts'")
        ).scalar()
        assert state == "O"


def test_reset_step_5_creates_a_table(session_factory: DbSessionFactory):
    with session_factory() as session:
        session.execute(text("CREATE TABLE leftover_t (i int)"))
        OBSERVED_OIDS["before_created_table"] = _messages_oid(session)
        session.commit()


def test_reset_step_6_drops_the_created_table_without_rebuilding(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        assert (
            session.execute(text("SELECT to_regclass('public.leftover_t')")).scalar()
            is None
        )
        # A table the test created is dropped in place: no rebuild needed.
        assert _messages_oid(session) == _observed_oid(
            "before_created_table", "test_reset_step_5_creates_a_table"
        )


def test_reset_step_7_drops_an_expected_table(session_factory: DbSessionFactory):
    with session_factory() as session:
        # ccn_metrics_default is a leaf partition with no dependents, so dropping
        # it exercises the missing-table path without cascading to other tables.
        session.execute(text("DROP TABLE public.ccn_metrics_default"))
        OBSERVED_OIDS["before_dropped_table"] = _messages_oid(session)
        session.commit()


def test_reset_step_8_rebuilds_after_an_expected_table_was_dropped(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        # Only re-running the migrations can bring a dropped table back.
        assert (
            session.execute(
                text("SELECT to_regclass('public.ccn_metrics_default')")
            ).scalar()
            is not None
        )
        assert _messages_oid(session) != _observed_oid(
            "before_dropped_table", "test_reset_step_7_drops_an_expected_table"
        )
        # Baseline for the marked test below, which must rebuild again.
        OBSERVED_OIDS["before_fresh_schema"] = _messages_oid(session)


@pytest.mark.fresh_schema
def test_fresh_schema_marker_rebuilds(session_factory: DbSessionFactory, migrated_db):
    # A marked test gets a rebuilt schema and a refreshed seed snapshot.
    with session_factory() as session:
        for table, count in SEED_COUNTS.items():
            assert (
                session.execute(text(f"SELECT count(*) FROM {table}")).scalar() == count
            )
        # The rebuild really re-ran the migrations rather than deleting rows.
        assert _messages_oid(session) != _observed_oid(
            "before_fresh_schema",
            "test_reset_step_8_rebuilds_after_an_expected_table_was_dropped",
        )
    assert set(migrated_db.seed_tables) == set(SEED_COUNTS)
