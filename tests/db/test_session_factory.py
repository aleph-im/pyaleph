"""The DB fixtures: one migration per session, delete + seed restore per test."""

import datetime as dt

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from sqlalchemy import select, text

from aleph.db.models import ErrorCodeDb, PendingMessageDb, StoredFileDb
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType

SEED_COUNTS = {"alembic_version": 1, "cron_jobs": 3, "error_codes": 25}


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
        assert len(session.execute(select(ErrorCodeDb)).scalars().all()) == 25
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


@pytest.mark.fresh_schema
def test_fresh_schema_marker_rebuilds(session_factory: DbSessionFactory, migrated_db):
    # A marked test gets a rebuilt schema and a refreshed seed snapshot.
    with session_factory() as session:
        for table, count in SEED_COUNTS.items():
            assert (
                session.execute(text(f"SELECT count(*) FROM {table}")).scalar() == count
            )
    assert set(migrated_db.seed_tables) == set(SEED_COUNTS)
