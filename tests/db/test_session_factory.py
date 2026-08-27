"""The DB fixtures: one migration per session, TRUNCATE + seed restore per test."""

from sqlalchemy import text

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
