"""Add removed_messages billing metadata table

Revision ID: f4b8a2d7c1e9
Revises: e4a8c2d6f1b9
Create Date: 2026-07-06

Two-phase removal record: the balance/credit-balance cron jobs snapshot the
file size at PROCESSED->REMOVING (the garbage collector deletes the files row
before the status flips to REMOVED, so the size must be captured while the
message is still alive), recovery deletes the row, and the garbage collector
stamps removed_at at REMOVING->REMOVED.

No backfill: legacy REMOVED rows have an unknown removal time (mirrors legacy
forgotten rows).

The removed list endpoint windows/sorts on removed_at, so its index is built
CONCURRENTLY (same pattern as migration 0060) to avoid blocking writes.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "f4b8a2d7c1e9"
down_revision = "e4a8c2d6f1b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # IF NOT EXISTS: the CONCURRENT index step below commits this CREATE
    # before alembic stamps the revision, so a rerun after an index failure
    # must not error on the already-created table.
    op.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS removed_messages (
            item_hash VARCHAR PRIMARY KEY,
            size BIGINT,
            removed_at TIMESTAMPTZ
        )
        """
        )
    )

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    engine = connection.engine
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "ix_removed_messages_removed_at "
                "ON removed_messages (removed_at)"
            )
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    op.execute(text("DROP TABLE IF EXISTS removed_messages"))
