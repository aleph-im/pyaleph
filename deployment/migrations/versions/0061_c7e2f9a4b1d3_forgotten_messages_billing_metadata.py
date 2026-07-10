"""Preserve billing metadata on forgotten_messages

Revision ID: c7e2f9a4b1d3
Revises: a1d4e7b2c9f6
Create Date: 2026-07-02

Adds owner, payment_type, size and forgotten_at columns to
forgotten_messages so forgotten STORE messages remain priceable and
deletions can be windowed by deletion time.

forgotten_at is the sender-supplied time of the forgetting FORGET message,
backfilled from the first FORGET still present in the messages table — the
same value on every node, consistent with the declared-time semantics the
live message list uses for sorting, date filters and cursors.
owner/payment_type/size cannot be recovered for legacy rows (the source
messages rows are already deleted) and stay NULL.

The forgotten list endpoint filters by owner and windows/sorts on
forgotten_at, so a composite (owner, forgotten_at) index and a plain
forgotten_at index are added. Both are built CONCURRENTLY (same pattern as
migration 0060) to avoid blocking writes on populated nodes.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "c7e2f9a4b1d3"
down_revision = "a1d4e7b2c9f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # IF NOT EXISTS: the CONCURRENT index step below commits the column
    # additions and the backfill before alembic stamps the revision, so a
    # rerun after an index failure must not error on the already-added
    # columns (same reasoning as migration 0063).
    op.execute(
        text(
            """
        ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS owner VARCHAR;
        ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS payment_type VARCHAR;
        ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS size BIGINT;
        ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS forgotten_at TIMESTAMPTZ;
        """
        )
    )
    op.execute(
        text(
            """
        UPDATE forgotten_messages fm
        SET forgotten_at = m.time
        FROM messages m
        WHERE m.item_hash = fm.forgotten_by[1]
          AND fm.forgotten_at IS NULL
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
                "ix_forgotten_messages_owner_forgotten_at "
                "ON forgotten_messages (owner, forgotten_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "ix_forgotten_messages_forgotten_at "
                "ON forgotten_messages (forgotten_at)"
            )
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    engine = connection.engine
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "DROP INDEX CONCURRENTLY IF EXISTS ix_forgotten_messages_forgotten_at"
            )
        )
        conn.execute(
            text(
                "DROP INDEX CONCURRENTLY IF EXISTS "
                "ix_forgotten_messages_owner_forgotten_at"
            )
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))

    op.execute(
        text(
            """
        ALTER TABLE forgotten_messages DROP COLUMN IF EXISTS forgotten_at;
        ALTER TABLE forgotten_messages DROP COLUMN IF EXISTS size;
        ALTER TABLE forgotten_messages DROP COLUMN IF EXISTS payment_type;
        ALTER TABLE forgotten_messages DROP COLUMN IF EXISTS owner;
        """
        )
    )
