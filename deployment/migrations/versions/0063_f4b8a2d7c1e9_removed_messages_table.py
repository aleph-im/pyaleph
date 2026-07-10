"""Add removed_messages snapshot table

Revision ID: f4b8a2d7c1e9
Revises: e4a8c2d6f1b9
Create Date: 2026-07-06

Snapshot table for removed messages, mirroring forgotten_messages: at
REMOVING->REMOVED the garbage collector copies the billing metadata of the
messages row here and deletes the row (confirmations/costs/metrics follow
via ON DELETE CASCADE).

Two-phase lifecycle: the balance/credit-balance cron jobs snapshot the file
size at PROCESSED->REMOVING (the garbage collector deletes the files row
before the status flips to REMOVED, so the size must be captured while the
message is still alive), recovery deletes the record, and the garbage
collector fills the metadata and stamps removed_at at REMOVING->REMOVED.

removed_at is node-local (each node's GC finalizes removals on its own
schedule) and NOT deterministic across nodes — unlike forgotten_at there is
no sender-declared removal time to share.

Existing REMOVED messages are moved into the snapshot: their metadata is
copied (size best-effort from a still-existing files row, NULL otherwise;
removed_at unknown -> NULL, mirroring legacy forgotten rows) and their
messages rows are deleted.

The removed list endpoint filters by owner and windows/sorts on removed_at,
so a composite (owner, removed_at) index and a plain removed_at index are
built CONCURRENTLY (same pattern as migration 0060) to avoid blocking
writes.
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
            type VARCHAR,
            chain VARCHAR,
            sender VARCHAR,
            signature VARCHAR,
            item_type VARCHAR,
            time TIMESTAMPTZ,
            channel VARCHAR,
            owner VARCHAR,
            payment_type VARCHAR,
            size BIGINT,
            removed_at TIMESTAMPTZ
        )
        """
        )
    )

    # Move existing REMOVED messages into the snapshot. ON CONFLICT keeps
    # reruns (and rows already snapshotted while REMOVING) intact except for
    # the metadata, which only the messages row knows; an existing size
    # snapshot always wins over the files lookup.
    op.execute(
        text(
            """
        INSERT INTO removed_messages (
            item_hash, type, chain, sender, signature, item_type, time,
            channel, owner, payment_type, size, removed_at
        )
        SELECT
            m.item_hash, m.type, m.chain, m.sender, m.signature, m.item_type,
            m.time, m.channel, m.owner,
            COALESCE(m.payment_type, 'hold'),
            f.size,
            NULL
        FROM messages m
        JOIN message_status ms ON ms.item_hash = m.item_hash
        LEFT JOIN files f ON f.hash = m.content_item_hash
        WHERE ms.status = 'removed'
        ON CONFLICT (item_hash) DO UPDATE SET
            type = EXCLUDED.type,
            chain = EXCLUDED.chain,
            sender = EXCLUDED.sender,
            signature = EXCLUDED.signature,
            item_type = EXCLUDED.item_type,
            time = EXCLUDED.time,
            channel = EXCLUDED.channel,
            owner = EXCLUDED.owner,
            payment_type = EXCLUDED.payment_type,
            size = COALESCE(removed_messages.size, EXCLUDED.size)
        """
        )
    )
    op.execute(
        text(
            """
        DELETE FROM messages m
        USING message_status ms
        WHERE ms.item_hash = m.item_hash
          AND ms.status = 'removed'
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
                "ix_removed_messages_owner_removed_at "
                "ON removed_messages (owner, removed_at)"
            )
        )
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
    # The messages rows moved into the snapshot cannot be resurrected: this
    # only drops the table (dev convenience, data-destructive).
    op.execute(text("DROP TABLE IF EXISTS removed_messages"))
