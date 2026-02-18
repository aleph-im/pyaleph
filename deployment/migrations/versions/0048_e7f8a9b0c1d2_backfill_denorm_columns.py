"""Backfill denormalized columns on messages

Revision ID: e7f8a9b0c1d2
Revises: d6e7f8a9b0c1
Create Date: 2026-02-18

Populates the new denormalized columns from message_status, JSONB content,
account_costs, message_confirmations, and forgotten_messages.
Enforces NOT NULL on status and reception_time after backfill.

Uses batched UPDATEs with intermediate COMMITs to avoid WAL bloat,
table bloat, and prolonged row locks on large tables.
"""

import logging

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "e7f8a9b0c1d2"
down_revision = "d6e7f8a9b0c1"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

BATCH_SIZE = 50000


def _batched_update(connection, label, sql):
    """Run an UPDATE in batches, committing between each."""
    total = 0
    while True:
        connection.execute(text("BEGIN"))
        result = connection.execute(text(sql))
        rows = result.rowcount
        total += rows
        connection.execute(text("COMMIT"))
        logger.info(f"  {label}: {total} rows so far (+{rows})")
        if rows < BATCH_SIZE:
            break
    logger.info(f"  {label}: done — {total} rows total")


def upgrade() -> None:
    connection = op.get_bind()

    # Exit Alembic's default transaction to allow intermediate COMMITs
    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    # Disable trigger — migration 0050 will populate counts from scratch
    connection.execute(text("BEGIN"))
    connection.execute(text("ALTER TABLE messages DISABLE TRIGGER trg_message_counts"))
    connection.execute(text("COMMIT"))

    # Step 1: Backfill status + reception_time from message_status
    logger.info("Step 1/5: Backfilling status + reception_time...")
    _batched_update(
        connection,
        "status",
        f"""
        WITH batch AS (
            SELECT item_hash FROM messages WHERE status IS NULL LIMIT {BATCH_SIZE}
        )
        UPDATE messages m
        SET status = ms.status,
            reception_time = ms.reception_time
        FROM message_status ms
        WHERE m.item_hash = ms.item_hash
          AND m.item_hash IN (SELECT item_hash FROM batch)
        """,
    )

    # Step 2: Backfill promoted JSONB fields
    logger.info("Step 2/5: Backfilling JSONB fields...")
    _batched_update(
        connection,
        "jsonb",
        f"""
        WITH batch AS (
            SELECT item_hash FROM messages
            WHERE owner IS NULL AND content IS NOT NULL
            LIMIT {BATCH_SIZE}
        )
        UPDATE messages
        SET owner = content->>'address',
            content_type = content->>'type',
            content_ref = content->>'ref',
            content_key = content->>'key'
        WHERE item_hash IN (SELECT item_hash FROM batch)
        """,
    )

    # Step 3: Backfill payment_type from account_costs
    logger.info("Step 3/5: Backfilling payment_type...")
    _batched_update(
        connection,
        "payment_type",
        f"""
        WITH batch AS (
            SELECT m.item_hash FROM messages m
            JOIN account_costs ac ON m.item_hash = ac.item_hash
            WHERE m.payment_type IS NULL
            LIMIT {BATCH_SIZE}
        )
        UPDATE messages m
        SET payment_type = ac.payment_type
        FROM account_costs ac
        WHERE m.item_hash = ac.item_hash
          AND m.item_hash IN (SELECT item_hash FROM batch)
        """,
    )

    # Step 4: Backfill first_confirmed_at + first_confirmed_height
    logger.info("Step 4/5: Backfilling confirmation timestamps...")
    _batched_update(
        connection,
        "confirmed",
        f"""
        WITH batch AS (
            SELECT DISTINCT m.item_hash FROM messages m
            JOIN message_confirmations mc ON m.item_hash = mc.item_hash
            WHERE m.first_confirmed_at IS NULL
            LIMIT {BATCH_SIZE}
        )
        UPDATE messages m
        SET first_confirmed_at = sub.earliest,
            first_confirmed_height = sub.height
        FROM (
            SELECT mc.item_hash,
                   MIN(ct.datetime) AS earliest,
                   MIN(ct.height) AS height
            FROM message_confirmations mc
            JOIN chain_txs ct ON mc.tx_hash = ct.hash
            WHERE mc.item_hash IN (SELECT item_hash FROM batch)
            GROUP BY mc.item_hash
        ) sub
        WHERE m.item_hash = sub.item_hash
        """,
    )

    # Step 5: Backfill forgotten messages (typically 0 rows — forgotten messages
    # are currently DELETEd from the messages table, not kept)
    logger.info("Step 5/5: Backfilling forgotten messages...")
    connection.execute(text("BEGIN"))
    connection.execute(
        text(
            """
        UPDATE messages m
        SET status = 'forgotten',
            forgotten_by = fm.forgotten_by
        FROM forgotten_messages fm
        WHERE m.item_hash = fm.item_hash
          AND m.status IS DISTINCT FROM 'forgotten'
        """
        )
    )
    connection.execute(text("COMMIT"))

    # Enforce NOT NULL constraints
    logger.info("Setting NOT NULL constraints...")
    connection.execute(text("BEGIN"))
    connection.execute(text("ALTER TABLE messages ALTER COLUMN status SET NOT NULL"))
    connection.execute(
        text("ALTER TABLE messages ALTER COLUMN reception_time SET NOT NULL")
    )
    connection.execute(text("COMMIT"))

    # Re-enter transaction for Alembic's version tracking
    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE messages ALTER COLUMN reception_time DROP NOT NULL;
        ALTER TABLE messages ALTER COLUMN status DROP NOT NULL;

        UPDATE messages SET status = NULL, reception_time = NULL,
            owner = NULL, content_type = NULL, content_ref = NULL,
            content_key = NULL, first_confirmed_at = NULL,
            first_confirmed_height = NULL, forgotten_by = NULL,
            payment_type = NULL;

        ALTER TABLE messages ENABLE TRIGGER trg_message_counts;
        """
        )
    )
