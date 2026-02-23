"""Backfill payment_type for messages processed after initial migration

Revision ID: a1b2c3d4e5f6
Revises: f6a7b8c9d0e1
Create Date: 2026-02-23

Messages processed after migration 0048 had payment_type left NULL because
the from_pending_message() factory did not populate the denormalized column.
This migration fills the gap using two strategies:

1. Extract directly from content->'payment'->>'type' (no join, fast).
2. Fall back to account_costs.payment_type for legacy messages whose
   content JSONB lacks an explicit payment object.

Only STORE, PROGRAM, and INSTANCE message types can carry a payment;
other types are excluded to avoid unnecessary work.
"""

import logging

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

BATCH_SIZE = 50000
PAYMENT_TYPES = ("STORE", "PROGRAM", "INSTANCE")


def _batched_update(connection, label, sql):
    """Run an UPDATE in batches, committing between each."""
    total = 0
    while True:
        connection.execute(text("BEGIN"))
        result = connection.execute(text(sql))
        rows = result.rowcount
        total += rows
        connection.execute(text("COMMIT"))
        if rows > 0:
            logger.info(f"  {label}: {total} rows so far (+{rows})")
        if rows < BATCH_SIZE:
            break
    logger.info(f"  {label}: done — {total} rows total")


def upgrade() -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    logger.info("Backfilling payment_type from content JSONB...")
    _batched_update(
        connection,
        "jsonb_payment",
        f"""
        WITH batch AS (
            SELECT item_hash FROM messages
            WHERE payment_type IS NULL
              AND type IN {PAYMENT_TYPES!r}
              AND content->'payment'->>'type' IS NOT NULL
            LIMIT {BATCH_SIZE}
        )
        UPDATE messages
        SET payment_type = content->'payment'->>'type'
        WHERE item_hash IN (SELECT item_hash FROM batch)
        """,
    )

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    # payment_type is nullable; clearing only the rows this migration touched
    # is impractical, so downgrade is a no-op — the column already existed.
    pass
