"""Backfill denormalized columns on messages

Revision ID: e7f8a9b0c1d2
Revises: d6e7f8a9b0c1
Create Date: 2026-02-18

Populates the new denormalized columns from message_status, JSONB content,
account_costs, message_confirmations, and forgotten_messages.
Enforces NOT NULL on status and reception_time after backfill.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "e7f8a9b0c1d2"
down_revision = "d6e7f8a9b0c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        -- Disable trigger during backfill to avoid unnecessary work.
        -- Migration 0050 will populate counts from scratch.
        ALTER TABLE messages DISABLE TRIGGER trg_message_counts;

        -- 1. Backfill status + reception_time from message_status.
        --    Every row in messages MUST have a matching message_status row.
        --    If any don't, the NOT NULL constraint below will fail -- that's
        --    intentional so we can debug missing data.
        UPDATE messages m
        SET status = ms.status,
            reception_time = ms.reception_time
        FROM message_status ms
        WHERE m.item_hash = ms.item_hash
          AND m.status IS NULL;

        -- 2. Backfill promoted JSONB fields
        UPDATE messages
        SET owner = content->>'address',
            content_type = content->>'type',
            content_ref = content->>'ref',
            content_key = content->>'key'
        WHERE owner IS NULL AND content IS NOT NULL;

        -- 3. Backfill payment_type from account_costs
        UPDATE messages m
        SET payment_type = ac.payment_type
        FROM account_costs ac
        WHERE m.item_hash = ac.item_hash
          AND m.payment_type IS NULL;

        -- 4. Backfill first_confirmed_at + first_confirmed_height
        UPDATE messages m
        SET first_confirmed_at = sub.earliest,
            first_confirmed_height = sub.height
        FROM (
            SELECT mc.item_hash,
                   MIN(ct.datetime) AS earliest,
                   MIN(ct.height) AS height
            FROM message_confirmations mc
            JOIN chain_txs ct ON mc.tx_hash = ct.hash
            GROUP BY mc.item_hash
        ) sub
        WHERE m.item_hash = sub.item_hash
          AND m.first_confirmed_at IS NULL;

        -- 5. Backfill forgotten messages: mark status='forgotten' for messages
        --    that exist in forgotten_messages.
        --    Note: currently forgotten messages are DELETED from messages table,
        --    so this UPDATE may find 0 rows. That's fine.
        UPDATE messages m
        SET status = 'forgotten',
            forgotten_by = fm.forgotten_by
        FROM forgotten_messages fm
        WHERE m.item_hash = fm.item_hash;

        -- 6. Enforce NOT NULL constraints.
        --    These will FAIL if any message is missing a corresponding
        --    message_status row. That's the desired behavior.
        ALTER TABLE messages ALTER COLUMN status SET NOT NULL;
        ALTER TABLE messages ALTER COLUMN reception_time SET NOT NULL;
        """
        )
    )


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
