"""Add indexes on denormalized columns

Revision ID: c5d6e7f8a9b0
Revises: b4c5d6e7f8a9
Create Date: 2026-02-18

Creates indexes on new denormalized columns using CONCURRENTLY to avoid
table locks during creation.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "c5d6e7f8a9b0"
down_revision = "b4c5d6e7f8a9"
branch_labels = None
depends_on = None

INDEXES = [
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_type_status_time ON messages (type, status, time DESC)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_owner_time ON messages (owner, time DESC)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_status ON messages (status)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_content_ref ON messages (content_ref) WHERE content_ref IS NOT NULL",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_content_type ON messages (content_type) WHERE content_type IS NOT NULL",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_content_key ON messages (content_key) WHERE content_key IS NOT NULL",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_first_confirmed ON messages (first_confirmed_at DESC NULLS LAST)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_confirmed_height ON messages (first_confirmed_height) WHERE first_confirmed_height IS NOT NULL",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_reception_time ON messages (reception_time DESC)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_payment_type ON messages (payment_type) WHERE payment_type IS NOT NULL",
]


TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION update_first_confirmed()
RETURNS TRIGGER AS $$
DECLARE
    tx_dt TIMESTAMPTZ;
    tx_ht BIGINT;
BEGIN
    SELECT datetime, height INTO tx_dt, tx_ht
    FROM chain_txs
    WHERE hash = NEW.tx_hash;

    IF tx_dt IS NOT NULL THEN
        UPDATE messages
        SET first_confirmed_at = LEAST(COALESCE(first_confirmed_at, tx_dt), tx_dt),
            first_confirmed_height = LEAST(COALESCE(first_confirmed_height, tx_ht), tx_ht)
        WHERE item_hash = NEW.item_hash;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_first_confirmed
    AFTER INSERT ON message_confirmations
    FOR EACH ROW
    EXECUTE FUNCTION update_first_confirmed();
"""


def upgrade() -> None:
    # CONCURRENTLY requires autocommit (no transaction)
    connection = op.get_bind()
    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    for idx_sql in INDEXES:
        connection.execute(text(idx_sql))

    if was_in_transaction:
        connection.execute(text("BEGIN"))

    # Trigger to keep first_confirmed_at/height in sync
    op.execute(text(TRIGGER_SQL))


def downgrade() -> None:
    op.execute(
        text(
            """
        DROP TRIGGER IF EXISTS trg_update_first_confirmed ON message_confirmations;
        DROP FUNCTION IF EXISTS update_first_confirmed();
        """
        )
    )

    connection = op.get_bind()
    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    for idx_name in [
        "ix_messages_type_status_time",
        "ix_messages_owner_time",
        "ix_messages_status",
        "ix_messages_content_ref",
        "ix_messages_content_type",
        "ix_messages_content_key",
        "ix_messages_first_confirmed",
        "ix_messages_confirmed_height",
        "ix_messages_reception_time",
        "ix_messages_payment_type",
    ]:
        connection.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}"))

    if was_in_transaction:
        connection.execute(text("BEGIN"))
