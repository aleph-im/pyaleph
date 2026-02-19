"""Populate message_counts from existing data

Revision ID: f8a9b0c1d2e3
Revises: c5d6e7f8a9b0
Create Date: 2026-02-18

Aggregates existing messages into the message_counts table for all dimension
combinations (global by status, per type+status, per sender+status,
per owner+status), then re-enables the trigger.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "f8a9b0c1d2e3"
down_revision = "c5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        -- Clear any stale data (safety)
        TRUNCATE message_counts;

        -- Global: by status only
        INSERT INTO message_counts (status, count)
        SELECT COALESCE(status, ''), COUNT(*)
        FROM messages
        GROUP BY status;

        -- Per (type, status)
        INSERT INTO message_counts (type, status, count)
        SELECT COALESCE(type, ''), COALESCE(status, ''), COUNT(*)
        FROM messages
        GROUP BY type, status;

        -- Per (sender, status)
        INSERT INTO message_counts (sender, status, count)
        SELECT COALESCE(sender, ''), COALESCE(status, ''), COUNT(*)
        FROM messages
        GROUP BY sender, status;

        -- Per (sender, type, status) — for per-address stats
        INSERT INTO message_counts (sender, type, status, count)
        SELECT COALESCE(sender, ''), COALESCE(type, ''), COALESCE(status, ''), COUNT(*)
        FROM messages
        GROUP BY sender, type, status;

        -- Per (owner, status)
        INSERT INTO message_counts (owner, status, count)
        SELECT owner, COALESCE(status, ''), COUNT(*)
        FROM messages
        WHERE owner IS NOT NULL AND owner != ''
        GROUP BY owner, status;

        -- Re-enable trigger: all future changes are tracked automatically
        ALTER TABLE messages ENABLE TRIGGER trg_message_counts;

        -- Drop the materialized view superseded by message_counts
        DROP MATERIALIZED VIEW IF EXISTS address_stats_mat_view;
        """
        )
    )


def downgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE messages DISABLE TRIGGER trg_message_counts;
        TRUNCATE message_counts;

        -- Recreate the materialized view that was dropped during upgrade
        CREATE MATERIALIZED VIEW address_stats_mat_view AS
            SELECT sender AS address, type, COUNT(*) AS nb_messages
            FROM messages
            GROUP BY sender, type;
        CREATE UNIQUE INDEX ix_address_type ON address_stats_mat_view(address, type);
        CREATE INDEX idx_address_stats_mat_view_address_trgm
            ON address_stats_mat_view USING gin (lower(address) gin_trgm_ops);
        CREATE INDEX idx_address_stats_covering
            ON address_stats_mat_view(address) INCLUDE (nb_messages);
        """
        )
    )
