"""create address_summary_view

Revision ID: d6539a42cd51
Revises: 83a04f64a1db
Create Date: 2025-11-26 16:09:24.612444
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "d6539a42cd51"
down_revision = "d0e1f2a3b4c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Ensure pg_trgm extension is enabled
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # Create the new total summary materialized view
    op.execute(
        """
        CREATE MATERIALIZED VIEW address_total_message_stats AS
        SELECT
            address,
            SUM(nb_messages) AS total_messages
        FROM address_stats_mat_view
        GROUP BY address;
        """
    )

    # Create a unique index on address - required for concurrent refresh
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_address_total_message_stats_unique
        ON address_total_message_stats (address);
        """
    )

    # Trigram index for substring search on the materialized view
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_address_total_message_stats_trgm
        ON address_total_message_stats
        USING gin (lower(address) gin_trgm_ops);
        """
    )


def downgrade() -> None:
    # Drop indexes created by this migration
    op.execute("DROP INDEX IF EXISTS idx_address_total_message_stats_trgm;")
    op.execute("DROP INDEX IF EXISTS idx_address_total_message_stats_unique;")

    # Drop materialized view created by this migration
    op.execute("DROP MATERIALIZED VIEW IF EXISTS address_total_message_stats;")
