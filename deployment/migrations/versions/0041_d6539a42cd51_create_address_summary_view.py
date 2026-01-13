"""create trigram extension for address search

Revision ID: d6539a42cd51
Revises: e1f2a3b4c5d6
Create Date: 2025-11-26 16:09:24.612444
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "d6539a42cd51"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Ensure pg_trgm extension is enabled for text search
    op.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))

    # Create trigram index for substring search on the address column of address_stats_mat_view
    op.execute(
        text(
            """
        CREATE INDEX IF NOT EXISTS idx_address_stats_mat_view_address_trgm
        ON address_stats_mat_view
        USING gin (lower(address) gin_trgm_ops);
        """
        )
    )

    # Create covering index to optimize queries that need address and nb_messages
    op.execute(
        text(
            """
        CREATE INDEX IF NOT EXISTS idx_address_stats_covering 
        ON address_stats_mat_view(address) 
        INCLUDE (nb_messages);
        """
        )
    )


def downgrade() -> None:
    # Drop indexes created by this migration
    op.execute(text("DROP INDEX IF EXISTS idx_address_stats_covering;"))
    op.execute(text("DROP INDEX IF EXISTS idx_address_stats_mat_view_address_trgm;"))
