"""address stats view

Revision ID: 8a5eaab15d40
Revises: 8edf69c47884
Create Date: 2023-03-06 17:27:14.514803

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '8a5eaab15d40'
down_revision = '8edf69c47884'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        create materialized view address_stats_mat_view as 
            select sender as address, type, count(*) as nb_messages
                from messages
                group by sender, type
        """)
    op.execute("create unique index ix_address_type on address_stats_mat_view(address, type)")


def downgrade() -> None:
    op.execute("drop materialized view address_stats_mat_view")
