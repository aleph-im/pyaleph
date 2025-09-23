"""add credit_history expiration_date index for cache invalidation performance

Revision ID: d0e1f2a3b4c5
Revises: c8d9e0f1a2b3
Create Date: 2025-01-11 00:00:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'd0e1f2a3b4c5'
down_revision = 'c8d9e0f1a2b3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add index on expiration_date for efficient cache invalidation queries
    # This index optimizes the query that checks if any credits expired
    # after the cached balance was last updated
    op.create_index(
        'ix_credit_history_expiration_date',
        'credit_history',
        ['expiration_date'],
        unique=False
    )


def downgrade() -> None:
    # Drop the expiration_date index
    op.drop_index('ix_credit_history_expiration_date', 'credit_history')