"""add credit_balance cron job

Revision ID: c8d9e0f1a2b3
Revises: b7c8d9e0f1a2
Create Date: 2025-01-11 00:00:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'c8d9e0f1a2b3'
down_revision = 'b7c8d9e0f1a2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add credit_balance cron job to run every hour (3600 seconds)
    op.execute(
        """
        INSERT INTO cron_jobs(id, interval, last_run) 
        VALUES ('credit_balance', 3600, '2025-01-01 00:00:00')
        """
    )


def downgrade() -> None:
    # Remove credit_balance cron job
    op.execute(
        """
        DELETE FROM cron_jobs WHERE id = 'credit_balance'
        """
    )