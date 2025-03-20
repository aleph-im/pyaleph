"""empty message

Revision ID: 8ece21fbeb47
Revises: 1c06d0ade60c
Create Date: 2025-03-18 09:58:57.469799

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision = '8ece21fbeb47'
down_revision = '1c06d0ade60c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "balances", sa.Column("last_update", sa.TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    )

    op.create_table(
        "cron_jobs",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("interval", sa.Integer(), nullable=False, default=24),
        sa.Column("last_run", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.execute(
        """
        INSERT INTO cron_jobs(id, interval, last_run) VALUES ('balance', 6, '2025-01-01 00:00:00')
        """
    )

    pass


def downgrade() -> None:
    op.drop_column("balances", "last_update")

    op.drop_table("cron_jobs")

    pass
