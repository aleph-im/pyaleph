"""empty message

Revision ID: 8ece21fbeb47
Revises: 1c06d0ade60c
Create Date: 2025-03-18 09:58:57.469799

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision = "8ece21fbeb47"
down_revision = "1c06d0ade60c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "balances",
        sa.Column(
            "last_update",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        ),
    )

    op.create_table(
        "cron_jobs",
        sa.Column("id", sa.String(), nullable=False),
        # Interval is specified in seconds
        sa.Column("interval", sa.Integer(), nullable=False, default=24),
        sa.Column("last_run", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.execute(
        text(
            """
        INSERT INTO cron_jobs(id, interval, last_run) VALUES ('balance', 3600, '2025-01-01 00:00:00')
        """
        )
    )

    op.execute(
        text(
            """
        INSERT INTO balances(address, chain, balance, eth_height)
        SELECT distinct m.sender, 'ETH', 0, 22196000 FROM messages m
        INNER JOIN message_status ms ON m.item_hash = ms.item_hash
        LEFT JOIN balances b ON m.sender = b.address
        WHERE m."type" = 'STORE' AND ms.status = 'processed' AND b.address is null AND m."time" > '2025-04-04T0:0:0.000Z'
        """
        )
    )

    pass


def downgrade() -> None:
    op.drop_column("balances", "last_update")

    op.drop_table("cron_jobs")

    op.execute(
        text(
            """
        DELETE FROM balances b WHERE b.eth_height = 22196000
        """
        )
    )

    pass
