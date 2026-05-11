"""Add running_balance column to credit_balances for eager-maintained sum

Revision ID: c7d8e9f0a1b2
Revises: 7e5a630e4b36
Create Date: 2026-05-08
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c7d8e9f0a1b2"
down_revision = "7e5a630e4b36"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "credit_balances",
        sa.Column("running_balance", sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("credit_balances", "running_balance")
