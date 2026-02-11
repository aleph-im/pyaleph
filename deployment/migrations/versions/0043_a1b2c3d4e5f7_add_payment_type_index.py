"""Add index on account_costs.payment_type

Revision ID: a1b2c3d4e5f7
Revises: f2a3b4c5d6e7
Create Date: 2026-02-10

Adds an index on the payment_type column to optimize queries filtering
messages by payment type (hold, superfluid, credit).
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f7"
down_revision = "f2a3b4c5d6e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_account_costs_payment_type",
        "account_costs",
        ["payment_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_account_costs_payment_type", table_name="account_costs")
