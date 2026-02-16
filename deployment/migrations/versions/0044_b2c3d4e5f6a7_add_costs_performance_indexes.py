"""Add performance indexes for costs endpoint

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f7
Create Date: 2026-02-11

Adds indexes to optimize the /api/v0/costs endpoint queries:
- Composite index on account_costs (owner, payment_type) for filtered queries
- Index on account_costs item_hash for FK lookups and joins
- Index on credit_history payment_method for consumed credits queries
- Index on credit_history origin for resource-specific credit lookups
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Composite index on (owner, payment_type) for filtered cost queries
    # This optimizes: WHERE owner = ? AND payment_type = ?
    op.create_index(
        "ix_account_costs_owner_payment_type",
        "account_costs",
        ["owner", "payment_type"],
        unique=False,
    )

    # Index on item_hash for FK lookups and joins with message_confirmations
    # PostgreSQL doesn't auto-create indexes on FK columns
    op.create_index(
        "ix_account_costs_item_hash",
        "account_costs",
        ["item_hash"],
        unique=False,
    )

    # Index on payment_method for filtering credit expenses
    # Optimizes: WHERE payment_method = 'credit_expense'
    op.create_index(
        "ix_credit_history_payment_method",
        "credit_history",
        ["payment_method"],
        unique=False,
    )

    # Index on origin for resource-specific credit lookups
    # Optimizes: WHERE origin IN (item_hash1, item_hash2, ...)
    op.create_index(
        "ix_credit_history_origin",
        "credit_history",
        ["origin"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_credit_history_origin", table_name="credit_history")
    op.drop_index("ix_credit_history_payment_method", table_name="credit_history")
    op.drop_index("ix_account_costs_item_hash", table_name="account_costs")
    op.drop_index("ix_account_costs_owner_payment_type", table_name="account_costs")
