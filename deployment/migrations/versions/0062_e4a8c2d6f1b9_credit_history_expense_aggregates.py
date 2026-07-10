"""Add expense_count and expense_size_mib to credit_history

Revision ID: e4a8c2d6f1b9
Revises: c7e2f9a4b1d3
Create Date: 2026-07-02

v2 storage credit-expense messages aggregate per-file lines into one entry
per address, carrying the number of files (count) and the total billed MiB
(size). These columns expose that breakdown in the credit history; v1
entries leave them NULL.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "e4a8c2d6f1b9"
down_revision = "c7e2f9a4b1d3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE credit_history ADD COLUMN expense_count BIGINT;
        ALTER TABLE credit_history ADD COLUMN expense_size_mib DECIMAL;
        """
        )
    )


def downgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE credit_history DROP COLUMN IF EXISTS expense_size_mib;
        ALTER TABLE credit_history DROP COLUMN IF EXISTS expense_count;
        """
        )
    )
