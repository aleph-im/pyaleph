"""add cost_credit column to account_costs table

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2025-01-11 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "b7c8d9e0f1a2"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add cost_credit column to account_costs table
    op.add_column(
        "account_costs",
        sa.Column("cost_credit", sa.DECIMAL(), nullable=False, server_default="0"),
    )

    # Add missing CREDIT_INSUFFICIENT error code
    op.execute(
        text(
            """
        INSERT INTO error_codes(code, description) VALUES 
            (6, 'Insufficient credit')
        ON CONFLICT (code) DO NOTHING
        """
        )
    )


def downgrade() -> None:
    # Remove CREDIT_INSUFFICIENT error code
    op.execute(text("DELETE FROM error_codes WHERE code = 6"))

    # Remove cost_credit column from account_costs table
    op.drop_column("account_costs", "cost_credit")
