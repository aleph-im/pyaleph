"""Add INVALID_PAYMENT_METHOD error code

Revision ID: a3b4c5d6e7f8
Revises: b2c3d4e5f6a7
Create Date: 2026-02-09

Adds error code 202 for messages that attempt to use non-credit payment
after the cutoff timestamp.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "a3b4c5d6e7f8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        INSERT INTO error_codes(code, description) VALUES
            (202, 'Messages with non-credit payment types are no longer allowed')
        """
        )
    )


def downgrade() -> None:
    op.execute(text("DELETE FROM error_codes WHERE code = 202"))
