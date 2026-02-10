"""Add STORE_HOLD_NOT_ALLOWED error code

Revision ID: a3b4c5d6e7f8
Revises: a1b2c3d4e5f7
Create Date: 2026-02-09

Adds error code 202 for STORE messages that attempt to use hold payment
after the cutoff timestamp.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "a3b4c5d6e7f8"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        INSERT INTO error_codes(code, description) VALUES
            (202, 'STORE messages with hold payment type are no longer allowed after the cutoff')
        """
        )
    )


def downgrade() -> None:
    op.execute(text("DELETE FROM error_codes WHERE code = 202"))
