"""Add STORE_HOLD_NOT_ALLOWED error code

Revision ID: g3h4i5j6k7l8
Revises: f2a3b4c5d6e7
Create Date: 2026-02-09

Adds error code 202 for STORE messages that attempt to use hold payment
after the cutoff timestamp.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "g3h4i5j6k7l8"
down_revision = "f2a3b4c5d6e7"
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
