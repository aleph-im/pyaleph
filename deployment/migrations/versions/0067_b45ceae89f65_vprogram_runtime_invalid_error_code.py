"""Add the V-PROGRAM runtime invalid error code

Revision ID: b45ceae89f65
Revises: c1e8b4a9d3f2
Create Date: 2026-08-27

Seeds ``error_codes`` with code 305 (``ErrorCode.VM_RUNTIME_INVALID``), used
when a V-PROGRAM's runtime manifest is not pinned, cannot be read, or does
not name a valid bundle. The enum value was added ahead of this migration;
without this row, ``error_codes`` is missing the mapping the enum expects.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "b45ceae89f65"
down_revision = "c1e8b4a9d3f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            "INSERT INTO error_codes(code, description) VALUES (305, 'V-PROGRAM runtime manifest is invalid or does not name a pinned bundle') ON CONFLICT (code) DO NOTHING"
        )
    )


def downgrade() -> None:
    op.execute(text("DELETE FROM error_codes WHERE code = 305"))
