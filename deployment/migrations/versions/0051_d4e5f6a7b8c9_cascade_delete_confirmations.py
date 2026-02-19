"""Add ON DELETE CASCADE to message_confirmations FK

Revision ID: d4e5f6a7b8c9
Revises: f8a9b0c1d2e3
Create Date: 2026-02-19

Ensures deleting a message automatically removes its confirmations,
preventing FK violation errors.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "f8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "message_confirmations_item_hash_fkey",
        "message_confirmations",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "message_confirmations_item_hash_fkey",
        "message_confirmations",
        "messages",
        ["item_hash"],
        ["item_hash"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "message_confirmations_item_hash_fkey",
        "message_confirmations",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "message_confirmations_item_hash_fkey",
        "message_confirmations",
        "messages",
        ["item_hash"],
        ["item_hash"],
    )
