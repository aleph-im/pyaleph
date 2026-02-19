"""Add denormalized columns to messages table

Revision ID: b4c5d6e7f8a9
Revises: a3b4c5d6e7f8
Create Date: 2026-02-18

Adds status, reception_time, owner, content_type, content_ref, content_key,
content_item_hash, first_confirmed_at, first_confirmed_height, and payment_type
columns to the messages table.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "b4c5d6e7f8a9"
down_revision = "a3b4c5d6e7f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE messages ADD COLUMN status VARCHAR;
        ALTER TABLE messages ADD COLUMN reception_time TIMESTAMPTZ;
        ALTER TABLE messages ADD COLUMN owner VARCHAR;
        ALTER TABLE messages ADD COLUMN content_type VARCHAR;
        ALTER TABLE messages ADD COLUMN content_ref VARCHAR;
        ALTER TABLE messages ADD COLUMN content_key VARCHAR;
        ALTER TABLE messages ADD COLUMN content_item_hash VARCHAR;
        ALTER TABLE messages ADD COLUMN first_confirmed_at TIMESTAMPTZ;
        ALTER TABLE messages ADD COLUMN first_confirmed_height BIGINT;
        ALTER TABLE messages ADD COLUMN payment_type VARCHAR;
        """
        )
    )


def downgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE messages DROP COLUMN IF EXISTS payment_type;
        ALTER TABLE messages DROP COLUMN IF EXISTS first_confirmed_height;
        ALTER TABLE messages DROP COLUMN IF EXISTS first_confirmed_at;
        ALTER TABLE messages DROP COLUMN IF EXISTS content_item_hash;
        ALTER TABLE messages DROP COLUMN IF EXISTS content_key;
        ALTER TABLE messages DROP COLUMN IF EXISTS content_ref;
        ALTER TABLE messages DROP COLUMN IF EXISTS content_type;
        ALTER TABLE messages DROP COLUMN IF EXISTS owner;
        ALTER TABLE messages DROP COLUMN IF EXISTS reception_time;
        ALTER TABLE messages DROP COLUMN IF EXISTS status;
        """
        )
    )
