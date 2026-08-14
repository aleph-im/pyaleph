"""Preserve observed time on forgotten/removed message snapshots

Revision ID: c1e8b4a9d3f2
Revises: b3d7f1a9c4e2
Create Date: 2026-08-14

Adds an ``observed_time`` column to ``forgotten_messages`` and
``removed_messages``. The pricing endpoints for forgotten/removed STORE
messages select the pricing model by message time; using the sender-supplied
``time`` there lets a backdated message be priced against an older model. The
messages row is deleted when a message is forgotten/removed, so the trusted
observed time (on-chain confirmation, else node reception) cannot be recovered
by joining back later and must be snapshotted at forget/remove time.

Legacy rows created before this column existed keep ``observed_time`` NULL;
their source messages rows are already gone, so there is nothing to backfill
from, and the pricing paths fall back to the stored ``time`` for those rows.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "c1e8b4a9d3f2"
down_revision = "b3d7f1a9c4e2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS observed_time TIMESTAMPTZ;
        ALTER TABLE removed_messages ADD COLUMN IF NOT EXISTS observed_time TIMESTAMPTZ;
        """
        )
    )


def downgrade() -> None:
    op.execute(
        text(
            """
        ALTER TABLE removed_messages DROP COLUMN IF EXISTS observed_time;
        ALTER TABLE forgotten_messages DROP COLUMN IF EXISTS observed_time;
        """
        )
    )
