"""add_pending_messages_origin

Revision ID: bafd49315934
Revises: d3bba5c2bfa0
Create Date: 2025-01-13 15:05:05.309960

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bafd49315934'
down_revision = 'd3bba5c2bfa0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pending_messages", sa.Column("origin", sa.String(), nullable=True, default="p2p"))


def downgrade() -> None:
    op.drop_column("pending_messages", "origin")
