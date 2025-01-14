"""Fix: add Unique Constraint on pending messsage

Revision ID: edb195b0ed62
Revises: bafd49315934
Create Date: 2025-01-14 12:16:10.920697

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'edb195b0ed62'
down_revision = 'bafd49315934'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint('uq_pending_message', 'pending_messages', ['sender', 'item_hash', 'signature'])


def downgrade() -> None:
    op.drop_constraint('uq_pending_message', 'pending_messages', type_='unique')
