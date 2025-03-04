"""Fix: UniqueCOnstraint should have sender item_hash and signature

Revision ID: 46f7e55ff55c
Revises: edb195b0ed62
Create Date: 2025-01-14 17:51:43.357255

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '46f7e55ff55c'
down_revision = 'edb195b0ed62'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint('uq_pending_message', 'pending_messages', type_='unique')
    op.create_unique_constraint('uq_pending_message', 'pending_messages', ['sender', 'item_hash', 'signature'])


def downgrade() -> None:
    op.drop_constraint('uq_pending_message', 'pending_messages', type_='unique')
    op.create_unique_constraint('uq_pending_message', 'pending_messages', ['item_hash'])
