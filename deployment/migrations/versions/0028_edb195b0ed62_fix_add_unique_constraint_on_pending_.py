"""Fix: add Unique Constraint on pending messsage

Revision ID: edb195b0ed62
Revises: bafd49315934
Create Date: 2025-01-14 12:16:10.920697

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'edb195b0ed62'
down_revision = 'bafd49315934'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DELETE FROM pending_messages a USING pending_messages b WHERE a.id < b.id AND a.item_hash = b.item_hash;")
    op.create_unique_constraint('uq_pending_message', 'pending_messages', ['item_hash'])


def downgrade() -> None:
    op.drop_constraint('uq_pending_message', 'pending_messages', type_='unique')
