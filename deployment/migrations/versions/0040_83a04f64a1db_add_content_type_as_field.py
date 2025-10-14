"""Add content>>type as field

Revision ID: 83a04f64a1db
Revises: d0e1f2a3b4c5
Create Date: 2025-10-14 09:26:24.239634

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '83a04f64a1db'
down_revision = 'd0e1f2a3b4c5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('messages', sa.Column('content_type', sa.String(), sa.Computed("content->>'type'", persisted=True), nullable=True))
    op.create_index('ix_messages_content_type', 'messages', ['content_type'], unique=False)
    # Add an index on sender + content.type as content.type are often used with the sender together
    op.create_index(
        'ix_messages_sender_content_type',
        'messages',
        ['sender', 'content_type'],
        unique=False
    )


def downgrade() -> None:
    op.drop_index('ix_messages_sender_content_type', 'messages')
    op.drop_index('ix_messages_content_type', 'messages')
    op.drop_column('messages', 'content_type')