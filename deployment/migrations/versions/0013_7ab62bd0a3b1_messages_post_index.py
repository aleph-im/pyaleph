"""messages post index

Revision ID: 7ab62bd0a3b1
Revises: 8a5eaab15d40
Create Date: 2023-03-23 12:48:36.687433

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7ab62bd0a3b1"
down_revision = "7a7704f044db"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX ix_messages_posts_type_tags 
        ON messages((content->>'type'),(content->'content'->>'tags')) WHERE type = 'POST'
    """
    )


def downgrade() -> None:
    op.drop_index("ix_messages_posts_type_tags")
