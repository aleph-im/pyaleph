"""add_new_error_code_on_db

Revision ID: 9e600f404aa1
Revises: 46f7e55ff55c
Create Date: 2025-01-16 13:51:36.699939

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '9e600f404aa1'
down_revision = '46f7e55ff55c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO error_codes(code, description) VALUES 
            (503, 'Cannot forget a used message')
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM error_codes WHERE code = 503")
