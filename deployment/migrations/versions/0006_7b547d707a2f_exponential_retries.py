"""exponential retries

Revision ID: 7b547d707a2f
Revises: 68fd4bed8a8e
Create Date: 2023-01-20 15:55:33.581234

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7b547d707a2f"
down_revision = "68fd4bed8a8e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "pending_messages",
        sa.Column("next_attempt", sa.TIMESTAMP(timezone=True), nullable=False),
    )
    op.drop_index("ix_retries_time", table_name="pending_messages")
    op.create_index(
        "ix_next_attempt",
        "pending_messages",
        [sa.text("next_attempt ASC")],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_next_attempt", table_name="pending_messages")
    op.create_index(
        "ix_retries_time", "pending_messages", ["retries", "time"], unique=False
    )
    op.drop_column("pending_messages", "next_attempt")
    # ### end Alembic commands ###
