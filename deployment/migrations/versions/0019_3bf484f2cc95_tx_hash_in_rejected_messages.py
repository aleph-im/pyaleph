"""tx_hash in rejected messages

Revision ID: 3bf484f2cc95
Revises: 7bcb8e5fe186
Create Date: 2023-07-31 00:08:17.990537

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "3bf484f2cc95"
down_revision = "7bcb8e5fe186"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("rejected_messages", sa.Column("tx_hash", sa.String(), nullable=True))
    op.create_foreign_key(None, "rejected_messages", "chain_txs", ["tx_hash"], ["hash"])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "rejected_messages_tx_hash_fkey", "rejected_messages", type_="foreignkey"
    )
    op.drop_column("rejected_messages", "tx_hash")
    # ### end Alembic commands ###
