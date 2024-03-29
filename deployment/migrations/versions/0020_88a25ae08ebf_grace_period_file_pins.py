"""grace period file pins

Revision ID: 88a25ae08ebf
Revises: 3bf484f2cc95
Create Date: 2023-11-02 22:43:40.223477

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "88a25ae08ebf"
down_revision = "3bf484f2cc95"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "file_pins", sa.Column("delete_by", sa.TIMESTAMP(timezone=True), nullable=True)
    )
    op.create_index(
        "ix_file_pins_delete_by",
        "file_pins",
        ["delete_by"],
        unique=False,
        postgresql_where=sa.text("delete_by IS NOT NULL"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        "ix_file_pins_delete_by",
        table_name="file_pins",
        postgresql_where=sa.text("delete_by IS NOT NULL"),
    )
    op.drop_column("file_pins", "delete_by")
    # ### end Alembic commands ###
