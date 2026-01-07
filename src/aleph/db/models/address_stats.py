from sqlalchemy import Column, Integer, String

from aleph.db.models.base import Base


class AddressStats(Base):
    __tablename__ = "address_stats_mat_view"

    address = Column(String, primary_key=True)
    type = Column(String, primary_key=True)
    nb_messages = Column(Integer, nullable=False)

    __mapper_args__ = {"confirm_deleted_rows": False}
