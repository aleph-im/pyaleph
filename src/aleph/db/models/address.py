from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base


# Define the base class
class ViewBase:
    pass


# Create the base with the class as a template
Base = declarative_base(cls=ViewBase)


class AddressStats(Base):
    __tablename__ = "address_stats_mat_view"

    address = Column(String, primary_key=True)
    type = Column(String, primary_key=True)
    nb_messages = Column(Integer, nullable=False)

    __mapper_args__ = {"confirm_deleted_rows": False}
