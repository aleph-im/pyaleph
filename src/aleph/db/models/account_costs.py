from decimal import Decimal
from typing import Optional

from aleph_message.models import PaymentType
from sqlalchemy import DECIMAL, BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.types.cost import CostType

from .base import Base


class AccountCostsDb(Base):
    __tablename__ = "account_costs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    owner: Mapped[str] = mapped_column(String, nullable=False, index=True)
    # item_hash: str = Column(String, nullable=False)
    item_hash: Mapped[str] = mapped_column(
        ForeignKey("messages.item_hash", ondelete="CASCADE"), nullable=False
    )
    type: Mapped[CostType] = mapped_column(ChoiceType(CostType), nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    payment_type: Mapped[PaymentType] = mapped_column(
        ChoiceType(PaymentType), nullable=False
    )
    cost_hold: Mapped[Decimal] = mapped_column(DECIMAL, nullable=False)
    cost_stream: Mapped[Decimal] = mapped_column(DECIMAL, nullable=False)
    cost_credit: Mapped[Decimal] = mapped_column(DECIMAL, nullable=False, default=0)

    __table_args__ = (UniqueConstraint("owner", "item_hash", "type", "name"),)
