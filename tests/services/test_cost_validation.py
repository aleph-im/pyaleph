import datetime as dt
from decimal import Decimal

import pytest
import pytz
from aleph_message.models import Chain, ItemType, MessageType, PaymentType

from aleph.db.models import AlephBalanceDb, ChainTxDb, MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.services.cost_validation import validate_balance_for_payment
from aleph.toolkit.constants import STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import InsufficientBalanceException

ADDRESS = "0x2E5aC1Ae5E7eC9F5E6F16d24B0DAdE70ea3c93Fd"


def _make_message(item_hash: str, confirmations: list) -> MessageDb:
    return MessageDb(
        item_hash=item_hash,
        sender=ADDRESS,
        chain=Chain.ETH,
        type=MessageType.store,
        time=pytz.utc.localize(dt.datetime(2023, 1, 1)),
        item_type=ItemType.inline,
        signature=f"sig_{item_hash[:8]}",
        size=42,
        content={"address": ADDRESS, "item_type": "storage"},
        confirmations=confirmations,
    )


def _make_chain_tx(tx_hash: str, height: int) -> ChainTxDb:
    return ChainTxDb(
        hash=tx_hash,
        chain=Chain.ETH,
        height=height,
        datetime=pytz.utc.localize(dt.datetime(2023, 1, 1)),
        publisher="0xabadbabe",
        protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
        protocol_version=1,
        content="test-data",
    )


def _make_cost(item_hash: str, cost: str) -> AccountCostsDb:
    return AccountCostsDb(
        owner=ADDRESS,
        item_hash=item_hash,
        type=CostType.STORAGE,
        name="store",
        payment_type=PaymentType.hold,
        cost_hold=Decimal(cost),
        cost_stream=Decimal("0.0"),
    )


@pytest.mark.asyncio
async def test_hold_balance_check_ignores_pre_cutoff_costs(
    session_factory: DbSessionFactory,
):
    """Grandfathered (pre-cutoff) costs must not be billed against new messages.

    The balance cron job only removes messages confirmed at or after the cost
    cutoff height; the required balance computed at ingestion must apply the
    same rule. Otherwise messages from depleted senders are rejected twice:
    once by ingestion and never recovered by the reaper.
    """
    message = _make_message(
        "5179df4f5e75e83824d26f784c9d1af4bd1f7d7820b3ba1c21d4c826ea21b3d51",
        [
            _make_chain_tx(
                "0x79c9d244a5e3e03b6e71697ce25e5bfaa899d4f952a6eaa1cf3274055482f26d",
                STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT - 1,
            )
        ],
    )

    with session_factory() as session:
        session.add(message)
        session.flush()
        session.add(_make_cost(message.item_hash, "20.0"))
        # Zero balance: the sender has nothing left, but nothing they hold
        # is billable either.
        session.add(
            AlephBalanceDb(
                address=ADDRESS,
                chain=Chain.ETH,
                dapp=None,
                balance=Decimal(0),
                eth_height=0,
            )
        )
        session.commit()

        validate_balance_for_payment(
            session=session,
            address=ADDRESS,
            message_cost=Decimal(0),
            payment_type=PaymentType.hold,
        )


@pytest.mark.asyncio
async def test_hold_balance_check_counts_post_cutoff_costs(
    session_factory: DbSessionFactory,
):
    """Costs confirmed after the cutoff still require balance."""
    message = _make_message(
        "5179df4f5e75e83824d26f784c9d1af4bd1f7d7820b3ba1c21d4c826ea21b3d52",
        [
            _make_chain_tx(
                "0x79c9d244a5e3e03b6e71697ce25e5bfaa899d4f952a6eaa1cf3274055482f26e",
                STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 1,
            )
        ],
    )

    with session_factory() as session:
        session.add(message)
        session.flush()
        session.add(_make_cost(message.item_hash, "20.0"))
        session.add(
            AlephBalanceDb(
                address=ADDRESS,
                chain=Chain.ETH,
                dapp=None,
                balance=Decimal(0),
                eth_height=0,
            )
        )
        session.commit()

        with pytest.raises(InsufficientBalanceException) as exc_info:
            validate_balance_for_payment(
                session=session,
                address=ADDRESS,
                message_cost=Decimal(0),
                payment_type=PaymentType.hold,
            )

        assert exc_info.value.required_balance == Decimal("20.0")
