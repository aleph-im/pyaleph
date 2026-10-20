"""Seeding helpers for the credit ledger v2 serving tests."""

import datetime as dt
from typing import Any, Dict, Optional

from aleph_message.models import MessageType, PaymentType
from sqlalchemy import update

from aleph.db.models.credit_ledger import (
    BillingResourceDb,
    CreditFoldStateDb,
    CreditLedgerDb,
    CreditLotDb,
)
from aleph.services.credits.addresses import normalize_address
from aleph.toolkit.constants import MiB
from aleph.types.db_session import DbSession

# A checksummed EVM address: the ledger stores it lowercased.
OWNER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
ROOTFS_REF = "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595"


def instance_spec() -> Dict[str, Any]:
    return {
        "type": "INSTANCE",
        "content": {
            "time": 1701099523.849,
            "rootfs": {
                "parent": {"ref": ROOTFS_REF, "use_latest": False},
                "size_mib": 20480,
                "persistence": "host",
            },
            "address": OWNER,
            "volumes": [],
            "metadata": {"name": "serving v2"},
            "resources": {"vcpus": 2, "memory": 4096, "seconds": 30},
            "allow_amend": False,
            "payment": {"chain": "ETH", "type": "credit"},
            "environment": {
                "internet": True,
                "aleph_api": True,
                "reproducible": False,
                "shared_cache": False,
                "hypervisor": "qemu",
            },
            "authorized_keys": [],
        },
        "volume_sizes_bytes": {ROOTFS_REF: MiB},
        "file_size_bytes": None,
    }


def set_last_closed_period(session: DbSession, period: Optional[dt.datetime]) -> None:
    session.execute(
        update(CreditFoldStateDb)
        .where(CreditFoldStateDb.id == 1)
        .values(last_closed_period=period)
    )


def seed_lot(
    session: DbSession,
    address: str,
    amount: int,
    *,
    period: dt.datetime,
    key: str = "1:0xtx:0",
    kind: str = "topup",
    expiration: Optional[dt.datetime] = None,
) -> None:
    """A positive ledger row and its lot, as the fold writes them."""
    row = CreditLedgerDb(
        period=period,
        address=address,
        kind=kind,
        business_key=key,
        amount=amount,
        expiration=expiration,
    )
    session.add(row)
    session.flush()
    session.add(
        CreditLotDb(
            ledger_id=row.id,
            address=address,
            period=period,
            kind=kind,
            business_key=key,
            amount_remaining=amount,
            expiration=expiration,
        )
    )
    session.flush()


def seed_instance(
    session: DbSession,
    item_hash: str,
    *,
    owner: str = OWNER,
    first_confirmed_at: Optional[dt.datetime],
    end_of_life: Optional[dt.datetime] = None,
    payment_type: PaymentType = PaymentType.credit,
) -> BillingResourceDb:
    resource = BillingResourceDb(
        item_hash=item_hash,
        type=MessageType.instance,
        # Stored in ledger form, as upsert_billing_resource writes it.
        owner=normalize_address(owner),
        payment_type=payment_type,
        first_confirmed_at=first_confirmed_at,
        end_of_life=end_of_life,
        cost_spec=instance_spec(),
    )
    session.add(resource)
    session.flush()
    return resource
