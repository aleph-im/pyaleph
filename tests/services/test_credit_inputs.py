import datetime as dt
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest
from aleph_message.models import Chain, ItemType, MessageType, PaymentType
from configmanager import Config
from services.test_cost_spec_rates import OWNER, ROOTFS_REF, instance_content

from aleph.db.models import MessageDb, PostDb
from aleph.db.models.credit_ledger import (
    BillingResourceDb,
    CreditAttestationDb,
    CreditTopupDb,
)
from aleph.services.credits.fold import (
    ExclusionInput,
    PeriodInputs,
    TopupInput,
    TransferInput,
    execution_amount,
)
from aleph.services.credits.inputs import (
    AttestationPending,
    collect_inputs,
    normalize_address,
)
from aleph.services.credits.rates import rate_for
from aleph.toolkit.constants import MiB
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

PS = dt.datetime(2025, 6, 1, 12, tzinfo=dt.timezone.utc)
PE = PS + dt.timedelta(hours=1)
P = int(PS.timestamp())
OWNER_KEY = OWNER.lower()
ATTESTOR = "0xAttestor0000000000000000000000000000000001"
ATTESTATION = "f" * 64
NODE_A = "a" * 64
NODE_B = "b" * 64
VM = "1" * 64
VM2 = "2" * 64


def at(minutes: int) -> dt.datetime:
    return PS + dt.timedelta(minutes=minutes)


def ts(minutes: int) -> int:
    return P + minutes * 60


def instance_spec(**spec_overrides: Any) -> Dict[str, Any]:
    spec: Dict[str, Any] = {
        "type": "INSTANCE",
        "content": instance_content(volumes=[]),
        "volume_sizes_bytes": {ROOTFS_REF: MiB},
        "file_size_bytes": None,
    }
    spec.update(spec_overrides)
    return spec


def program_spec(persistent: bool) -> Dict[str, Any]:
    return {
        "type": "PROGRAM",
        "content": {"on": {"http": True, "persistent": persistent}},
        "volume_sizes_bytes": {},
        "file_size_bytes": None,
    }


def store_spec(size_bytes: Optional[int]) -> Dict[str, Any]:
    return {
        "type": "STORE",
        "content": {
            "address": OWNER,
            "time": 1701099523.849,
            "item_type": "ipfs",
            "item_hash": "Qm" + "a" * 44,
            "payment": {"chain": "ETH", "type": "credit"},
        },
        "volume_sizes_bytes": {},
        "file_size_bytes": size_bytes,
    }


def resource(
    item_hash: str,
    *,
    type_: MessageType = MessageType.instance,
    payment_type: PaymentType = PaymentType.credit,
    first_confirmed_at: Optional[dt.datetime] = PS - dt.timedelta(days=1),
    end_of_life: Optional[dt.datetime] = None,
    spec: Optional[Dict[str, Any]] = None,
    owner: str = OWNER,
) -> BillingResourceDb:
    return BillingResourceDb(
        item_hash=item_hash,
        type=type_,
        owner=owner,
        payment_type=payment_type,
        first_confirmed_at=first_confirmed_at,
        end_of_life=end_of_life,
        cost_spec=spec if spec is not None else instance_spec(),
    )


def attestation_message(
    item_hash: str, confirmed: Optional[dt.datetime] = PS + dt.timedelta(minutes=70)
) -> MessageDb:
    """The message row of an attestation post: its confirmation orders it."""
    return MessageDb(
        item_hash=item_hash,
        chain=Chain.ETH,
        sender=ATTESTOR,
        signature="0xsig",
        item_type=ItemType.inline,
        type=MessageType.post,
        content={"address": ATTESTOR, "type": "aleph-execution-intervals"},
        size=100,
        time=PS + dt.timedelta(minutes=65),
        channel="ALEPH-METERING",
        first_confirmed_at=confirmed,
        status_value=MessageStatus.PROCESSED,
    )


def attestation(
    intervals: List[Dict[str, Any]],
    *,
    item_hash: str = ATTESTATION,
    confirmed: Optional[dt.datetime] = PS + dt.timedelta(minutes=70),
    supersedes: Optional[str] = None,
) -> List[Any]:
    """Message, post and index rows of a (by default confirmed) attestation."""
    content = {
        "version": 1,
        "period": {"start": P, "end": P + 3600},
        "source": "test-attestor",
        "intervals": intervals,
        "totals": {},
    }
    if supersedes is not None:
        content["supersedes"] = supersedes
    return [
        attestation_message(item_hash, confirmed),
        PostDb(
            item_hash=item_hash,
            owner=ATTESTOR,
            type="aleph-execution-intervals",
            channel="ALEPH-METERING",
            content=content,
            creation_datetime=PS + dt.timedelta(minutes=65),
        ),
        CreditAttestationDb(
            item_hash=item_hash,
            period=PS,
            sender=ATTESTOR,
            supersedes=supersedes,
            status="active",
        ),
    ]


def interval(vm: str, node: str, start: int, end: int) -> Dict[str, Any]:
    return {"vm": vm, "node": node, "start": ts(start), "end": ts(end)}


@pytest.fixture
def pricing(fixture_product_prices_aggregate_in_db, fixture_settings_aggregate_in_db):
    return None


def collect(
    session_factory: DbSessionFactory,
    config: Config,
    rows: List[Any],
    *,
    grace: bool = True,
) -> PeriodInputs:
    with session_factory() as session:
        session.add_all(rows)
        session.flush()
        inputs = collect_inputs(session, PS, config, attestation_grace_elapsed=grace)
        session.rollback()
    return inputs


def expected_amount(
    session_factory: DbSessionFactory, res: BillingResourceDb, seconds: int
) -> int:
    with session_factory() as session:
        session.add(res)
        session.flush()
        rate = rate_for(session, res, PS)
        session.rollback()
    return execution_amount(rate, seconds)


def vm_exclusion(
    vm: str, node: str, reason: str, address: Optional[str] = OWNER_KEY
) -> ExclusionInput:
    return ExclusionInput(
        kind="execution",
        business_key=f"{P}:{vm}:{node}",
        address=address,
        resource=vm,
        node_id=node,
        reason=reason,
    )


def test_execution_interval_for_unknown_vm_is_excluded(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    inputs = collect(
        session_factory, mock_config, attestation([interval(VM, NODE_A, 0, 60)])
    )
    assert inputs.executions == []
    assert inputs.exclusions == [
        vm_exclusion(VM, NODE_A, "unknown_message", address=None)
    ]


def test_execution_interval_clamped_to_confirmation_and_forget(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM, first_confirmed_at=at(10), end_of_life=at(40)),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.exclusions == []
    [execution] = inputs.executions
    assert (execution.vm, execution.node, execution.seconds) == (VM, NODE_A, 1800)
    assert execution.owner == OWNER_KEY
    assert execution.payment_type == "credit"
    assert execution.rate is not None and execution.rate > 0


def test_interval_entirely_outside_the_vm_life_is_excluded(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM, first_confirmed_at=at(30)),
        resource(VM2, end_of_life=at(20)),
        *attestation([interval(VM, NODE_A, 0, 20), interval(VM2, NODE_A, 30, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.executions == []
    assert inputs.exclusions == [
        vm_exclusion(VM, NODE_A, "before_confirmation"),
        vm_exclusion(VM2, NODE_A, "after_end_of_life"),
    ]


def test_two_nodes_same_vm_capped_at_3600_trimming_later_node(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM),
        *attestation([interval(VM, NODE_B, 30, 60), interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    [execution] = inputs.executions
    assert (execution.node, execution.seconds) == (NODE_A, 3600)
    assert inputs.exclusions == [vm_exclusion(VM, NODE_B, "over_cap")]


def test_cap_trims_later_node_partially(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM),
        *attestation([interval(VM, NODE_A, 0, 40), interval(VM, NODE_B, 30, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    by_node = {e.node: e.seconds for e in inputs.executions}
    assert by_node == {NODE_A: 2400, NODE_B: 1200}
    assert inputs.exclusions == []


def test_hold_vm_becomes_hold_equivalent_input(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM, payment_type=PaymentType.hold),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    [execution] = inputs.executions
    assert execution.payment_type == "hold"
    assert execution.seconds == 3600
    assert execution.excluded_reason is None
    assert inputs.exclusions == []


def test_stream_vm_excluded_payment_stream(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM, payment_type=PaymentType.superfluid),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.executions == []
    assert inputs.exclusions == [vm_exclusion(VM, NODE_A, "payment_stream")]


def test_on_demand_program_excluded(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM, type_=MessageType.program, spec=program_spec(False)),
        *attestation([interval(VM, NODE_A, 0, 10), interval(VM, NODE_A, 20, 30)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.executions == []
    assert inputs.exclusions == [vm_exclusion(VM, NODE_A, "on_demand_program")]


def test_store_in_an_interval_is_unknown_message(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM, type_=MessageType.store, spec=store_spec(MiB)),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.executions == []
    # The STORE itself is still billed as storage.
    assert len(inputs.storage) == 1
    assert inputs.exclusions == [vm_exclusion(VM, NODE_A, "unknown_message")]


def test_unpriceable_vm_and_store_are_excluded(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    broken = instance_spec()
    del broken["volume_sizes_bytes"]
    store_hash = "5" * 64
    rows = [
        resource(VM, spec=broken),
        resource(store_hash, type_=MessageType.store, spec=store_spec(None)),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.executions == []
    assert inputs.storage == []
    assert inputs.exclusions == [
        vm_exclusion(VM, NODE_A, "unpriceable"),
        ExclusionInput(
            kind="storage",
            business_key=f"{P}:{store_hash}",
            address=OWNER_KEY,
            resource=store_hash,
            node_id=None,
            reason="unpriceable",
        ),
    ]


def test_vm_whose_content_no_longer_validates_is_unpriceable(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    # Neither a pydantic error nor a cost-code ValueError may stop the fold.
    invalid = instance_spec(content={"rootfs": "not an object"})
    rows = [
        resource(VM, spec=invalid),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.executions == []
    assert inputs.exclusions == [vm_exclusion(VM, NODE_A, "unpriceable")]


def test_live_vm_without_interval_gets_no_attestation_row(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    persistent = "3" * 64
    on_demand = "4" * 64
    held = "6" * 64
    dead = "7" * 64
    rows = [
        resource(VM),
        resource(VM2),
        resource(persistent, type_=MessageType.program, spec=program_spec(True)),
        resource(on_demand, type_=MessageType.program, spec=program_spec(False)),
        resource(held, payment_type=PaymentType.hold),
        resource(dead, end_of_life=PS),
        *attestation([interval(VM, NODE_A, 0, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert [e.vm for e in inputs.executions] == [VM]
    assert inputs.exclusions == [
        ExclusionInput(
            kind="execution",
            business_key=f"{P}:{vm}:",
            address=OWNER_KEY,
            resource=vm,
            node_id=None,
            reason="no_attestation",
        )
        for vm in (VM2, persistent)
    ]


def test_missing_attestation_after_grace_yields_single_row(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    inputs = collect(session_factory, mock_config, [resource(VM)])
    assert inputs.executions == []
    assert inputs.exclusions == [
        ExclusionInput(
            kind="execution",
            business_key=f"{P}:attestation_missing",
            address=None,
            resource=None,
            node_id=None,
            reason="attestation_missing",
        )
    ]


def test_missing_attestation_before_grace_raises_pending(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    with pytest.raises(AttestationPending):
        collect(session_factory, mock_config, [resource(VM)], grace=False)


def test_storage_aggregates_per_owner(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    small = resource("8" * 64, type_=MessageType.store, spec=store_spec(30 * MiB))
    big = resource("9" * 64, type_=MessageType.store, spec=store_spec(100 * MiB))
    expected = expected_amount(
        session_factory,
        resource("8" * 64, type_=MessageType.store, spec=store_spec(30 * MiB)),
        3600,
    ) + expected_amount(
        session_factory,
        resource("9" * 64, type_=MessageType.store, spec=store_spec(100 * MiB)),
        3600,
    )
    inputs = collect(session_factory, mock_config, [small, big, *attestation([])])
    [storage] = inputs.storage
    assert storage.address == OWNER_KEY
    assert storage.count == 2
    assert storage.size_mib == 130
    assert storage.seconds == 7200
    assert storage.amount == expected
    assert inputs.exclusions == []


def test_storage_window_clips_to_forget(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    spec = store_spec(100 * MiB)
    expected = expected_amount(
        session_factory,
        resource("8" * 64, type_=MessageType.store, spec=spec),
        900,
    )
    forgotten = resource(
        "8" * 64,
        type_=MessageType.store,
        spec=spec,
        first_confirmed_at=at(5),
        end_of_life=at(20),
    )
    inputs = collect(session_factory, mock_config, [forgotten, *attestation([])])
    [storage] = inputs.storage
    assert (storage.seconds, storage.count, storage.size_mib) == (900, 1, 100)
    assert storage.amount == expected


_UNSET = object()
_MISSING = object()


def transfer_message(
    item_hash: str,
    confirmed: dt.datetime,
    sender: str,
    credits: List[Dict[str, Any]],
    channel: str = "ALEPH_CREDIT",
    status: MessageStatus = MessageStatus.PROCESSED,
    post_content: Any = _UNSET,
) -> MessageDb:
    content: Dict[str, Any] = {
        "address": sender,
        "time": 1701099523.849,
        "type": "aleph_credit_transfer",
        "content": {"transfer": {"credits": credits}},
    }
    if post_content is _MISSING:
        del content["content"]
    elif post_content is not _UNSET:
        content["content"] = post_content
    return MessageDb(
        item_hash=item_hash,
        chain=Chain.ETH,
        sender=sender,
        signature="0xsig",
        item_type=ItemType.inline,
        type=MessageType.post,
        content=content,
        size=100,
        time=confirmed,
        channel=channel,
        first_confirmed_at=confirmed,
        status_value=status,
    )


def test_transfer_inputs_ordered_by_confirmation_then_hash(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    recipient = "0xBEEF000000000000000000000000000000000001"
    expiration_ms = 1_800_000_000_000
    rows = [
        transfer_message(
            "c" * 64, at(30), OWNER, [{"address": recipient, "amount": 5}]
        ),
        transfer_message(
            "b" * 64,
            at(10),
            OWNER,
            [{"address": recipient, "amount": 7, "expiration": expiration_ms}],
        ),
        transfer_message(
            "a" * 64, at(30), OWNER, [{"address": recipient, "amount": 3}]
        ),
        # Out of the period, and on another channel: not inputs.
        transfer_message(
            "d" * 64, at(60), OWNER, [{"address": recipient, "amount": 1}]
        ),
        transfer_message(
            "e" * 64,
            at(15),
            OWNER,
            [{"address": recipient, "amount": 1}],
            channel="OTHER",
        ),
        *attestation([]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    expiration = dt.datetime.fromtimestamp(1_800_000_000, tz=dt.timezone.utc)
    assert inputs.transfers == [
        TransferInput("b" * 64, OWNER_KEY, [(recipient.lower(), 7, expiration)]),
        TransferInput("a" * 64, OWNER_KEY, [(recipient.lower(), 3, None)]),
        TransferInput("c" * 64, OWNER_KEY, [(recipient.lower(), 5, None)]),
    ]


def test_topup_inputs_keyed_by_chain_tx_and_log(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    sender = "0x" + "ab" * 20
    rows = [
        CreditTopupDb(
            chain_id=1,
            tx_hash="0x" + "11" * 32,
            log_index=log_index,
            block_number=100,
            block_time=at(5),
            from_address=sender,
            token="usdc",
            raw_amount=Decimal(1),
            quote_num=Decimal(1),
            quote_den=Decimal(1),
            credits=credits,
            status=status,
        )
        for log_index, credits, status in ((2, 50, "credited"), (3, 9, "unquoted"))
    ]
    inputs = collect(session_factory, mock_config, [*rows, *attestation([])])
    assert inputs.topups == [
        TopupInput(
            address=sender,
            credits=50,
            business_key=f"1:{'0x' + '11' * 32}:2",
            source_ref="0x" + "11" * 32,
        )
    ]


@pytest.mark.parametrize(
    "post_content",
    [
        {},
        None,
        _MISSING,
        "not a transfer",
        {"transfer": {"credits": "x"}},
        {
            "transfer": {
                "credits": [{"address": "0xB", "amount": 1, "expiration": 10**30}]
            }
        },
    ],
    ids=["empty", "null", "missing", "wrong-type", "wrong-credits", "huge-expiration"],
)
def test_malformed_transfer_is_excluded_not_raised(
    session_factory: DbSessionFactory, mock_config: Config, pricing, post_content
):
    rows = [
        transfer_message("a" * 64, at(10), OWNER, [], post_content=post_content),
        transfer_message("b" * 64, at(20), OWNER, [{"address": OWNER, "amount": 2}]),
        *attestation([]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.transfers == [
        TransferInput("b" * 64, OWNER_KEY, [(OWNER_KEY, 2, None)])
    ]
    assert inputs.exclusions == [
        ExclusionInput(
            kind="transfer_out",
            business_key=f"{'a' * 64}:0",
            address=OWNER_KEY,
            resource="a" * 64,
            node_id=None,
            reason="malformed",
        )
    ]


def test_transfer_not_processed_is_not_an_input(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        transfer_message(
            "a" * 64,
            at(10),
            OWNER,
            [{"address": OWNER, "amount": 2}],
            status=MessageStatus.REMOVED,
        ),
        *attestation([]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert inputs.transfers == []
    assert inputs.exclusions == []


def test_attestation_without_post_row_is_treated_as_missing(
    session_factory: DbSessionFactory, mock_config: Config, pricing, caplog
):
    def orphan() -> List[Any]:
        return [
            attestation_message(ATTESTATION),
            CreditAttestationDb(
                item_hash=ATTESTATION, period=PS, sender=ATTESTOR, status="active"
            ),
        ]

    with pytest.raises(AttestationPending):
        collect(session_factory, mock_config, orphan(), grace=False)

    inputs = collect(session_factory, mock_config, [resource(VM), *orphan()])
    assert inputs.executions == []
    assert inputs.exclusions == [
        ExclusionInput(
            kind="execution",
            business_key=f"{P}:attestation_missing",
            address=None,
            resource=None,
            node_id=None,
            reason="attestation_missing",
        )
    ]
    assert ATTESTATION in caplog.text


def test_overlapping_nodes_cap_trims_the_later_node(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    rows = [
        resource(VM),
        *attestation([interval(VM, NODE_A, 0, 40), interval(VM, NODE_B, 20, 60)]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    assert {e.node: e.seconds for e in inputs.executions} == {
        NODE_A: 2400,
        NODE_B: 1200,
    }
    assert inputs.exclusions == []


def test_unpriceable_store_next_to_billable_store_of_same_owner(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    good_hash, bad_hash = "8" * 64, "9" * 64
    expected = expected_amount(
        session_factory,
        resource(good_hash, type_=MessageType.store, spec=store_spec(30 * MiB)),
        3600,
    )
    rows = [
        resource(good_hash, type_=MessageType.store, spec=store_spec(30 * MiB)),
        resource(bad_hash, type_=MessageType.store, spec=store_spec(None)),
        *attestation([]),
    ]
    inputs = collect(session_factory, mock_config, rows)
    [storage] = inputs.storage
    assert (storage.address, storage.count, storage.size_mib) == (OWNER_KEY, 1, 30)
    assert storage.amount == expected
    assert inputs.exclusions == [
        ExclusionInput(
            kind="storage",
            business_key=f"{P}:{bad_hash}",
            address=OWNER_KEY,
            resource=bad_hash,
            node_id=None,
            reason="unpriceable",
        )
    ]


def test_topups_lowercase_checksum_sender_and_keep_chain_order(
    session_factory: DbSessionFactory, mock_config: Config, pricing
):
    sender = "0xAbCdEf0000000000000000000000000000000001"
    tx = "0x" + "22" * 32
    rows = [
        CreditTopupDb(
            chain_id=1,
            tx_hash=tx,
            log_index=log_index,
            block_number=100,
            block_time=at(5),
            from_address=sender,
            token="usdc",
            raw_amount=Decimal(1),
            quote_num=Decimal(1),
            quote_den=Decimal(1),
            credits=credits,
            status="credited",
        )
        for log_index, credits in ((10, 1), (2, 2))
    ]
    inputs = collect(session_factory, mock_config, [*rows, *attestation([])])
    assert [(t.address, t.business_key) for t in inputs.topups] == [
        (sender.lower(), f"1:{tx}:2"),
        (sender.lower(), f"1:{tx}:10"),
    ]


def test_normalize_address_handles_uppercase_prefix():
    assert normalize_address("0XABcd") == "0xabcd"
    assert normalize_address("So1AnaBase58") == "So1AnaBase58"
