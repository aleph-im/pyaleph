"""
Collects the inputs of one hourly period from the database: top-ups, credit
transfers, the execution attestation and credit-paid storage, as the
`PeriodInputs` the fold core (`fold.apply_period`) consumes.

Every interval, VM or file this module decides not to bill becomes an
`ExclusionInput` (spec 5.8): there is no silent skip. Every list is sorted
explicitly and the wall clock is never read, so all CCNs build the same inputs
from the same database.

Addresses: EVM addresses (0x-prefixed) are case-insensitive and are lowercased
so that lots credited by a top-up (indexed lowercase) are the ones drained by
the same account's executions. Other address formats are case-sensitive and
kept as given.

Transfers and attestations are read from live rows (`messages`, `posts`).
This relies on an ingest invariant (Task 10): a FORGET of an
`aleph_credit_transfer` post, or of an attestation post from a whitelisted
attestor, is refused, so the rows a period was folded from never disappear
and every CCN refolds a period from the same inputs.
"""

import datetime as dt
import logging
import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from aleph_message.models import MessageType, PaymentType
from configmanager import Config
from pydantic import ValidationError
from sqlalchemy import select

from aleph.db.accessors.credit_ledger import (
    active_attestation,
    get_billing_resource,
    iter_live_billing_resources,
    topups_for_period,
)
from aleph.db.models.credit_ledger import BillingResourceDb
from aleph.db.models.messages import MessageDb
from aleph.db.models.posts import PostDb
from aleph.schemas.credit_transfer import CreditTransferContent
from aleph.schemas.execution_intervals import ExecutionIntervalsContent, IntervalEntry
from aleph.services.credits.cost_spec import CostSpecError
from aleph.services.credits.fold import (
    ExclusionInput,
    ExecutionInput,
    PeriodInputs,
    StorageInput,
    TopupInput,
    TransferInput,
    execution_amount,
)
from aleph.services.credits.periods import floor_hour, period_end
from aleph.services.credits.rates import PricingVersion, rate_for
from aleph.services.pricing_utils import pricing_model_at
from aleph.toolkit.constants import MiB
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageStatus

LOGGER = logging.getLogger(__name__)

TRANSFER_POST_TYPE = "aleph_credit_transfer"
MAX_SECONDS_PER_VM = 3600
ONE_SECOND = dt.timedelta(seconds=1)

EXECUTABLE_TYPES = (MessageType.instance, MessageType.program, MessageType.v_program)

# Exclusion reasons written by this module (spec 5.8).
UNKNOWN_MESSAGE = "unknown_message"
ON_DEMAND_PROGRAM = "on_demand_program"
PAYMENT_STREAM = "payment_stream"
BEFORE_CONFIRMATION = "before_confirmation"
AFTER_END_OF_LIFE = "after_end_of_life"
OVER_CAP = "over_cap"
NO_ATTESTATION = "no_attestation"
ATTESTATION_MISSING = "attestation_missing"
UNPRICEABLE = "unpriceable"
MALFORMED = "malformed"


class AttestationPending(Exception):
    """
    No active attestation for the period and its grace has not elapsed: the
    period cannot be folded yet. The caller waits and retries.
    """

    def __init__(self, period_start: dt.datetime):
        super().__init__(f"no attestation yet for period {period_start.isoformat()}")
        self.period_start = period_start


def normalize_address(address: str) -> str:
    """Ledger form of an address: EVM (0x) addresses lowercased, others as is."""
    return address.lower() if address[:2].lower() == "0x" else address


def _seconds(start: dt.datetime, end: dt.datetime) -> int:
    """Whole seconds in [start, end), floored; 0 when the window is empty."""
    if end <= start:
        return 0
    return (end - start) // ONE_SECOND


def _utc(timestamp: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)


# --- top-ups ------------------------------------------------------------------


def topup_inputs(
    session: DbSession, period_start: dt.datetime, end: dt.datetime
) -> List[TopupInput]:
    inputs: List[TopupInput] = []
    for row in topups_for_period(session, period_start, end):
        business_key = f"{row.chain_id}:{row.tx_hash}:{row.log_index}"
        if row.credits is None:
            # status `credited` always carries credits; anything else is a bug.
            raise ValueError(f"credited top-up {business_key} has no credits")
        inputs.append(
            TopupInput(
                address=normalize_address(row.from_address),
                credits=int(row.credits),
                business_key=business_key,
                source_ref=row.tx_hash,
            )
        )
    # Kept in the accessor's (block_time, tx_hash, log_index) order; the fold
    # orders top-ups itself.
    return inputs


# --- transfers ----------------------------------------------------------------


def transfer_inputs(
    session: DbSession,
    period_start: dt.datetime,
    end: dt.datetime,
    channels: List[str],
) -> Tuple[List[TransferInput], List[ExclusionInput]]:
    """
    Credit transfers confirmed in [period_start, end), in confirmation then
    item hash order (the order the fold applies them in).

    A processed transfer whose content does not parse (the post handler only
    validates a truthy `content`) becomes a `malformed` transfer_out
    exclusion: this layer never raises on user content.
    """
    stmt = select(MessageDb).where(
        MessageDb.type == MessageType.post,
        MessageDb.content_type == TRANSFER_POST_TYPE,
        MessageDb.status_value == MessageStatus.PROCESSED,
        MessageDb.first_confirmed_at >= period_start,
        MessageDb.first_confirmed_at < end,
    )
    if channels:
        stmt = stmt.where(MessageDb.channel.in_(channels))
    messages = session.execute(stmt).scalars().all()

    inputs: List[TransferInput] = []
    exclusions: List[ExclusionInput] = []
    for message in sorted(messages, key=lambda m: (m.first_confirmed_at, m.item_hash)):
        raw_sender = (
            message.content.get("address")
            if isinstance(message.content, dict)
            else None
        )
        sender = (
            normalize_address(raw_sender)
            if isinstance(raw_sender, str) and raw_sender
            else None
        )
        try:
            if sender is None:
                raise ValueError("transfer has no sender address")
            content = CreditTransferContent.model_validate(message.content["content"])
            entries: List[Tuple[str, int, Optional[dt.datetime]]] = [
                (
                    normalize_address(entry.address),
                    entry.amount,
                    (
                        None
                        if entry.expiration is None
                        else dt.datetime.fromtimestamp(
                            entry.expiration / 1000, tz=dt.timezone.utc
                        )
                    ),
                )
                for entry in content.transfer.credits
            ]
        except (
            ValidationError,
            KeyError,
            TypeError,
            ValueError,
            OverflowError,
            OSError,
        ) as error:
            LOGGER.warning(
                "credit transfer %s is malformed, excluded: %s",
                message.item_hash,
                error,
            )
            exclusions.append(
                ExclusionInput(
                    kind="transfer_out",
                    business_key=f"{message.item_hash}:0",
                    address=sender,
                    resource=message.item_hash,
                    node_id=None,
                    reason=MALFORMED,
                )
            )
            continue
        inputs.append(
            TransferInput(
                message_hash=message.item_hash, sender=sender, entries=entries
            )
        )
    return inputs, exclusions


# --- executions ---------------------------------------------------------------


@dataclass
class _NodeUsage:
    """Seconds of one (vm, node) in the period, summed over its intervals."""

    node: str
    first_start: dt.datetime
    seconds: int = 0
    # Why the intervals that contributed nothing did so, in interval order.
    empty_reasons: List[str] = field(default_factory=list)


def _is_on_demand_program(resource: BillingResourceDb) -> bool:
    if resource.type != MessageType.program:
        return False
    content = resource.cost_spec.get("content") or {}
    on = content.get("on") or {}
    return not bool(on.get("persistent", False))


def _resource_exclusion(resource: Optional[BillingResourceDb]) -> Optional[str]:
    """Reason a metered VM is not billed at all, whatever its intervals."""
    if resource is None or resource.type not in EXECUTABLE_TYPES:
        return UNKNOWN_MESSAGE
    if _is_on_demand_program(resource):
        return ON_DEMAND_PROGRAM
    if resource.payment_type == PaymentType.superfluid:
        return PAYMENT_STREAM
    return None


def _clamp(
    interval: IntervalEntry,
    resource: BillingResourceDb,
    period_start: dt.datetime,
    end: dt.datetime,
) -> Tuple[dt.datetime, int, Optional[str]]:
    """
    (clamped start, seconds, reason when it contributes nothing) of `interval`
    clamped to the period and to [first_confirmed_at, end_of_life).
    """
    confirmed = resource.first_confirmed_at
    end_of_life = resource.end_of_life
    if confirmed is None or confirmed >= end:
        return period_start, 0, BEFORE_CONFIRMATION
    if end_of_life is not None and end_of_life <= period_start:
        return period_start, 0, AFTER_END_OF_LIFE

    interval_start = max(_utc(interval.start), period_start)
    interval_end = min(_utc(interval.end), end)
    lo = max(interval_start, confirmed)
    hi = interval_end if end_of_life is None else min(interval_end, end_of_life)
    seconds = _seconds(lo, hi)
    if seconds > 0:
        return lo, seconds, None
    # The interval itself is at least one second long, so a bound emptied it.
    reason = BEFORE_CONFIRMATION if confirmed > interval_start else AFTER_END_OF_LIFE
    return lo, 0, reason


def _cap(usages: List[_NodeUsage]) -> List[Tuple[_NodeUsage, int]]:
    """
    Billed seconds per node, at most MAX_SECONDS_PER_VM for the VM: nodes are
    kept in order of their first interval start (node id breaks ties) and the
    later-starting ones are trimmed first.
    """
    remaining = MAX_SECONDS_PER_VM
    capped: List[Tuple[_NodeUsage, int]] = []
    for usage in sorted(usages, key=lambda u: (u.first_start, u.node)):
        billed = min(usage.seconds, remaining)
        remaining -= billed
        capped.append((usage, billed))
    return capped


@dataclass
class _ExecutionCollector:
    session: DbSession
    period_start: dt.datetime
    end: dt.datetime
    pricing: PricingVersion
    executions: List[ExecutionInput] = field(default_factory=list)
    exclusions: List[ExclusionInput] = field(default_factory=list)

    @property
    def period_unix(self) -> int:
        return int(self.period_start.timestamp())

    def exclude(self, vm: str, node: str, owner: Optional[str], reason: str) -> None:
        self.exclusions.append(
            ExclusionInput(
                kind="execution",
                business_key=f"{self.period_unix}:{vm}:{node}",
                address=None if owner is None else normalize_address(owner),
                resource=vm,
                node_id=node,
                reason=reason,
            )
        )

    def collect_vm(self, vm: str, intervals: List[IntervalEntry]) -> None:
        """All the intervals of one VM, sorted by (node, start)."""
        nodes = sorted({i.node for i in intervals})
        resource = get_billing_resource(self.session, vm)
        reason = _resource_exclusion(resource)
        if resource is None or reason is not None:
            owner = None if resource is None else resource.owner
            for node in nodes:
                self.exclude(vm, node, owner, reason or UNKNOWN_MESSAGE)
            return

        usages: Dict[str, _NodeUsage] = {}
        for interval in intervals:
            start, seconds, empty_reason = _clamp(
                interval, resource, self.period_start, self.end
            )
            usage = usages.get(interval.node)
            if usage is None:
                usage = _NodeUsage(node=interval.node, first_start=start)
                usages[interval.node] = usage
            if seconds > 0:
                if usage.seconds == 0:
                    usage.first_start = start
                usage.seconds += seconds
            elif empty_reason is not None:
                usage.empty_reasons.append(empty_reason)

        billable: List[Tuple[str, int]] = []
        for node in nodes:
            usage = usages[node]
            if usage.seconds == 0:
                self.exclude(vm, node, resource.owner, usage.empty_reasons[0])
        for usage, billed in _cap([usages[n] for n in nodes if usages[n].seconds]):
            if billed == 0:
                self.exclude(vm, usage.node, resource.owner, OVER_CAP)
            else:
                billable.append((usage.node, billed))
        if not billable:
            return

        try:
            rate: Decimal = rate_for(
                self.session, resource, self.period_start, pricing=self.pricing
            )
        except CostSpecError:
            for node, _ in billable:
                self.exclude(vm, node, resource.owner, UNPRICEABLE)
            return

        for node, billed in sorted(billable):
            self.executions.append(
                ExecutionInput(
                    vm=vm,
                    node=node,
                    owner=normalize_address(resource.owner),
                    payment_type=str(resource.payment_type.value),
                    seconds=billed,
                    rate=rate,
                )
            )


def _attestation_content(
    session: DbSession, item_hash: str
) -> Optional[ExecutionIntervalsContent]:
    """Content of the attestation post, or None when its post row is gone."""
    content = session.execute(
        select(PostDb.content).where(PostDb.item_hash == item_hash)
    ).scalar_one_or_none()
    if content is None:
        LOGGER.warning(
            "active attestation %s has no post row: treated as no attestation",
            item_hash,
        )
        return None
    return ExecutionIntervalsContent.model_validate(content)


def _is_metered_kind(resource: BillingResourceDb) -> bool:
    """Credit-paid VMs the attestation is expected to cover."""
    return resource.type in EXECUTABLE_TYPES and not _is_on_demand_program(resource)


def execution_inputs(
    session: DbSession,
    period_start: dt.datetime,
    end: dt.datetime,
    pricing: PricingVersion,
    *,
    attestation_grace_elapsed: bool,
) -> Tuple[List[ExecutionInput], List[ExclusionInput]]:
    period_unix = int(period_start.timestamp())
    attestation = active_attestation(session, period_start)
    content = (
        None
        if attestation is None
        else _attestation_content(session, attestation.item_hash)
    )
    if content is None:
        if not attestation_grace_elapsed:
            raise AttestationPending(period_start)
        return [], [
            ExclusionInput(
                kind="execution",
                business_key=f"{period_unix}:{ATTESTATION_MISSING}",
                address=None,
                resource=None,
                node_id=None,
                reason=ATTESTATION_MISSING,
            )
        ]

    intervals = sorted(content.intervals, key=lambda i: (i.vm, i.node, i.start, i.end))
    by_vm: Dict[str, List[IntervalEntry]] = {}
    for interval in intervals:
        by_vm.setdefault(interval.vm, []).append(interval)

    collector = _ExecutionCollector(session, period_start, end, pricing)
    for vm in sorted(by_vm):
        collector.collect_vm(vm, by_vm[vm])

    attested: Set[str] = set(by_vm)
    for resource in iter_live_billing_resources(
        session, None, PaymentType.credit, period_start, end
    ):
        if not _is_metered_kind(resource) or resource.item_hash in attested:
            continue
        collector.exclusions.append(
            ExclusionInput(
                kind="execution",
                business_key=f"{period_unix}:{resource.item_hash}:",
                address=normalize_address(resource.owner),
                resource=resource.item_hash,
                node_id=None,
                reason=NO_ATTESTATION,
            )
        )

    return collector.executions, collector.exclusions


# --- storage ------------------------------------------------------------------


@dataclass
class _StorageTotal:
    amount: int = 0
    count: int = 0
    size_mib: int = 0
    seconds: int = 0


def storage_inputs(
    session: DbSession,
    period_start: dt.datetime,
    end: dt.datetime,
    pricing: PricingVersion,
) -> Tuple[List[StorageInput], List[ExclusionInput]]:
    """
    One StorageInput per owner of credit-paid STOREs alive in the period. A
    STORE whose end of life is provisional (local removal) is billed up to it.
    """
    period_unix = int(period_start.timestamp())
    totals: Dict[str, _StorageTotal] = {}
    exclusions: List[ExclusionInput] = []

    def exclude(resource: BillingResourceDb, reason: str) -> None:
        exclusions.append(
            ExclusionInput(
                kind="storage",
                business_key=f"{period_unix}:{resource.item_hash}",
                address=normalize_address(resource.owner),
                resource=resource.item_hash,
                node_id=None,
                reason=reason,
            )
        )

    resources = iter_live_billing_resources(
        session, MessageType.store, PaymentType.credit, period_start, end
    )
    for resource in sorted(resources, key=lambda r: r.item_hash):
        if resource.first_confirmed_at is None:
            # iter_live_billing_resources only returns confirmed resources.
            raise RuntimeError(f"live STORE {resource.item_hash} is unconfirmed")
        lo = max(resource.first_confirmed_at, period_start)
        hi = end if resource.end_of_life is None else min(resource.end_of_life, end)
        if hi <= lo:
            # Only possible for an end of life before the confirmation.
            exclude(resource, AFTER_END_OF_LIFE)
            continue
        seconds = _seconds(lo, hi)
        try:
            rate = rate_for(session, resource, period_start, pricing=pricing)
        except CostSpecError:
            exclude(resource, UNPRICEABLE)
            continue
        size_bytes = int(resource.cost_spec["file_size_bytes"])
        total = totals.setdefault(normalize_address(resource.owner), _StorageTotal())
        total.amount += execution_amount(rate, seconds)
        total.count += 1
        total.size_mib += math.ceil(size_bytes / MiB)
        total.seconds += seconds

    inputs = [
        StorageInput(
            address=address,
            amount=total.amount,
            count=total.count,
            size_mib=total.size_mib,
            seconds=total.seconds,
        )
        for address, total in sorted(totals.items())
    ]
    return inputs, exclusions


# --- entry point --------------------------------------------------------------


def collect_inputs(
    session: DbSession,
    period_start: dt.datetime,
    settings: Config,
    *,
    attestation_grace_elapsed: bool,
) -> PeriodInputs:
    """
    Inputs of the period starting at `period_start`. Raises AttestationPending
    when the period has no active attestation yet and its grace has not
    elapsed. `settings` is the node config (credit transfer channels).
    """
    assert period_start == floor_hour(period_start), "period_start must be an hour"
    end = period_end(period_start)
    pricing = pricing_model_at(session, period_start)

    executions, execution_exclusions = execution_inputs(
        session,
        period_start,
        end,
        pricing,
        attestation_grace_elapsed=attestation_grace_elapsed,
    )
    storage, storage_exclusions = storage_inputs(session, period_start, end, pricing)
    channels: List[Any] = list(settings.aleph.credit_balances.channels.value or [])
    transfers, transfer_exclusions = transfer_inputs(
        session, period_start, end, channels
    )

    return PeriodInputs(
        topups=topup_inputs(session, period_start, end),
        transfers=transfers,
        executions=executions,
        storage=storage,
        exclusions=transfer_exclusions + execution_exclusions + storage_exclusions,
    )
