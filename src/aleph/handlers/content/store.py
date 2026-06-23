"""
Content handler for STORE messages.

TODO:
- handle incentives from 3rd party
"""

import asyncio
import base64
import binascii
import datetime as dt
import logging
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Set

import aioipfs
from aleph_message.models import ItemHash, ItemType, PaymentType, StoreContent

from aleph.config import get_config
from aleph.db.accessors.cost import delete_costs_for_message
from aleph.db.accessors.files import (
    delete_file_pin,
    delete_ipns_file_pin,
    get_file,
    get_file_tag,
    get_ipns_file_pin,
    get_message_file_pin,
    insert_grace_period_file_pin,
    insert_ipns_file_pin,
    insert_message_file_pin,
    is_pinned_file,
    refresh_file_tag,
    update_ipns_file_pin,
    upsert_file,
    upsert_file_tag,
)
from aleph.db.accessors.ipns import (
    delete_ipns_record,
    get_ipns_record,
    upsert_ipns_record,
)
from aleph.db.models import MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.handlers.content.content_handler import ContentHandler
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.services.cost import (
    calculate_storage_size,
    get_payment_type,
    get_total_and_detailed_costs,
)
from aleph.services.cost_validation import validate_balance_for_payment
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.service import InvalidIpnsRecordError, IpnsResolutionError
from aleph.storage import StorageService
from aleph.toolkit.constants import MiB
from aleph.toolkit.costs import are_store_and_program_free, is_credit_only_required
from aleph.toolkit.metrics_keys import store_fetch_keys
from aleph.toolkit.timer import Timer
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.db_session import DbSession
from aleph.types.files import FileType
from aleph.types.ipns import IpnsStatus
from aleph.types.message_status import (
    FileUnavailable,
    InvalidMessageFormat,
    InvalidPaymentMethod,
    PermissionDenied,
    StoreCannotUpdateStoreWithRef,
    StoreRefNotFound,
)
from aleph.utils import item_type_from_hash, make_file_tag

LOGGER = logging.getLogger(__name__)


def _get_store_content(message: MessageDb) -> StoreContent:
    content = message.parsed_content
    if not isinstance(content, StoreContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for store message: {message.item_hash}"
        )
    return content


@dataclass
class IpfsFileStats:
    size: int
    file_type: FileType


async def _get_file_stats_from_ipfs(
    cid: str, ipfs_service: IpfsService, stat_timeout: int
) -> IpfsFileStats:
    # Stat is a storage operation: route through the storage client (which
    # the service maps to the pinning service when configured, otherwise the
    # main daemon).
    ipfs_client = ipfs_service.storage_client

    try:
        try:
            # The timeout of the aioipfs client does not seem to work, time out manually
            stats = await asyncio.wait_for(
                ipfs_client.files.stat(f"/ipfs/{cid}"),
                stat_timeout,
            )
        except aioipfs.InvalidCIDError as e:
            raise UnknownHashError(f"Invalid IPFS hash from API: '{cid}'") from e
        if stats is None:
            raise FileUnavailable(
                str(cid), "could not retrieve IPFS file stats at this time"
            )

        if stats["Type"] == "file":
            is_folder = False
            size = stats["Size"]
        else:
            is_folder = True
            # Size is 0 for folders, use cumulative size instead
            size = stats["CumulativeSize"]

        return IpfsFileStats(
            size=size, file_type=FileType.DIRECTORY if is_folder else FileType.FILE
        )

    except asyncio.TimeoutError as error:
        LOGGER.warning(
            "Timeout (%ds) while retrieving stats of hash %s: %s",
            stat_timeout,
            cid,
            getattr(error, "message", None),
        )
        raise FileUnavailable(
            str(cid),
            f"timeout {stat_timeout}s retrieving IPFS file stats: {getattr(error, 'message', None)}",
        )

    except aioipfs.APIError as error:
        LOGGER.exception(
            "Error retrieving stats of hash %s: %s",
            cid,
            getattr(error, "message", None),
        )
        raise


async def _apply_fetch_jitter(window_seconds: float, file_hash: str) -> None:
    """Wait a randomized delay before starting an IPFS fetch.

    Spreads the simultaneous fetch attempts from many CCNs receiving the same
    STORE message into a rolling wave so early fetchers can become reseeders for
    later ones before the origin's uplink is saturated. A no-op when the window
    is zero, so the behaviour is opt-in via ``ipfs.fetch_jitter_seconds``.
    """
    if window_seconds <= 0:
        return
    delay = random.uniform(0, window_seconds)
    LOGGER.info("ipfs_fetch_jitter hash=%s delay=%.2f", file_hash, delay)
    await asyncio.sleep(delay)


def _should_pin_on_ipfs(
    file_stats: IpfsFileStats,
    min_file_size_for_pinning: int,
) -> bool:
    if file_stats.file_type == FileType.DIRECTORY:
        # Always pin directories
        return True
    else:
        # Only pin on IPFS if the file size is over the threshold size
        return file_stats.size > min_file_size_for_pinning


class StoreMessageHandler(ContentHandler):
    def __init__(
        self,
        storage_service: StorageService,
        grace_period: int,
        max_unauthenticated_upload_file_size: int,
    ):
        self.storage_service = storage_service
        self.grace_period = grace_period
        self.max_unauthenticated_upload_file_size = max_unauthenticated_upload_file_size

    async def is_related_content_fetched(
        self, session: DbSession, message: MessageDb
    ) -> bool:
        content = message.parsed_content
        assert isinstance(content, StoreContent)

        if content.item_type == ItemType.ipns:
            record_db = get_ipns_record(
                session, name=content.item_hash, owner=content.address
            )
            return record_db is not None and record_db.item_hash == message.item_hash

        file_hash = content.item_hash
        return await self.storage_service.storage_engine.exists(file_hash)

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        config = get_config()

        # This check is essential to ensure that files are not added to the system
        # or the current node when the configuration disables storing of files.
        ipfs_enabled = config.ipfs.enabled.value

        content = message.parsed_content
        file_hash = content.item_hash
        item_type = content.item_type

        # Basic sanity checks
        assert isinstance(content, StoreContent)
        if item_type_from_hash(file_hash) != item_type:
            LOGGER.warning(
                "Item hash '%s' is not of the expected type ('%s')",
                file_hash,
                item_type,
            )
            raise InvalidMessageFormat(
                f"Item hash '{file_hash}' is not of the expected type ('{item_type}')"
            )

        if item_type == ItemType.ipns:
            await self._fetch_ipns(session=session, message=message, content=content)
            return

        total_key, failed_key, duration_key = store_fetch_keys(item_type)

        # For CIDs, pin directories and files > 1MiB
        if item_type == ItemType.ipfs:
            await _apply_fetch_jitter(config.ipfs.fetch_jitter_seconds.value, file_hash)
            ipfs_service = self.storage_service.ipfs_service

            file_stats = await _get_file_stats_from_ipfs(
                cid=file_hash,
                ipfs_service=ipfs_service,
                stat_timeout=config.ipfs.stat_timeout.value,
            )
            if ipfs_enabled and _should_pin_on_ipfs(
                file_stats=file_stats, min_file_size_for_pinning=1024 * 1024
            ):
                # Counted before the fetch so every attempt is represented. A crash
                # between here and the success/failure path leaves the total without a
                # matching duration or failure entry, marginally skewing the mean — an
                # acceptable tradeoff for approximate monitoring counters.
                await self.storage_service.node_cache.incr(total_key)
                try:
                    with Timer() as timer:
                        await ipfs_service.pin_add(cid=file_hash)
                except Exception:
                    await self.storage_service.node_cache.incr(failed_key)
                    LOGGER.warning(
                        "ipfs_fetch hash=%s type=ipfs path=pin size=%s duration=%.2f outcome=fail",
                        file_hash,
                        file_stats.size,
                        timer.elapsed(),
                    )
                    raise
                await self.storage_service.node_cache.incrby(
                    duration_key, round(timer.elapsed() * 1000)
                )
                LOGGER.info(
                    "ipfs_fetch hash=%s type=ipfs path=pin size=%s duration=%.2f outcome=ok",
                    file_hash,
                    file_stats.size,
                    timer.elapsed(),
                )
                upsert_file(
                    session=session,
                    file_hash=file_hash,
                    file_type=file_stats.file_type,
                    size=file_stats.size,
                )
                return

        # Otherwise, fetch content directly from the Aleph network storage API
        await self.storage_service.node_cache.incr(total_key)
        try:
            with Timer() as timer:
                file_content = await self.storage_service.get_hash_content(
                    file_hash,
                    engine=item_type,
                    tries=4,
                    timeout=15,  # We only end up here for files < 1MB, a short timeout is okay
                    use_network=True,
                    use_ipfs=True,
                    store_value=config.storage.store_files.value,
                )
        except AlephStorageException:
            await self.storage_service.node_cache.incr(failed_key)
            LOGGER.warning(
                "ipfs_fetch hash=%s type=%s path=http duration=%.2f outcome=unavailable",
                file_hash,
                item_type,
                timer.elapsed(),
            )
            raise FileUnavailable(
                file_hash, "could not retrieve file from storage at this time"
            )

        await self.storage_service.node_cache.incrby(
            duration_key, round(timer.elapsed() * 1000)
        )
        LOGGER.info(
            "ipfs_fetch hash=%s type=%s path=http size=%s duration=%.2f outcome=ok",
            file_hash,
            item_type,
            len(file_content),
            timer.elapsed(),
        )
        upsert_file(
            session=session,
            file_hash=file_hash,
            # Directories are handled above and pinned by force
            file_type=FileType.FILE,
            size=len(file_content),
        )

    async def _fetch_ipns(
        self, session: DbSession, message: MessageDb, content: StoreContent
    ) -> None:
        config = get_config()
        if not (config.ipfs.enabled.value and config.ipfs.ipns.enabled.value):
            raise FileUnavailable(
                content.item_hash, "IPNS support is disabled on this node"
            )

        ipfs_service = self.storage_service.ipfs_service
        name = content.item_hash
        owner = content.address
        if content.max_size_mib is None:
            raise InvalidMessageFormat(
                f"IPNS store message '{message.item_hash}' has no max_size_mib"
            )

        if content.ipns_record is not None:
            try:
                record = base64.b64decode(content.ipns_record, validate=True)
            except binascii.Error as e:
                raise InvalidMessageFormat(
                    f"IPNS record for '{name}' is not valid base64: {e}"
                )
        else:
            # Track-only flow: fetch the current record from the DHT.
            try:
                record = await ipfs_service.resolve_ipns_record(
                    name, timeout=config.ipfs.ipns.resolve_timeout.value
                )
            except IpnsResolutionError:
                raise FileUnavailable(
                    name, "could not resolve the IPNS record from the DHT at this time"
                )

        try:
            record_info = await ipfs_service.verify_ipns_record(record, name)
        except InvalidIpnsRecordError as e:
            raise InvalidMessageFormat(f"Invalid IPNS record for '{name}': {e}")

        if record_info.validity <= utc_now():
            raise InvalidMessageFormat(
                f"IPNS record for '{name}' is already past its end-of-life "
                f"({record_info.validity.isoformat()})"
            )

        existing = get_ipns_record(session, name=name, owner=owner)
        if (
            existing is not None
            and existing.record_sequence is not None
            and record_info.sequence <= existing.record_sequence
        ):
            # Records self-order by sequence: a stale or duplicate update is
            # an idempotent no-op, which also makes out-of-order message
            # processing safe without any chain logic.
            LOGGER.info(
                "ipns_fetch name=%s sequence=%d outcome=stale_noop",
                name,
                record_info.sequence,
            )
            return

        file_stats = await _get_file_stats_from_ipfs(
            cid=record_info.value_cid,
            ipfs_service=ipfs_service,
            stat_timeout=config.ipfs.stat_timeout.value,
        )
        if file_stats.size > content.max_size_mib * MiB:
            raise InvalidMessageFormat(
                f"IPNS content for '{name}' exceeds the paid quota "
                f"({file_stats.size} bytes > {content.max_size_mib} MiB)"
            )

        await ipfs_service.pin_add(cid=record_info.value_cid)
        upsert_file(
            session=session,
            file_hash=record_info.value_cid,
            file_type=file_stats.file_type,
            size=file_stats.size,
        )
        # Note: fetch and process commit separately. If the message is
        # rejected at process time (e.g. balance dropped between stages),
        # this row and the pin survive until the registration is updated
        # or forgotten. Accepted as a narrow, self-healing window.
        upsert_ipns_record(
            session=session,
            name=name,
            owner=owner,
            item_hash=message.item_hash,
            record=record,
            record_sequence=record_info.sequence,
            record_validity=record_info.validity,
            max_size_mib=content.max_size_mib,
            resolved_cid=record_info.value_cid,
            last_resolved=utc_now(),
            status=IpnsStatus.OK,
            created=timestamp_to_datetime(content.time),
        )
        # Keep-alive: inject the record into the DHT. Best effort, the
        # republish task retries every cycle.
        try:
            await ipfs_service.put_ipns_record(name, record)
        except Exception:
            LOGGER.warning("ipns_put name=%s outcome=fail (will retry in cycle)", name)

    async def pre_check_balance(self, session: DbSession, message: MessageDb):
        content = _get_store_content(message)
        assert isinstance(content, StoreContent)

        if are_store_and_program_free(message):
            return None

        payment_type = get_payment_type(content)

        # After the cutoff, STORE messages must use credit payment only
        if is_credit_only_required(message) and payment_type != PaymentType.credit:
            raise InvalidPaymentMethod()

        # This check is essential to ensure that files are not added to the system
        # on the current node when the configuration disables storing of files.
        config = get_config()
        ipfs_enabled = config.ipfs.enabled.value

        engine = content.item_type

        if content.item_type == ItemType.ipns:
            message_cost, _ = get_total_and_detailed_costs(
                session, content, message.item_hash
            )
            validate_balance_for_payment(
                session=session,
                address=content.address,
                message_cost=message_cost,
                payment_type=payment_type,
            )
            return None

        # Initially only do that balance pre-check for ipfs files.
        if engine == ItemType.ipfs and ipfs_enabled:
            # If we already have the file locally (e.g. from a prior add_file
            # or add_car upload on this node), use the stored size instead of
            # asking kubo. Avoids a redundant dag.get round-trip and the
            # rejection risk when the daemon is busy right after upload.
            stored_file = get_file(session, content.item_hash)
            if stored_file is not None:
                ipfs_byte_size: int | None = stored_file.size
            else:
                ipfs_byte_size = await self.storage_service.ipfs_service.get_ipfs_size(
                    content.item_hash,
                    timeout=config.ipfs.stat_timeout.value,
                    tries=3,
                )
            if ipfs_byte_size:
                storage_mib = Decimal(ipfs_byte_size / MiB)

                # Allow users to pin small files (only for hold payment type, before cutoff)
                if payment_type == PaymentType.hold and storage_mib <= (
                    self.max_unauthenticated_upload_file_size / MiB
                ):
                    return None

                computable_content_data = {
                    **content.model_dump(),
                    "estimated_size_mib": int(storage_mib),
                }
                computable_content = CostEstimationStoreContent.model_validate(
                    computable_content_data
                )

                message_cost, _ = get_total_and_detailed_costs(
                    session, computable_content, message.item_hash
                )
            else:
                message_cost = Decimal(0)
        else:
            message_cost = Decimal(0)

        validate_balance_for_payment(
            session=session,
            address=content.address,
            message_cost=message_cost,
            payment_type=payment_type,
        )

        return None

    async def check_balance(
        self, session: DbSession, message: MessageDb
    ) -> List[AccountCostsDb]:
        content = _get_store_content(message)

        message_cost, costs = get_total_and_detailed_costs(
            session, content, message.item_hash
        )

        if are_store_and_program_free(message):
            return costs

        payment_type = get_payment_type(content)

        # After the cutoff, STORE messages must use credit payment only
        if is_credit_only_required(message) and payment_type != PaymentType.credit:
            raise InvalidPaymentMethod()

        storage_size_mib = calculate_storage_size(session, content)

        # Allow users to pin small files (only for hold payment type, before cutoff)
        # IPNS registrations never get the free-file exception as quota is
        # explicitly paid via max_size_mib.
        if (
            payment_type == PaymentType.hold
            and storage_size_mib
            and storage_size_mib <= (self.max_unauthenticated_upload_file_size / MiB)
            and content.item_type != ItemType.ipns
        ):
            return costs

        validate_balance_for_payment(
            session=session,
            address=content.address,
            message_cost=message_cost,
            payment_type=payment_type,
        )

        return costs

    async def check_dependencies(self, session: DbSession, message: MessageDb) -> None:
        content = _get_store_content(message)
        if content.ref is None:
            return

        # Determine whether the ref field represents a message hash or a user-defined
        # string. If it is a user-defined string, we simply consider the file as a
        # revision. It does not matter if the original message or other revisions
        # were processed beforehand as the tag system supports out of order updates
        # and there is no way to determine which message originally defined the ref/tag.
        # On the other hand, if the ref is a message hash, we must check if the target
        # file is itself a revision of another file as we do not support revision trees.
        try:
            _ = ItemHash(content.ref)
            ref_is_hash = True
        except ValueError:
            ref_is_hash = False

        if not ref_is_hash:
            return

        ref_file_pin_db = get_message_file_pin(session=session, item_hash=content.ref)

        if ref_file_pin_db is None:
            raise StoreRefNotFound(content.ref)

        if ref_file_pin_db.ref is not None:
            raise StoreCannotUpdateStoreWithRef()

    async def check_permissions(self, session: DbSession, message: MessageDb):
        await super().check_permissions(session=session, message=message)
        content = _get_store_content(message)
        if content.ref is None:
            return

        owner = content.address
        file_tag = make_file_tag(
            owner=owner, ref=content.ref, item_hash=message.item_hash
        )
        file_tag_db = get_file_tag(session=session, tag=file_tag)

        if not file_tag_db:
            return

        if file_tag_db.owner != owner:
            raise PermissionDenied(
                f"{message.item_hash} attempts to update a file tag belonging to another user"
            )

    async def _pin_and_tag_file(self, session: DbSession, message: MessageDb):
        content = _get_store_content(message)

        file_hash = content.item_hash
        owner = content.address

        insert_message_file_pin(
            session=session,
            file_hash=file_hash,
            owner=owner,
            item_hash=message.item_hash,
            ref=content.ref,
            created=timestamp_to_datetime(content.time),
        )

        file_tag = make_file_tag(
            owner=content.address, ref=content.ref, item_hash=message.item_hash
        )
        upsert_file_tag(
            session=session,
            tag=file_tag,
            owner=owner,
            file_hash=file_hash,
            last_updated=timestamp_to_datetime(content.time),
        )

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            content = _get_store_content(message)
            if content.item_type == ItemType.ipns:
                await self._process_ipns(session=session, message=message)
            else:
                await self._pin_and_tag_file(session=session, message=message)

    async def _process_ipns(self, session: DbSession, message: MessageDb) -> None:
        content = _get_store_content(message)
        name = content.item_hash
        owner = content.address

        record_db = get_ipns_record(session, name=name, owner=owner)
        if record_db is None or record_db.item_hash != message.item_hash:
            # This message lost the sequence race (stale update): no pin
            # change, and it must not bill either, so drop the cost rows
            # inserted for it earlier in this transaction.
            delete_costs_for_message(session=session, item_hash=message.item_hash)
            return
        assert record_db.resolved_cid is not None

        pin = get_ipns_file_pin(session, name=name, owner=owner)
        if pin is None:
            insert_ipns_file_pin(
                session=session,
                file_hash=record_db.resolved_cid,
                owner=owner,
                item_hash=message.item_hash,
                name=name,
                created=timestamp_to_datetime(content.time),
            )
            return

        old_file_hash = pin.file_hash
        old_item_hash = pin.item_hash
        update_ipns_file_pin(
            session=session,
            name=name,
            owner=owner,
            file_hash=record_db.resolved_cid,
            item_hash=message.item_hash,
        )
        if old_item_hash and old_item_hash != message.item_hash:
            # One logical registration carries one cost line: the superseded
            # message must stop billing once the update takes over.
            delete_costs_for_message(session=session, item_hash=old_item_hash)
        if old_file_hash != record_db.resolved_cid:
            await self._check_remaining_pins(
                session=session,
                storage_hash=old_file_hash,
                storage_type=ItemType.ipfs,
            )

    async def _check_remaining_pins(
        self, session: DbSession, storage_hash: str, storage_type: ItemType
    ):
        """
        If a file is not pinned anymore, mark it as pickable by the garbage collector.

        We do not delete files directly from the message processor for two reasons:
        1. Performance (deleting large files is slow)
        2. Give the users some time to react if a file gets unpinned.

        If a file is not pinned by a TX or message, we give it a grace period pin.
        """
        LOGGER.debug(f"Garbage collecting {storage_hash}")

        if is_pinned_file(session=session, file_hash=storage_hash):
            LOGGER.debug(f"File {storage_hash} has at least one reference left")
            return

        # Unpin the file from IPFS or remove it from local storage
        try:
            storage_detected: ItemType = item_type_from_hash(storage_hash)
        except UnknownHashError:
            # IPNS records may point at CID shapes the STORE shape matcher
            # does not recognize (e.g. raw-codec bafk... CIDv1). The hash
            # still designates IPFS content; skip the cross-check.
            storage_detected = storage_type

        if storage_type != storage_detected:
            raise ValueError(
                f"Inconsistent ItemType {storage_type} != {storage_detected} "
                f"for hash '{storage_hash}'"
            )

        LOGGER.info("Inserting grace period pin for %s", storage_hash)

        current_datetime = utc_now()
        delete_by = current_datetime + dt.timedelta(hours=self.grace_period)
        insert_grace_period_file_pin(
            session=session,
            file_hash=storage_hash,
            created=utc_now(),
            delete_by=delete_by,
        )

    async def _forget_ipns(self, session: DbSession, message: MessageDb) -> None:
        content = _get_store_content(message)
        name = content.item_hash
        owner = content.address

        record_db = get_ipns_record(session, name=name, owner=owner)
        if record_db is None or record_db.item_hash != message.item_hash:
            # Superseded registration message: nothing to tear down.
            return

        pin = get_ipns_file_pin(session, name=name, owner=owner)
        delete_ipns_record(session, name=name, owner=owner)
        if pin is not None:
            pinned_hash = pin.file_hash
            delete_ipns_file_pin(session=session, name=name, owner=owner)
            await self._check_remaining_pins(
                session=session,
                storage_hash=pinned_hash,
                storage_type=ItemType.ipfs,
            )

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = _get_store_content(message)

        if content.item_type == ItemType.ipns:
            await self._forget_ipns(session=session, message=message)
            return set()

        delete_file_pin(session=session, item_hash=message.item_hash)
        refresh_file_tag(
            session=session,
            tag=make_file_tag(
                owner=content.address,
                ref=content.ref,
                item_hash=message.item_hash,
            ),
        )
        await self._check_remaining_pins(
            session=session,
            storage_hash=content.item_hash,
            storage_type=content.item_type,
        )

        return set()
