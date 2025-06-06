"""
Content handler for STORE messages.

TODO:
- handle incentives from 3rd party
"""

import asyncio
import datetime as dt
import logging
from decimal import Decimal
from typing import List, Set

import aioipfs
from aleph_message.models import ItemHash, ItemType, StoreContent

from aleph.config import get_config
from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import (
    delete_file_pin,
    get_file_tag,
    get_message_file_pin,
    insert_grace_period_file_pin,
    insert_message_file_pin,
    is_pinned_file,
    refresh_file_tag,
    upsert_file,
    upsert_file_tag,
)
from aleph.db.models import MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.handlers.content.content_handler import ContentHandler
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.services.cost import calculate_storage_size, get_total_and_detailed_costs
from aleph.storage import StorageService
from aleph.toolkit.constants import MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE, MiB
from aleph.toolkit.costs import are_store_and_program_free
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.db_session import DbSession
from aleph.types.files import FileType
from aleph.types.message_status import (
    FileUnavailable,
    InsufficientBalanceException,
    InvalidMessageFormat,
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


class StoreMessageHandler(ContentHandler):
    def __init__(self, storage_service: StorageService, grace_period: int):
        self.storage_service = storage_service
        self.grace_period = grace_period

    async def is_related_content_fetched(
        self, session: DbSession, message: MessageDb
    ) -> bool:
        content = message.parsed_content
        assert isinstance(content, StoreContent)

        file_hash = content.item_hash
        return await self.storage_service.storage_engine.exists(file_hash)

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        # TODO: simplify this function, it's overly complicated for no good reason.

        # This check is essential to ensure that files are not added to the system
        # or the current node when the configuration disables storing of files.
        config = get_config()
        content = message.parsed_content
        assert isinstance(content, StoreContent)

        engine = content.item_type

        is_folder = False
        item_hash = content.item_hash

        ipfs_enabled = config.ipfs.enabled.value
        do_standard_lookup = True

        # Sentinel value, the code below always sets a value but mypy does not see it.
        # otherwise if config.storage.store_files is False, this will be the database value
        size: int = -1

        if engine == ItemType.ipfs and ipfs_enabled:
            if item_type_from_hash(item_hash) != ItemType.ipfs:
                LOGGER.warning("Invalid IPFS hash: '%s'", item_hash)
                raise InvalidMessageFormat(
                    f"Item hash is not an IPFS CID: '{item_hash}'"
                )

            ipfs_service = self.storage_service.ipfs_service
            ipfs_client = ipfs_service.ipfs_client

            try:
                try:
                    # The timeout of the aioipfs client does not seem to work, time out manually
                    stats = await asyncio.wait_for(
                        ipfs_client.files.stat(f"/ipfs/{item_hash}"), 5
                    )
                except aioipfs.InvalidCIDError as e:
                    raise UnknownHashError(
                        f"Invalid IPFS hash from API: '{item_hash}'"
                    ) from e
                if stats is None:
                    raise FileUnavailable(
                        "Could not retrieve IPFS content at this time"
                    )

                if stats["Type"] == "file":
                    is_folder = False
                    size = stats["Size"]
                    do_standard_lookup = size < 1024**2 and len(item_hash) == 46
                else:
                    is_folder = True
                    # Size is 0 for folders, use cumulative size instead
                    size = stats["CumulativeSize"]
                    do_standard_lookup = False

                # Pin folders and files larger than 1MB
                if not do_standard_lookup:
                    await ipfs_service.pin_add(cid=item_hash)

            except asyncio.TimeoutError as error:
                LOGGER.warning(
                    f"Timeout while retrieving stats of hash {item_hash}: {getattr(error, 'message', None)}"
                )
                do_standard_lookup = True

            except aioipfs.APIError as error:
                LOGGER.exception(
                    f"Error retrieving stats of hash {item_hash}: {getattr(error, 'message', None)}"
                )
                do_standard_lookup = True

        if do_standard_lookup:
            if config.storage.store_files.value:
                try:
                    file_content = await self.storage_service.get_hash_content(
                        item_hash,
                        engine=engine,
                        tries=4,
                        timeout=15,  # We only end up here for files < 1MB, a short timeout is okay
                        use_network=True,
                        use_ipfs=True,
                        store_value=True,
                    )
                except AlephStorageException:
                    raise FileUnavailable(
                        "Could not retrieve file from storage at this time"
                    )

                size = len(file_content)
            else:
                size = -1

        upsert_file(
            session=session,
            file_hash=item_hash,
            file_type=FileType.DIRECTORY if is_folder else FileType.FILE,
            size=size,
        )

    async def pre_check_balance(self, session: DbSession, message: MessageDb):
        content = _get_store_content(message)
        assert isinstance(content, StoreContent)

        if are_store_and_program_free(message):
            return None

        # This check is essential to ensure that files are not added to the system
        # on the current node when the configuration disables storing of files.
        config = get_config()
        ipfs_enabled = config.ipfs.enabled.value

        current_balance = get_total_balance(session=session, address=content.address)
        current_cost = get_total_cost_for_address(
            session=session, address=content.address
        )

        engine = content.item_type
        # Initially only do that balance pre-check for ipfs files.
        if engine == ItemType.ipfs and ipfs_enabled:
            ipfs_byte_size = await self.storage_service.ipfs_service.get_ipfs_size(
                content.item_hash
            )
            if ipfs_byte_size:
                storage_mib = Decimal(ipfs_byte_size / MiB)

                # Allow users to pin small files
                if storage_mib and storage_mib <= (
                    MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE / MiB
                ):
                    LOGGER.debug(
                        f"Cost for {message.item_hash} supposed to be free as size is {storage_mib}"
                    )
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

        required_balance = current_cost + message_cost

        if current_balance < required_balance:
            raise InsufficientBalanceException(
                balance=current_balance,
                required_balance=required_balance,
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

        storage_size_mib = calculate_storage_size(session, content)

        if storage_size_mib and storage_size_mib <= (
            MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE / MiB
        ):
            return costs

        current_balance = get_total_balance(address=content.address, session=session)
        current_cost = get_total_cost_for_address(
            session=session, address=content.address
        )

        required_balance = current_cost + message_cost

        if current_balance < required_balance:
            raise InsufficientBalanceException(
                balance=current_balance,
                required_balance=required_balance,
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
            await self._pin_and_tag_file(session=session, message=message)

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
        storage_detected: ItemType = item_type_from_hash(storage_hash)

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

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = _get_store_content(message)

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
