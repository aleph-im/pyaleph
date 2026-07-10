import logging
from typing import List, Set

from aleph_message.models import PaymentType, VerifiableProgramContent

from aleph.db.accessors.vms import delete_vm
from aleph.db.models import MessageDb, VProgramDb, VProgramVerifiedVolumeDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.services.cost import get_payment_type, get_total_and_detailed_costs
from aleph.services.cost_validation import validate_balance_for_payment
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidMessageFormat, InvalidPaymentMethod

LOGGER = logging.getLogger(__name__)


def _get_vprogram_content(message: MessageDb) -> VerifiableProgramContent:
    content = message.parsed_content
    if not isinstance(content, VerifiableProgramContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for V-PROGRAM message: {message.item_hash}"
        )
    return content


def vprogram_message_to_db(message: MessageDb) -> VProgramDb:
    content = _get_vprogram_content(message)

    cpu_architecture = None
    cpu_vendor = None
    node_owner = None
    node_address_regex = None
    node_hash = None

    if content.requirements:
        if cpu := content.requirements.cpu:
            cpu_architecture = cpu.architecture
            cpu_vendor = cpu.vendor

        if node := content.requirements.node:
            node_owner = node.owner
            node_address_regex = node.address_regex
            node_hash = node.node_hash

    return VProgramDb(
        item_hash=message.item_hash,
        owner=content.address,
        # The schema rejects allow_amend/replaces (V-Programs are immutable);
        # map them anyway rather than hardcoding the invariant here.
        allow_amend=content.allow_amend,
        replaces=content.replaces,
        metadata_=content.metadata,
        # variables and authorized_keys are rejected by the schema
        # (unmeasured host-to-guest channels).
        variables=content.variables,
        authorized_keys=None,
        message_triggers=None,
        environment_reproducible=False,
        environment_internet=content.environment.internet,
        environment_aleph_api=False,
        environment_shared_cache=False,
        # The verification block (SNP policy, launch measurements) stays in
        # the message content: the SNP policy is 64-bit and does not fit the
        # int4 column that holds the SEV policy of instances.
        environment_trusted_execution_policy=None,
        environment_trusted_execution_firmware=None,
        resources_vcpus=content.resources.vcpus,
        resources_memory=content.resources.memory,
        resources_seconds=content.resources.seconds,
        cpu_architecture=cpu_architecture,
        cpu_vendor=cpu_vendor,
        node_owner=node_owner,
        node_address_regex=node_address_regex,
        node_hash=node_hash,
        payment_type=content.payment.type,
        created=timestamp_to_datetime(content.time),
        volumes=[],
        runtime_ref=str(content.runtime.ref),
        runtime_comment=content.runtime.comment,
        workload_ref=str(content.workload.ref),
        workload_hash_tree=str(content.workload.hash_tree),
        workload_roothash=content.workload.roothash,
        verified_volumes=[
            VProgramVerifiedVolumeDb(
                position=position,
                ref=str(volume.ref),
                hash_tree=str(volume.hash_tree),
                roothash=volume.roothash,
                comment=volume.comment,
            )
            for position, volume in enumerate(content.volumes)
        ],
    )


class VProgramMessageHandler(ContentHandler):
    """Handles V-PROGRAM (verifiable program) messages.

    Validates the credit balance, persists costs, and maintains the vms
    representation: a VProgramDb row plus verified volume rows, so that
    forgetting a STORE file referenced by a V-Program (runtime manifest,
    workload image, hash trees, verified volumes) is blocked by the
    forget handler's dependent-volumes check. Measurement validation and
    CRN dispatch are later phases.
    """

    async def check_balance(
        self, session: DbSession, message: MessageDb
    ) -> List[AccountCostsDb]:
        content = _get_vprogram_content(message)

        message_cost, costs = get_total_and_detailed_costs(
            session, content, message.item_hash
        )

        # The schema already restricts V-Programs to credit payment; keep
        # the pipeline error explicit in case the model ever loosens.
        payment_type = get_payment_type(content)
        if payment_type != PaymentType.credit:
            raise InvalidPaymentMethod()

        validate_balance_for_payment(
            session=session,
            address=content.address,
            message_cost=message_cost,
            payment_type=payment_type,
        )

        return costs

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            # No vm_versions row: V-Programs are immutable, there is no
            # amend chain to track.
            session.add(vprogram_message_to_db(message))

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        LOGGER.debug("Deleting v-program %s...", message.item_hash)

        # Verified volume rows follow via ON DELETE CASCADE. V-Programs are
        # immutable, so there are no updates to forget and no version table
        # to refresh. Costs are removed by the generic forget path.
        delete_vm(session=session, vm_hash=message.item_hash)

        return set()
