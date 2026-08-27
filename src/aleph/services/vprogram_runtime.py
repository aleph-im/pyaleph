"""
Resolution of a V-PROGRAM's runtime bundle from its manifest.

The message only references the runtime *manifest* (a STORE of JSON). The
manifest names the bundle tarball by `bundle.ref`, the STORE message hash
that both the SDK and the CRN download. The bundle is the bulk of a
V-PROGRAM's disk footprint, so pricing and dependency checks need its ref;
its size is then measured from the pinned file, never from the manifest's
own `bundle.size`, which is user-controlled.
"""

import json
from typing import Literal, TypeAlias, get_args

from aleph_message.models import ItemHash
from pydantic import BaseModel, ConfigDict, ValidationError

from aleph.db.accessors.files import get_message_file_pin
from aleph.exceptions import (
    AlephStorageException,
    ContentCurrentlyUnavailable,
    UnknownHashError,
)
from aleph.storage import StorageService
from aleph.toolkit.constants import MiB
from aleph.types.cost import CostType, RefVolume
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidVProgramRuntime, VmVolumeNotFound
from aleph.utils import item_type_from_hash

# Single source for the manifest format tag: the model validates against it,
# the rejection message names it.
RuntimeManifestFormat: TypeAlias = Literal["aleph-vprogram-runtime"]
RUNTIME_MANIFEST_FORMAT: str = get_args(RuntimeManifestFormat)[0]

# A runtime manifest is a small JSON document (a handful of KiB in practice).
# The pinned file size is known before the bytes are fetched, so cap it: a
# multi-GiB "manifest" would otherwise be downloaded and JSON-parsed in full.
MAX_RUNTIME_MANIFEST_SIZE = 1 * MiB


class RuntimeBundleRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # Any item hash the network can address, including IPFS CIDs: the bundle
    # is a STORE message like any other.
    ref: ItemHash


class RuntimeManifestBundle(BaseModel):
    """The two manifest fields the CCN needs. Everything else is ignored:
    the CRN validates the full schema."""

    model_config = ConfigDict(extra="ignore")

    format: RuntimeManifestFormat
    bundle: RuntimeBundleRef


async def resolve_runtime_bundle_ref(
    session: DbSession, storage_service: StorageService, runtime_ref: str
) -> str:
    """Return the STORE message hash of the bundle named by the manifest at
    `runtime_ref`.

    Raises InvalidVProgramRuntime when the manifest is not pinned, is too
    large, or does not describe a valid bundle: a CRN could not boot it
    either, so the message is permanently rejected. A manifest whose bytes
    are merely unreachable right now raises VmVolumeNotFound instead, so the
    message is retried like any other missing volume.
    """
    pin = get_message_file_pin(session, runtime_ref)
    if pin is None:
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} is not pinned on this node"
        )

    if pin.file.size > MAX_RUNTIME_MANIFEST_SIZE:
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} (file {pin.file_hash}) is "
            f"{pin.file.size} bytes, over the {MAX_RUNTIME_MANIFEST_SIZE} byte limit"
        )

    try:
        engine = item_type_from_hash(pin.file_hash)
    except UnknownHashError as e:
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} pins an unrecognised file hash {pin.file_hash}"
        ) from e

    try:
        content = await storage_service.get_hash_content(
            pin.file_hash,
            engine=engine,
            tries=2,
            timeout=15,
            use_network=True,
            use_ipfs=True,
            store_value=False,
        )
    except ContentCurrentlyUnavailable as e:
        # Transient: the manifest is pinned but its bytes have not reached
        # this node yet. Same semantics as an unpinned bundle, i.e. retry.
        raise VmVolumeNotFound([runtime_ref]) from e
    except AlephStorageException as e:
        # Permanent, e.g. InvalidContent: the bytes are there but wrong.
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} (file {pin.file_hash}) could not be read: {e}"
        ) from e

    try:
        manifest = RuntimeManifestBundle.model_validate(json.loads(content.value))
    except (ValueError, ValidationError, RecursionError) as e:
        # RecursionError: json.loads has no depth cap of its own, so deeply
        # nested (but otherwise well-formed and under the size cap) input
        # can blow the interpreter's recursion limit instead of raising a
        # normal parse error.
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} is not a valid {RUNTIME_MANIFEST_FORMAT} manifest: {e}"
        ) from e

    return str(manifest.bundle.ref)


def runtime_bundle_volume(bundle_ref: str) -> RefVolume:
    """The cost-model volume for a resolved bundle: sized from its pinned
    file like every other artifact."""
    return RefVolume(CostType.EXECUTION_VPROGRAM_VOLUME, bundle_ref, False, "runtime")
