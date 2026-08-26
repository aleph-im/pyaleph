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
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from aleph.db.accessors.files import get_message_file_pin
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.storage import StorageService
from aleph.types.cost import CostType, RefVolume
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidVProgramRuntime
from aleph.utils import item_type_from_hash

RUNTIME_MANIFEST_FORMAT = "aleph-vprogram-runtime"
ITEM_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class RuntimeBundleRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ref: str

    @field_validator("ref")
    @classmethod
    def _ref_is_item_hash(cls, value: str) -> str:
        if not ITEM_HASH_PATTERN.fullmatch(value):
            raise ValueError("bundle.ref must be a 64-char hex STORE message hash")
        return value


class RuntimeManifestBundle(BaseModel):
    """The two manifest fields the CCN needs. Everything else is ignored:
    the CRN validates the full schema."""

    model_config = ConfigDict(extra="ignore")

    format: Literal["aleph-vprogram-runtime"]
    bundle: RuntimeBundleRef


async def resolve_runtime_bundle_ref(
    session: DbSession, storage_service: StorageService, runtime_ref: str
) -> str:
    """Return the STORE message hash of the bundle named by the manifest at
    `runtime_ref`. Raises InvalidVProgramRuntime when the manifest is not
    pinned, cannot be read, or does not describe a valid bundle."""
    pin = get_message_file_pin(session, runtime_ref)
    if pin is None:
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} is not pinned on this node"
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
    except AlephStorageException as e:
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} (file {pin.file_hash}) could not be read: {e}"
        ) from e

    try:
        manifest = RuntimeManifestBundle.model_validate(json.loads(content.value))
    except (ValueError, ValidationError) as e:
        raise InvalidVProgramRuntime(
            f"runtime manifest {runtime_ref} is not a valid {RUNTIME_MANIFEST_FORMAT} manifest: {e}"
        ) from e

    return manifest.bundle.ref


def runtime_bundle_volume(bundle_ref: str) -> RefVolume:
    """The cost-model volume for a resolved bundle: sized from its pinned
    file like every other artifact."""
    return RefVolume(CostType.EXECUTION_VPROGRAM_VOLUME, bundle_ref, False, "runtime")
