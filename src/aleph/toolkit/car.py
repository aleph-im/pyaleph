"""CARv1 header parser.

Reads only the leading varint-prefixed DAG-CBOR header of a CARv1 file and
returns its single root CID. Does not validate or read any block past the
header. Kubo's /api/v0/dag/import performs full block-level verification
during import.

Reference: https://ipld.io/specs/transport/car/carv1/
"""

from pathlib import Path
from typing import BinaryIO

import dag_cbor
from multiformats import CID

MAX_CAR_HEADER_BYTES = 8 * 1024
"""Upper bound on the declared header size. A CARv1 single-root header is
typically ~40 bytes; this bound exists to cap attacker-controlled
allocations from a malicious varint."""

_MAX_VARINT_BYTES = 10
"""Maximum bytes a 64-bit unsigned LEB128 varint can occupy."""


class InvalidCarFile(ValueError):
    """The CAR header could not be parsed or did not meet our constraints."""


def read_carv1_root(path: Path) -> str:
    """Read the CARv1 header from `path` and return its single root CID as a
    canonical string (base32 for CIDv1, base58btc for CIDv0).

    Raises:
        InvalidCarFile: with a specific reason for: malformed varint, header
            exceeds maximum size, truncated header, malformed DAG-CBOR
            header, unsupported version, roots length != 1, malformed root
            CID.
    """
    with path.open("rb") as f:
        header_len = _read_varint(f)
        if header_len > MAX_CAR_HEADER_BYTES:
            raise InvalidCarFile(
                f"header exceeds maximum size ({header_len} > {MAX_CAR_HEADER_BYTES})"
            )
        header_bytes = f.read(header_len)
        if len(header_bytes) != header_len:
            raise InvalidCarFile("truncated header")

    try:
        header = dag_cbor.decode(header_bytes)
    except Exception as e:
        raise InvalidCarFile(f"malformed DAG-CBOR header: {e}") from e

    if not isinstance(header, dict):
        raise InvalidCarFile("malformed DAG-CBOR header: not a map")

    version = header.get("version")
    if version != 1:
        raise InvalidCarFile(f"unsupported CAR version (got {version!r}, expected 1)")

    roots = header.get("roots")
    if not isinstance(roots, list):
        raise InvalidCarFile("malformed DAG-CBOR header: roots is not a list")
    if len(roots) != 1:
        raise InvalidCarFile(f"expected exactly 1 root, got {len(roots)}")

    raw_root = roots[0]
    if isinstance(raw_root, CID):
        cid = raw_root
    elif isinstance(raw_root, (str, bytes)):
        try:
            cid = CID.decode(raw_root)
        except Exception as e:
            raise InvalidCarFile(f"malformed root CID: {e}") from e
    else:
        raise InvalidCarFile(
            f"malformed root CID: unexpected type {type(raw_root).__name__}"
        )

    # Canonicalize the string form: base32 for CIDv1, base58btc for CIDv0.
    # dag_cbor.decode reconstructs a CID without preserving its multibase, so
    # str(cid) on a v1 CID can yield base58btc by default. Force the canonical
    # encoding to match the IPFS convention.
    if cid.version == 1:
        return str(cid.set(base="base32"))
    return str(cid)


def _read_varint(f: BinaryIO) -> int:
    """Read an unsigned LEB128 varint from `f`. Raises InvalidCarFile on
    truncation or oversized values."""
    result = 0
    shift = 0
    for i in range(_MAX_VARINT_BYTES):
        byte = f.read(1)
        if not byte:
            raise InvalidCarFile("malformed varint: truncated")
        b = byte[0]
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result
        shift += 7
    raise InvalidCarFile("malformed varint: exceeds 10 bytes")
