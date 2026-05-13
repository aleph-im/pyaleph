"""Shared CAR-construction helpers for CAR-related tests."""

from typing import Any

import dag_cbor
from multiformats import CID


def encode_varint(n: int) -> bytes:
    """Unsigned LEB128 varint, matching the CARv1 wire format."""
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def build_carv1(root: CID | str, version: int = 1, n_roots: int = 1) -> bytes:
    """Build a CARv1 file with only its header (no blocks). Sufficient for
    parser tests and for the API handler, which never reads past the header
    in the unit-test path (kubo is mocked)."""
    if isinstance(root, str):
        root = CID.decode(root)
    roots: list[CID]
    if n_roots == 0:
        roots = []
    elif n_roots == 1:
        roots = [root]
    else:
        roots = [root] * n_roots
    header_data: dict[str, Any] = {"version": version, "roots": roots}
    header = dag_cbor.encode(header_data)
    return encode_varint(len(header)) + header
