"""Unit tests for src/aleph/toolkit/car.py: CARv1 header parser."""

from pathlib import Path

import dag_cbor
import pytest
from car_test_utils import build_carv1, encode_varint
from multiformats import CID

from aleph.toolkit.car import InvalidCarFile, read_carv1_root


@pytest.fixture
def sample_root_cid() -> CID:
    # CIDv1, dag-pb codec (0x70), sha2-256 multihash of a known byte string.
    # Stable across runs.
    return CID.decode("bafybeibwzifw72ttrkqglhi64gn3stoyjs6t2vcyfzr67gqkogfgcyo3uy")


def test_valid_carv1_single_root_returns_cid(
    tmp_path: Path, sample_root_cid: CID
) -> None:
    car_bytes = build_carv1(sample_root_cid)
    car_path = tmp_path / "valid.car"
    car_path.write_bytes(car_bytes)

    result = read_carv1_root(car_path)

    assert result == str(sample_root_cid)


def test_truncated_varint(tmp_path: Path) -> None:
    # 0x80 has the continuation bit set with no follow-up byte.
    car_path = tmp_path / "bad.car"
    car_path.write_bytes(b"\x80")

    with pytest.raises(InvalidCarFile, match="malformed varint"):
        read_carv1_root(car_path)


def test_oversized_header_declared(tmp_path: Path) -> None:
    # varint value 1 << 20 (~1 MiB) exceeds MAX_CAR_HEADER_BYTES.
    car_path = tmp_path / "bad.car"
    car_path.write_bytes(encode_varint(1 << 20))

    with pytest.raises(InvalidCarFile, match="header exceeds maximum size"):
        read_carv1_root(car_path)


def test_truncated_header(tmp_path: Path, sample_root_cid: CID) -> None:
    full = build_carv1(sample_root_cid)
    # Take the varint plus only part of the declared header.
    varint_len = len(encode_varint(len(full) - len(encode_varint(0)) - 1))
    truncated = full[: varint_len + 5]
    car_path = tmp_path / "bad.car"
    car_path.write_bytes(truncated)

    with pytest.raises(InvalidCarFile, match="truncated header"):
        read_carv1_root(car_path)


def test_malformed_dag_cbor_header(tmp_path: Path) -> None:
    # 10 bytes of random garbage prefixed with a varint claiming 10 bytes.
    garbage = b"\xff" * 10
    car_path = tmp_path / "bad.car"
    car_path.write_bytes(encode_varint(len(garbage)) + garbage)

    with pytest.raises(InvalidCarFile, match="malformed DAG-CBOR header"):
        read_carv1_root(car_path)


def test_dag_cbor_header_not_a_map(tmp_path: Path) -> None:
    # DAG-CBOR encoding of an integer, not a map.
    body = dag_cbor.encode(42)
    car_path = tmp_path / "bad.car"
    car_path.write_bytes(encode_varint(len(body)) + body)

    with pytest.raises(InvalidCarFile, match="not a map"):
        read_carv1_root(car_path)


def test_unsupported_version(tmp_path: Path, sample_root_cid: CID) -> None:
    car_bytes = build_carv1(sample_root_cid, version=2)
    car_path = tmp_path / "v2.car"
    car_path.write_bytes(car_bytes)

    with pytest.raises(InvalidCarFile, match="unsupported CAR version"):
        read_carv1_root(car_path)


def test_roots_field_missing(tmp_path: Path) -> None:
    body = dag_cbor.encode({"version": 1})
    car_path = tmp_path / "no_roots.car"
    car_path.write_bytes(encode_varint(len(body)) + body)

    with pytest.raises(InvalidCarFile, match="roots is not a list"):
        read_carv1_root(car_path)


def test_zero_roots(tmp_path: Path, sample_root_cid: CID) -> None:
    car_bytes = build_carv1(sample_root_cid, n_roots=0)
    car_path = tmp_path / "zero.car"
    car_path.write_bytes(car_bytes)

    with pytest.raises(InvalidCarFile, match="expected exactly 1 root, got 0"):
        read_carv1_root(car_path)


def test_multi_roots(tmp_path: Path, sample_root_cid: CID) -> None:
    car_bytes = build_carv1(sample_root_cid, n_roots=2)
    car_path = tmp_path / "multi.car"
    car_path.write_bytes(car_bytes)

    with pytest.raises(InvalidCarFile, match="expected exactly 1 root, got 2"):
        read_carv1_root(car_path)


def test_malformed_root_cid(tmp_path: Path) -> None:
    # A map whose `roots` entry is a string, not a CID. dag_cbor.decode
    # accepts the encoding but our parser should reject the root.
    body = dag_cbor.encode({"version": 1, "roots": ["not a cid"]})
    car_path = tmp_path / "bad_root.car"
    car_path.write_bytes(encode_varint(len(body)) + body)

    with pytest.raises(InvalidCarFile, match="malformed root CID"):
        read_carv1_root(car_path)
