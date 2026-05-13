"""Tier-2 integration tests for /api/v0/ipfs/add_car against a real kubo.

Skipped by default. To run:

    ALEPH_PYALEPH_TEST_REAL_KUBO=1 venv/bin/pytest tests/api/test_ipfs_car_real_kubo.py -v

The tests assume a kubo daemon at the address in tests/conftest.py's
mock_config (default host=ipfs, port=5001). They will create real pins on
that daemon and rely on the operator to GC them.
"""

import os
import pathlib

import pytest

REAL_KUBO = bool(os.environ.get("ALEPH_PYALEPH_TEST_REAL_KUBO"))

pytestmark = pytest.mark.skipif(
    not REAL_KUBO,
    reason="set ALEPH_PYALEPH_TEST_REAL_KUBO=1 to run real-kubo tests",
)


@pytest.mark.asyncio
async def test_add_car_real_roundtrip(tmp_path: pathlib.Path) -> None:
    """TODO: build a small UnixFS directory locally (via aleph-rs producer
    once it lands, or a hand-rolled DAG via ipld-prime), serialize as
    CARv1, POST to /ipfs/add_car, then GET /storage/raw/<cid> and assert
    the tar contains the expected file tree."""
    pytest.skip("requires aleph-rs CAR producer or a committed real-kubo CAR fixture")


@pytest.mark.asyncio
async def test_add_car_real_imported_root_mismatch(
    tmp_path: pathlib.Path,
) -> None:
    """TODO: build a CARv1 with a tampered header (root field replaced),
    POST it, assert 422 'Imported root does not match expected'."""
    pytest.skip("requires real CAR fixture builder")


@pytest.mark.asyncio
async def test_add_car_real_double_upload_idempotent(
    tmp_path: pathlib.Path,
) -> None:
    """TODO: POST the same CAR twice, assert both succeed and the second
    is a fast no-op pin."""
    pytest.skip("requires real CAR fixture builder")
