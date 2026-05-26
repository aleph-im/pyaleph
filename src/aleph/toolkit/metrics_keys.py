"""Shared Redis key constants for node metrics.

Kept in toolkit (a leaf module that imports nothing from the web or handler
layers) so both the writer (``handlers/content/store.py``) and the reader
(``web/controllers/metrics.py``) use the same strings without a circular import.
"""

from aleph_message.models import ItemType

# STORE file-fetch metrics, split by item type because the fetch source differs:
# `storage` files are pulled from CCN HTTP APIs, `ipfs` files come through the
# IPFS (Kubo) daemon. Tracking them separately tells a disk-bound regression
# (both slow) from a network/bitswap one (only ipfs slow). The mean fetch time
# is duration_ms_sum / (total - failed) per type.
STORE_FETCH_IPFS_TOTAL_KEY = "pyaleph_store_fetch_ipfs_total"
STORE_FETCH_IPFS_FAILED_KEY = "pyaleph_store_fetch_ipfs_failed_total"
STORE_FETCH_IPFS_DURATION_MS_SUM_KEY = "pyaleph_store_fetch_ipfs_duration_ms_sum"
STORE_FETCH_STORAGE_TOTAL_KEY = "pyaleph_store_fetch_storage_total"
STORE_FETCH_STORAGE_FAILED_KEY = "pyaleph_store_fetch_storage_failed_total"
STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY = "pyaleph_store_fetch_storage_duration_ms_sum"


def store_fetch_keys(item_type: ItemType) -> tuple[str, str, str]:
    """Return the (total, failed, duration_ms_sum) Redis keys for an item type."""
    if item_type == ItemType.ipfs:
        return (
            STORE_FETCH_IPFS_TOTAL_KEY,
            STORE_FETCH_IPFS_FAILED_KEY,
            STORE_FETCH_IPFS_DURATION_MS_SUM_KEY,
        )
    return (
        STORE_FETCH_STORAGE_TOTAL_KEY,
        STORE_FETCH_STORAGE_FAILED_KEY,
        STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY,
    )
