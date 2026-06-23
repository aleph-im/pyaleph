from enum import Enum


class IpnsStatus(str, Enum):
    """Lifecycle status of an IPNS registration."""

    # Record valid, content pinned within quota.
    OK = "ok"
    # The name resolved to content larger than the paid quota.
    # The last good CID stays pinned and served.
    OVER_QUOTA = "over_quota"
    # The signed record passed its end-of-life and is no longer
    # republished. The pin is kept; the user must renew the record.
    EXPIRED = "expired"
