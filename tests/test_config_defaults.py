from aleph.config import get_defaults
from aleph.toolkit.constants import MiB


def test_upload_mitigation_defaults():
    defaults = get_defaults()
    assert defaults["storage"]["grace_period"] == 6
    assert defaults["storage"]["garbage_collector_period"] == 4
    assert defaults["storage"]["max_unauthenticated_upload_file_size"] == 25 * MiB
