import pytest
from aleph_message.models import ItemType

from aleph.exceptions import UnknownHashError
from aleph.utils import item_type_from_hash

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"


def test_item_type_from_hash_ipns():
    assert item_type_from_hash(IPNS_NAME) == ItemType.ipns


def test_item_type_from_hash_rejects_bad_ipns_shapes():
    with pytest.raises(UnknownHashError):
        item_type_from_hash(IPNS_NAME[:-1])
    with pytest.raises(UnknownHashError):
        item_type_from_hash(IPNS_NAME.upper())
