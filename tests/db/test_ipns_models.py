import datetime as dt

from aleph.db.models.files import FilePinType, IpnsFilePinDb
from aleph.db.models.ipns import IpnsRecordDb
from aleph.types.ipns import IpnsStatus

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"


def test_ipns_pin_type_exists():
    assert FilePinType.IPNS.value == "ipns"
    assert IpnsFilePinDb.__mapper_args__["polymorphic_identity"] == "ipns"


def test_ipns_record_model_columns():
    record = IpnsRecordDb(
        name=IPNS_NAME,
        owner="0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        item_hash="7f2d09b2c4e1a8f3d6b5c2e9a1f4d7b8e3c6a9f2d5b8e1c4a7f0d3b6e9c2a5f8",
        record=b"\x0a\x01raw",
        record_sequence=1,
        record_validity=dt.datetime(2028, 6, 9, tzinfo=dt.timezone.utc),
        max_size_mib=100,
        resolved_cid=None,
        status=IpnsStatus.OK,
        created=dt.datetime(2026, 6, 10, tzinfo=dt.timezone.utc),
    )
    assert record.status == IpnsStatus.OK
    assert record.record_sequence == 1
