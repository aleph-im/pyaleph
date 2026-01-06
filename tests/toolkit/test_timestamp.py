import datetime as dt

from aleph.toolkit.timestamp import timestamp_to_datetime


def test_timestamp_to_datetime():
    t1 = 1675206096.0  # 20230201T00:01:36+01:00
    dt1 = timestamp_to_datetime(t1)

    assert dt1.year == 2023
    assert dt1.month == 1
    assert dt1.day == 31
    assert dt1.hour == 23
    assert dt1.minute == 1
    assert dt1.second == 36

    assert dt1.tzinfo == dt.timezone.utc
