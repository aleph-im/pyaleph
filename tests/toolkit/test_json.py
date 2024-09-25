import json

import pytest

import aleph.toolkit.json as aleph_json


def test_loads():
    """
    A (simplistic) load test, as a sanity check.
    """

    expected = {"1": {"a": "b", "c": "d"}, "2": ["x", "y", "z"], "3": "world"}
    serialized_json = json.dumps(expected)

    actual = aleph_json.loads(serialized_json)
    assert actual == expected


def test_loads_invalid_json():
    s = '{"1": "3"'
    with pytest.raises(aleph_json.DecodeError):
        _ = aleph_json.loads(s)


def test_reject_nans():
    """
    Test that the implementation rejects NaN as it is not part of the official
    JSON specification and is unsupported by Postgres.
    """

    serialized_json = '{"1": 1, "2": 2, "3": NaN}'
    with pytest.raises(json.decoder.JSONDecodeError):
        _ = aleph_json.loads(serialized_json)


def test_serialized_json_type():
    """
    Check that the output of dumps is of the announced type.
    """

    expected = {"1": "2", "3": {"4": "5"}}

    serialized_json = aleph_json.dumps(expected)
    assert isinstance(serialized_json, aleph_json.SerializedJson)

    actual = json.loads(serialized_json)
    assert actual == expected
