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


def test_loads_large_ints_json():
    """
    Check that the output of dumps and loads don't raise TypeError errors caused by large ints on orjson library.
    """

    expected = {
        "0x3E1aba4ad853Dd7Aa531aB59F10bd9f4d89aebaF": 498729072221377800000,
        "0x525C49BF83Ce3a1AAf425ac1A463537dB68c8bd7": 8059602048250472000000,
        "0x7F05Ed9650E48f3E564125EAdCdc0d5E7c2E8DaB": 1991950397951749400000000,
        "0xb6e45ADfa0C7D70886bBFC990790d64620F1BAE8": 497997000000000000000000000,
    }

    serialized_json = aleph_json.dumps(expected)
    assert isinstance(serialized_json, aleph_json.SerializedJson)

    actual = json.loads(serialized_json)
    assert actual == expected
