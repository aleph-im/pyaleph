from aleph.toolkit.blocklist import is_blocked_sender


def test_is_blocked_sender_is_case_insensitive():
    assert is_blocked_sender("0xAbCdEf", ["0xabcdef"])


def test_is_blocked_sender_ignores_surrounding_whitespace():
    assert is_blocked_sender("  0xAbCdEf  ", [" 0xabcdef "])


def test_is_blocked_sender_accepts_a_single_address():
    assert is_blocked_sender("0xAbCdEf", "0xabcdef")


def test_is_blocked_sender_returns_false_for_unlisted_sender():
    assert not is_blocked_sender("0xabcdef", ["0x123456"])


def test_is_blocked_sender_returns_false_for_empty_blocklist():
    assert not is_blocked_sender("0xabcdef", [])
