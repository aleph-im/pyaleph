import pytest

from aleph.cli.args import parse_args


def test_repair_enabled_by_default():
    """Repair operations run at boot unless explicitly disabled."""
    args = parse_args([])
    assert args.repair is True


def test_repair_flag_enables_repair():
    args = parse_args(["--repair"])
    assert args.repair is True


def test_no_repair_flag_disables_repair():
    args = parse_args(["--no-repair"])
    assert args.repair is False


def test_credits_refold_from_flag():
    assert parse_args([]).credits_refold_from is None
    args = parse_args(["--credits-refold-from", "1748779200"])
    assert args.credits_refold_from == 1748779200


def test_credits_refold_from_must_be_hour_aligned():
    with pytest.raises(SystemExit):
        parse_args(["--credits-refold-from", "1748779201"])
