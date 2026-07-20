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
