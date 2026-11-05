from configmanager import Config

from aleph.config import get_defaults
from aleph.toolkit.constants import DEFAULT_REMOVAL_GRACE_PERIOD_HOURS


def test_removal_grace_period_uses_default():
    config = Config(schema=get_defaults())

    assert (
        config.storage.removal_grace_period_hours.value
        == DEFAULT_REMOVAL_GRACE_PERIOD_HOURS
    )


def test_removal_grace_period_env_override(monkeypatch):
    monkeypatch.setenv("STORAGE_REMOVAL_GRACE_PERIOD_HOURS", "48")

    config = Config(schema=get_defaults())

    assert config.storage.removal_grace_period_hours.value == 48
