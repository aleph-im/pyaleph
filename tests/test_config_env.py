import multiprocessing

import pytest
import yaml
from configmanager import Config

from aleph.config import get_defaults
from aleph.jobs.job_utils import prepare_config

SECRET_VARS = {
    ("postgres", "password"): "POSTGRES_PASSWORD",
    ("rabbitmq", "password"): "RABBITMQ_PASSWORD",
    ("sentry", "dsn"): "SENTRY_DSN",
    ("nuls2", "private_key"): "NULS2_PRIVATE_KEY",
    ("ethereum", "private_key"): "ETHEREUM_PRIVATE_KEY",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in SECRET_VARS.values():
        monkeypatch.delenv(var, raising=False)


def _load_yaml(tmp_path, content: dict) -> Config:
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(content))
    config = Config(schema=get_defaults())
    config.yaml.load(str(path))
    return config


def test_default_applies_without_env_or_yaml():
    config = Config(schema=get_defaults())
    assert config.postgres.password.value == "decentralize-everything"
    assert config.rabbitmq.password.value == "change-me!"
    assert config.sentry.dsn.value is None


def test_yaml_value_applies_without_env(tmp_path):
    config = _load_yaml(
        tmp_path,
        {"postgres": {"password": "from-yaml"}, "rabbitmq": {"password": "mq-yaml"}},
    )
    assert config.postgres.password.value == "from-yaml"
    assert config.rabbitmq.password.value == "mq-yaml"


def test_env_overrides_yaml(tmp_path, monkeypatch):
    config = _load_yaml(
        tmp_path,
        {"postgres": {"password": "from-yaml"}, "rabbitmq": {"password": "mq-yaml"}},
    )
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-env")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "mq-env")
    assert config.postgres.password.value == "from-env"
    assert config.rabbitmq.password.value == "mq-env"


def test_env_overrides_default(monkeypatch):
    config = Config(schema=get_defaults())
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-env")
    assert config.postgres.password.value == "from-env"


@pytest.mark.parametrize("key,var", list(SECRET_VARS.items()))
def test_every_secret_reads_its_env_var(key, var, monkeypatch):
    config = Config(schema=get_defaults())
    monkeypatch.setenv(var, "s3cret")
    assert config[key[0]][key[1]].value == "s3cret"


def _child(config_values, queue):
    # Mimics what the job subprocesses do with the dict from dump_values().
    config = prepare_config(config_values)
    queue.put((config.postgres.password.value, config.rabbitmq.password.value))


def test_env_password_reaches_subprocess_config(tmp_path, monkeypatch):
    config = _load_yaml(tmp_path, {"postgres": {"password": "from-yaml"}})
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-env")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "mq-env")
    config_values = config.dump_values()

    # Spawn (not fork) so the child rebuilds the config from scratch, like a
    # fresh process would; it inherits the environment from this process.
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    process = ctx.Process(target=_child, args=(config_values, queue))
    process.start()
    result = queue.get(timeout=60)
    process.join(timeout=60)

    assert result == ("from-env", "mq-env")


def test_prepare_config_in_process_keeps_env_value(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-env")
    values = Config(schema=get_defaults()).dump_values()
    assert prepare_config(values).postgres.password.value == "from-env"
