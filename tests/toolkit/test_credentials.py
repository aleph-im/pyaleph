import logging
from types import SimpleNamespace

import pytest

from aleph.toolkit.credentials import (
    CredentialFileError,
    PasswordSource,
    install_password_hook,
    is_authentication_failure,
    read_credential,
)


def test_file_wins_over_env_and_inline(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("from-file")

    assert (
        read_credential("inline", "from-env", str(secret), what="postgres password")
        == "from-file"
    )


def test_env_wins_over_inline_when_no_file():
    assert read_credential("inline", "from-env", None, what="pg") == "from-env"
    assert read_credential("inline", None, None, what="pg") == "inline"


def test_trailing_newline_stripped(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("  s3cret \r\n\n")

    # Only trailing whitespace goes: leading spaces may be part of the secret.
    assert read_credential(None, None, str(secret), what="pg") == "  s3cret"


def test_empty_file_raises_with_path(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("\n")

    with pytest.raises(CredentialFileError) as exc_info:
        read_credential("inline", "env", str(secret), what="postgres password")

    assert str(secret) in str(exc_info.value)
    assert "postgres password" in str(exc_info.value)


def test_missing_file_raises_with_path(tmp_path):
    secret = tmp_path / "absent"

    with pytest.raises(CredentialFileError) as exc_info:
        read_credential("inline", "env", str(secret), what="rabbitmq password")

    assert str(secret) in str(exc_info.value)
    assert "rabbitmq password" in str(exc_info.value)


def test_current_reads_file_each_time(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("A\n")
    source = PasswordSource(secret, fallback="inline")

    assert source.current() == "A"
    secret.write_text("B\n")
    assert source.current() == "B"


def test_current_uses_fallback_without_file():
    assert PasswordSource(None, fallback="inline").current() == "inline"


def test_current_raises_when_file_disappears(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("A")
    source = PasswordSource(secret, fallback="inline")
    secret.unlink()

    with pytest.raises(CredentialFileError):
        source.current()


def test_last_good_updates_on_remember(tmp_path):
    source = PasswordSource(None, fallback="inline")
    assert source.last_good is None

    source.remember("A")
    assert source.last_good == "A"
    source.remember("B")
    assert source.last_good == "B"


class _FakeOperationalError(Exception):
    def __init__(self, message, pgcode=None):
        super().__init__(message)
        self.pgcode = pgcode


def test_authentication_failure_detected_by_sqlstate():
    assert is_authentication_failure(_FakeOperationalError("whatever", "28P01"))


def test_authentication_failure_detected_by_message():
    error = _FakeOperationalError(
        'connection to server at "127.0.0.1", port 5432 failed: FATAL:  '
        'password authentication failed for user "aleph"'
    )
    assert is_authentication_failure(error)


def test_other_errors_are_not_authentication_failures():
    assert not is_authentication_failure(
        _FakeOperationalError("could not connect to server: Connection refused")
    )
    # SQLSTATE wins over the text: a different code is not an auth failure.
    assert not is_authentication_failure(
        _FakeOperationalError("password authentication failed", "08006")
    )


class _FakeDialect:
    """Stands in for the SQLAlchemy dialect: accepts only `accepted`."""

    def __init__(self, accepted, error=None):
        self.accepted = accepted
        self.error = error
        self.attempts = []

    def connect(self, *cargs, **cparams):
        self.attempts.append(cparams["password"])
        if self.error is not None:
            raise self.error
        if cparams["password"] != self.accepted:
            raise _FakeOperationalError(
                'FATAL:  password authentication failed for user "aleph"'
            )
        return SimpleNamespace(password=cparams["password"])


def _capture_hook(source):
    """Install the hook on a fake engine and return the registered function."""
    registered = {}

    def listen(target, identifier, fn, **kw):
        registered[identifier] = (fn, kw)

    install_password_hook(SimpleNamespace(), source, _listen=listen)
    fn, kw = registered["do_connect"]
    return fn


def test_hook_sets_file_password_and_remembers_it(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("A")
    source = PasswordSource(secret, fallback="inline")
    hook = _capture_hook(source)
    dialect = _FakeDialect(accepted="A")
    cparams = {"password": "inline", "user": "aleph"}

    connection = hook(dialect, None, [], cparams)

    assert connection.password == "A"
    assert dialect.attempts == ["A"]
    assert source.last_good == "A"


def test_hook_retries_with_last_good_on_auth_failure(tmp_path, caplog):
    secret = tmp_path / "password"
    secret.write_text("pw-new")
    source = PasswordSource(secret, fallback="inline")
    source.remember("pw-old")
    hook = _capture_hook(source)
    dialect = _FakeDialect(accepted="pw-old")

    with caplog.at_level(logging.WARNING):
        connection = hook(dialect, None, [], {"password": "inline"})

    assert connection.password == "pw-old"
    assert dialect.attempts == ["pw-new", "pw-old"]
    assert source.last_good == "pw-old"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "last known good" in message
    assert str(secret) in message
    # Never log a password.
    assert "pw-new" not in message and "pw-old" not in message


def test_hook_does_not_retry_other_errors(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("B")
    source = PasswordSource(secret, fallback="inline")
    source.remember("A")
    hook = _capture_hook(source)
    dialect = _FakeDialect(
        accepted="A", error=_FakeOperationalError("Connection refused")
    )

    with pytest.raises(_FakeOperationalError):
        hook(dialect, None, [], {"password": "inline"})

    assert dialect.attempts == ["B"]


def test_hook_does_not_retry_without_a_different_last_good(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("B")
    source = PasswordSource(secret, fallback="inline")
    source.remember("B")
    hook = _capture_hook(source)
    dialect = _FakeDialect(accepted="A")

    with pytest.raises(_FakeOperationalError):
        hook(dialect, None, [], {"password": "inline"})

    assert dialect.attempts == ["B"]


def test_hook_raises_original_error_when_retry_also_fails(tmp_path):
    secret = tmp_path / "password"
    secret.write_text("B")
    source = PasswordSource(secret, fallback="inline")
    source.remember("A")
    hook = _capture_hook(source)
    dialect = _FakeDialect(accepted="C")

    with pytest.raises(_FakeOperationalError):
        hook(dialect, None, [], {"password": "inline"})

    assert dialect.attempts == ["B", "A"]
    # Nothing worked, so nothing new is remembered.
    assert source.last_good == "A"


def test_password_file_settings_default_to_unset_and_read_env(monkeypatch):
    from configmanager import Config

    from aleph.config import get_defaults

    config = Config(get_defaults())
    assert config.postgres.password_file.value is None
    assert config.rabbitmq.password_file.value is None

    monkeypatch.setenv("POSTGRES_PASSWORD_FILE", "/run/secrets/pg")
    monkeypatch.setenv("RABBITMQ_PASSWORD_FILE", "/run/secrets/mq")
    config = Config(get_defaults())
    assert config.postgres.password_file.value == "/run/secrets/pg"
    assert config.rabbitmq.password_file.value == "/run/secrets/mq"


@pytest.mark.asyncio
async def test_make_mq_conn_reads_password_file_at_connect(tmp_path, mocker):
    from configmanager import Config

    from aleph.config import get_defaults
    from aleph.toolkit.rabbitmq import make_mq_conn

    secret = tmp_path / "mq-password"
    secret.write_text("mq-from-file\n")
    config = Config(get_defaults())
    config.rabbitmq.password_file.value = str(secret)
    connect_robust = mocker.patch(
        "aleph.toolkit.rabbitmq.aio_pika.connect_robust", new=mocker.AsyncMock()
    )

    await make_mq_conn(config)
    assert connect_robust.call_args.kwargs["password"] == "mq-from-file"

    # Read again at the next connect, not cached at import or config load.
    secret.write_text("mq-rotated\n")
    await make_mq_conn(config)
    assert connect_robust.call_args.kwargs["password"] == "mq-rotated"
