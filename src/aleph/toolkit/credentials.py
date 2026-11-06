"""Credentials read from files, so a secret can rotate without a restart.

A ``*_file`` setting names a file holding the secret (for example a Kubernetes
Secret mounted as a volume). It wins over the inline value and the environment
variable. The file is read again every time a new connection is opened, so a
rotated secret is picked up by the next connection instead of at the next
restart.

PostgreSQL rotation can happen in either order (Secret first, then
``ALTER ROLE``, or the reverse with the Secret updated shortly after), so the
psycopg2 hook keeps the last password that worked and, on an authentication
failure, retries once with it.
"""

import logging
from pathlib import Path
from typing import Any, Callable, Optional

LOGGER = logging.getLogger(__name__)

# SQLSTATE invalid_password.
_INVALID_PASSWORD_SQLSTATE = "28P01"
_INVALID_PASSWORD_MESSAGE = "password authentication failed"


class CredentialFileError(Exception):
    """A configured credential file is missing, unreadable or empty.

    Raised instead of falling back to an empty password, which would hide a
    broken mount behind a confusing authentication error.
    """

    def __init__(self, what: str, path: str, reason: str = "is missing or empty"):
        self.what = what
        self.path = path
        super().__init__(f"{what}: credential file {path} {reason}")


def _read_file(path: str, what: str) -> str:
    try:
        value = Path(path).read_text().rstrip()
    except FileNotFoundError:
        raise CredentialFileError(what, path, "does not exist") from None
    except OSError as error:
        raise CredentialFileError(
            what, path, f"cannot be read ({error.strerror or error})"
        ) from None
    if not value:
        raise CredentialFileError(what, path, "is empty")
    return value


def read_credential(
    inline: Optional[str],
    env_value: Optional[str],
    file_path: Optional[str],
    *,
    what: str,
) -> str:
    """Resolve a credential: file > environment variable > inline value.

    The file content has its trailing whitespace (the newline editors and
    ``kubectl create secret --from-file`` leave) stripped. A missing or empty
    file raises CredentialFileError naming ``what`` and the path.

    Settings declared with ``Item(envvar=...)`` already fold the environment
    variable into their value, so callers reading such an item pass it as
    ``inline`` and ``env_value=None``.
    """
    if file_path:
        return _read_file(str(file_path), what)
    if env_value is not None:
        return env_value
    return inline  # type: ignore[return-value]


class PasswordSource:
    """The password to use for the next connection, and the last that worked.

    ``fallback`` is the inline / environment value, used when no file is set.
    """

    def __init__(
        self,
        file_path: Optional[Path],
        fallback: Optional[str],
        *,
        what: str = "password",
    ):
        self.file_path = Path(file_path) if file_path else None
        self.fallback = fallback
        self.what = what
        self.last_good: Optional[str] = None

    def current(self) -> str:
        """The configured password, re-reading the file on every call."""
        return read_credential(
            self.fallback,
            None,
            str(self.file_path) if self.file_path else None,
            what=self.what,
        )

    def remember(self, password: str) -> None:
        self.last_good = password

    def describe(self) -> str:
        return f"file {self.file_path}" if self.file_path else "inline value"


def is_authentication_failure(error: BaseException) -> bool:
    """Whether a DBAPI error is PostgreSQL rejecting the password.

    Uses the SQLSTATE when the driver exposes one (``pgcode``); libpq does not
    report it for errors raised while connecting, so psycopg2 usually leaves it
    unset there and the server message is matched instead.
    """
    pgcode = getattr(error, "pgcode", None)
    if pgcode:
        return pgcode == _INVALID_PASSWORD_SQLSTATE
    return _INVALID_PASSWORD_MESSAGE in str(error)


def install_password_hook(
    engine: Any,
    source: PasswordSource,
    *,
    _listen: Optional[Callable[..., None]] = None,
) -> None:
    """Make ``engine`` read its password from ``source`` for each connection.

    Registers a SQLAlchemy ``do_connect`` listener that connects with the
    current password and, if PostgreSQL rejects it, retries once with the last
    password that worked (when it differs). Whichever succeeds is remembered.
    Any other error, or a second rejection, propagates the first error.

    For drivers called synchronously by the pool (psycopg2). ``_listen`` lets
    unit tests capture the listener without a real engine.
    """
    if _listen is None:
        from sqlalchemy import event

        _listen = event.listen

    def connect_with_rotating_password(dialect, conn_rec, cargs, cparams):
        password = source.current()
        cparams["password"] = password
        try:
            connection = dialect.connect(*cargs, **cparams)
        except Exception as error:
            last_good = source.last_good
            if (
                not is_authentication_failure(error)
                or last_good is None
                or last_good == password
            ):
                raise
            cparams["password"] = last_good
            try:
                connection = dialect.connect(*cargs, **cparams)
            except Exception:
                raise error from None
            source.remember(last_good)
            LOGGER.warning(
                "PostgreSQL rejected the password from %s; connected with the "
                "last known good password. Expected while a password rotation "
                "is in progress (the role not altered yet).",
                source.describe(),
            )
            return connection
        source.remember(password)
        return connection

    _listen(engine, "do_connect", connect_with_rotating_password)
