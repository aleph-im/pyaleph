from __future__ import annotations


class AlephException(Exception): ...


class AlephStorageException(AlephException):
    """
    Base exception class for all errors related to the storage
    and retrieval of Aleph messages.
    """

    ...


class InvalidConfigException(AlephException): ...


class KeyNotFoundException(AlephException): ...


class InvalidContent(AlephStorageException):
    """
    The content requested by the user is invalid. Examples:
    * its integrity is compromised
    * it does not match the Aleph message specification.
    """

    ...


class ContentCurrentlyUnavailable(AlephStorageException):
    """
    The content is currently unavailable, for example because of a
    synchronisation issue.
    """

    ...


class UnknownHashError(AlephException): ...
