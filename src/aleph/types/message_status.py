from enum import Enum, IntEnum
from typing import Optional, Any, Dict, Sequence, Union


class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FORGOTTEN = "forgotten"


class MessageProcessingStatus(IntEnum):
    NEW_MESSAGE = 1
    NEW_CONFIRMATION = 2
    MESSAGE_ALREADY_PROCESSED = 3


class ErrorCode(IntEnum):
    INTERNAL_ERROR = -1
    INVALID_FORMAT = 0
    INVALID_SIGNATURE = 1
    PERMISSION_DENIED = 2
    CONTENT_UNAVAILABLE = 3
    FILE_UNAVAILABLE = 4
    POST_AMEND_NO_TARGET = 100
    POST_AMEND_TARGET_NOT_FOUND = 101
    POST_AMEND_AMEND = 102
    FORGET_NO_TARGET = 500
    FORGET_TARGET_NOT_FOUND = 501
    FORGET_FORGET = 502


class MessageProcessingException(Exception):
    error_code: ErrorCode

    def __init__(
        self,
        errors: Optional[Union[str, Sequence[Any]]] = None,
    ):
        if errors is None:
            errors = []
        elif isinstance(errors, str):
            errors = [errors]
        super().__init__(errors)

    def details(self) -> Optional[Dict[str, Any]]:
        errors = self.args[0]
        return {"errors": errors} if errors else None


class InvalidMessageException(MessageProcessingException):
    """
    The message is invalid and should be rejected.
    """

    ...


class RetryMessageException(MessageProcessingException):
    """
    The message should be retried.
    """

    ...


class InternalError(RetryMessageException):
    """
    An unexpected situation occurred.
    """

    error_code = ErrorCode.INTERNAL_ERROR


class InvalidMessageFormat(InvalidMessageException):
    """
    The message is invalid because it is not properly formatted:
    missing field(s), incorrect value types, etc.
    """

    error_code = ErrorCode.INVALID_FORMAT


class InvalidSignature(InvalidMessageException):
    """
    The message is invalid, in particular because its signature does not
    match the expected value.
    """

    error_code = ErrorCode.INVALID_SIGNATURE


class PermissionDenied(InvalidMessageException):
    """
    The sender does not have the permission to perform the requested operation
    on the specified object.
    """

    error_code = ErrorCode.PERMISSION_DENIED


class MissingDependency(RetryMessageException):
    """
    An object targeted by the message is missing.
    """

    ...


class FileNotFoundException(RetryMessageException):
    """
    A file required to process the message could not be found, locally and/or
    on the network.
    """

    def __init__(self, file_hash: str):
        super().__init__(f"File not found: {file_hash}")


class MessageContentUnavailable(FileNotFoundException):
    """
    The message content is not available at the moment (storage/IPFS item types).
    """

    error_code = ErrorCode.CONTENT_UNAVAILABLE


class FileUnavailable(FileNotFoundException):
    """
    A file pointed to by the message is not available at the moment.
    """

    error_code = ErrorCode.FILE_UNAVAILABLE


class NoAmendTarget(InvalidMessageException):
    """
    A POST with type = amend does not specify a value in the ref field.
    """

    error_code = ErrorCode.POST_AMEND_NO_TARGET


class AmendTargetNotFound(RetryMessageException):
    """
    The original post for an amend could not be found.
    """

    error_code = ErrorCode.POST_AMEND_TARGET_NOT_FOUND


class CannotAmendAmend(InvalidMessageException):
    """
    The original post targeted by an amend is an amend itself, which is forbidden.
    """

    error_code = ErrorCode.POST_AMEND_AMEND


class NoForgetTarget(InvalidMessageException):
    """
    The FORGET message specifies nothing to forget.
    """

    error_code = ErrorCode.FORGET_NO_TARGET


class ForgetTargetNotFound(RetryMessageException):
    """
    A target specified in the FORGET message could not be found.
    """

    error_code = ErrorCode.FORGET_TARGET_NOT_FOUND

    def __init__(
        self, target_hash: Optional[str] = None, aggregate_key: Optional[str] = None
    ):
        self.target_hash = target_hash
        self.aggregate_key = aggregate_key

    def details(self) -> Optional[Dict[str, Any]]:
        errors = []
        if self.target_hash is not None:
            errors.append({"message": self.target_hash})
        if self.aggregate_key is not None:
            errors.append({"aggregate": self.aggregate_key})

        return {"errors": errors}


class CannotForgetForgetMessage(InvalidMessageException):
    """
    The FORGET message targets another FORGET message, which is forbidden.
    """

    error_code = ErrorCode.FORGET_FORGET

    def __init__(self, target_hash: str):
        self.target_hash = target_hash

    def details(self) -> Optional[Dict[str, Any]]:
        return {"errors": [{"message": self.target_hash}]}
