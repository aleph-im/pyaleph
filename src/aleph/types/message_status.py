from enum import Enum, IntEnum
from typing import Optional, Any, Dict, Sequence, Union
from decimal import Decimal


class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FORGOTTEN = "forgotten"


class MessageProcessingStatus(str, Enum):
    PROCESSED_NEW_MESSAGE = "processed"
    PROCESSED_CONFIRMATION = "confirmed"
    FAILED_WILL_RETRY = "retry"
    FAILED_REJECTED = "rejected"

    def to_message_status(self) -> MessageStatus:
        if self == self.PROCESSED_CONFIRMATION or self == self.PROCESSED_NEW_MESSAGE:
            return MessageStatus.PROCESSED
        elif self == self.FAILED_WILL_RETRY:
            return MessageStatus.PENDING
        else:
            return MessageStatus.REJECTED


class ErrorCode(IntEnum):
    INTERNAL_ERROR = -1
    INVALID_FORMAT = 0
    INVALID_SIGNATURE = 1
    PERMISSION_DENIED = 2
    CONTENT_UNAVAILABLE = 3
    FILE_UNAVAILABLE = 4
    BALANCE_INSUFFICIENT = 5
    POST_AMEND_NO_TARGET = 100
    POST_AMEND_TARGET_NOT_FOUND = 101
    POST_AMEND_AMEND = 102
    STORE_REF_NOT_FOUND = 200
    STORE_UPDATE_UPDATE = 201
    VM_REF_NOT_FOUND = 300
    VM_VOLUME_NOT_FOUND = 301
    VM_AMEND_NOT_ALLOWED = 302
    VM_UPDATE_UPDATE = 303
    VM_VOLUME_TOO_SMALL = 304
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


class StoreRefNotFound(RetryMessageException):
    """
    The original store message hash specified in the `ref` field could not be found.
    """

    error_code = ErrorCode.STORE_REF_NOT_FOUND


class StoreCannotUpdateStoreWithRef(InvalidMessageException):
    """
    The store message targeted by the `ref` field has a value in the `ref` field itself.
    Update trees are not supported.
    """

    error_code = ErrorCode.STORE_UPDATE_UPDATE


class VmRefNotFound(RetryMessageException):
    """
    The original program specified in the `ref` field could not be found.
    """

    error_code = ErrorCode.VM_REF_NOT_FOUND


class VmVolumeNotFound(RetryMessageException):
    """
    One or more volume files could not be found.
    """

    error_code = ErrorCode.VM_VOLUME_NOT_FOUND


class VmUpdateNotAllowed(InvalidMessageException):
    """
    The message attempts to amend an immutable program, i.e. for which allow_amend
    is set to False.
    """

    error_code = ErrorCode.VM_AMEND_NOT_ALLOWED


class VmCannotUpdateUpdate(InvalidMessageException):
    """
    The program hash in the `replaces` field has a value for the `replaces` field
    itself. Update trees are not supported.
    """

    error_code = ErrorCode.VM_UPDATE_UPDATE


class VmVolumeTooSmall(InvalidMessageException):
    """
    A volume with a parent volume has a size inferior to the size of the parent.
    Ex: attempting to use a 4GB Ubuntu rootfs to a 2GB volume.
    """

    error_code = ErrorCode.VM_VOLUME_TOO_SMALL

    def __init__(
        self,
        volume_name: str,
        volume_size: int,
        parent_ref: str,
        parent_file: str,
        parent_size: int,
    ):
        self.volume_name = volume_name
        self.volume_size = volume_size
        self.parent_ref = parent_ref
        self.parent_file = parent_file
        self.parent_size = parent_size

    def details(self) -> Optional[Dict[str, Any]]:
        return {
            "errors": [
                {
                    "volume_name": self.volume_name,
                    "parent_ref": self.parent_ref,
                    "parent_file": self.parent_file,
                    "parent_size": self.parent_size,
                    "volume_size": self.volume_size,
                }
            ]
        }


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


class InsufficientBalanceException(InvalidMessageException):
    """
    You don't have enough in balance
    """

    def __init__(
        self,
        balance: float,
        required_balance: float,
    ):
        self.balance = balance
        self.required_balance = required_balance
        super().__init__(
            f"Insufficient balances : {self.balance} required : {self.required_balance}"
        )

    error_code = ErrorCode.BALANCE_INSUFFICIENT
