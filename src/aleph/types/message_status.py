from decimal import Decimal
from enum import Enum, IntEnum
from typing import Any, Dict, Optional, Sequence, Union


class MessageOrigin(str, Enum):
    ONCHAIN = "onchain"
    P2P = "p2p"
    IPFS = "ipfs"


class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FORGOTTEN = "forgotten"
    REMOVING = "removing"
    REMOVED = "removed"


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
    FORGET_NOT_ALLOWED = 503
    FORGOTTEN_DUPLICATE = 504


class RemovedMessageReason(str, Enum):
    BALANCE_INSUFFICIENT = "balance_insufficient"


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

    def __str__(self):
        # TODO: reimplement for each exception subtype
        return self.__class__.__name__

    def details(self) -> Optional[Dict[str, Any]]:
        """
        Return error details in a JSON serializable format.

        Returns:
            Dictionary with error details or None if no errors.
        """
        errors = self.args[0]

        # Ensure errors are JSON serializable
        if errors:
            # Convert non-serializable objects to strings if needed
            serializable_errors = []
            for err in errors:
                try:
                    # Test if the error is JSON serializable by attempting to convert to dict
                    # This will fail for custom objects
                    if hasattr(err, "__dict__"):
                        serializable_errors.append(str(err))
                    else:
                        serializable_errors.append(err)
                except (TypeError, ValueError):
                    # If conversion fails, use string representation
                    serializable_errors.append(str(err))

            return {"errors": serializable_errors}
        return None


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


class ForgetNotAllowed(InvalidMessageException):
    """
    The store message targeted by the `ref` field has a value in the `ref` field of a dependent volume.
    """

    def __init__(self, file_hash: str, vm_hash: str):
        super().__init__(f"File {file_hash} used on vm {vm_hash}")

    error_code = ErrorCode.FORGET_NOT_ALLOWED


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
        """
        Return error details in a JSON serializable format.

        Returns:
            Dictionary with error details.
        """
        # Ensure all values are JSON serializable
        return {
            "errors": [
                {
                    "volume_name": str(self.volume_name),
                    "parent_ref": str(self.parent_ref),
                    "parent_file": str(self.parent_file),
                    "parent_size": int(self.parent_size),
                    "volume_size": int(self.volume_size),
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
        """
        Return error details in a JSON serializable format.

        Returns:
            Dictionary with error details.
        """
        errors = []
        if self.target_hash is not None:
            errors.append({"message": str(self.target_hash)})
        if self.aggregate_key is not None:
            errors.append({"aggregate": str(self.aggregate_key)})

        return {"errors": errors}


class CannotForgetForgetMessage(InvalidMessageException):
    """
    The FORGET message targets another FORGET message, which is forbidden.
    """

    error_code = ErrorCode.FORGET_FORGET

    def __init__(self, target_hash: str):
        self.target_hash = target_hash

    def details(self) -> Optional[Dict[str, Any]]:
        """
        Return error details in a JSON serializable format.

        Returns:
            Dictionary with error details.
        """
        return {"errors": [{"message": str(self.target_hash)}]}


class InsufficientBalanceException(InvalidMessageException):
    """
    The user does not have enough Aleph tokens to process the message.
    """

    error_code = ErrorCode.BALANCE_INSUFFICIENT

    def __init__(
        self,
        balance: Decimal,
        required_balance: Decimal,
    ):
        self.balance = balance
        self.required_balance = required_balance

    def details(self) -> Optional[Dict[str, Any]]:
        """
        Return error details in a JSON serializable format.

        Returns:
            Dictionary with error details.
        """
        # Note: cast to string to keep the precision and ensure it's JSON serializable
        return {
            "errors": [
                {
                    "required_balance": str(self.required_balance),
                    "account_balance": str(self.balance),
                }
            ]
        }
