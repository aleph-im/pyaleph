from typing import Any, Dict, Optional, Protocol

from aleph.schemas.api.messages import BaseMessage, PendingMessage, format_message_dict
from aleph.types.message_status import ErrorCode, MessageOrigin, MessageProcessingStatus


class MessageProcessingResult(Protocol):
    status: MessageProcessingStatus
    origin: Optional[MessageOrigin] = None

    @property
    def item_hash(self) -> str:
        pass

    def to_dict(self) -> Dict[str, Any]:
        pass

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageProcessingResult":
        raw_status = data.get("status")

        if raw_status is None:
            raise ValueError("Missing status in result data")

        try:
            status = MessageProcessingStatus(raw_status)
        except ValueError:
            raise ValueError(f"Invalid status: {raw_status}")

        if status in (
            MessageProcessingStatus.PROCESSED_NEW_MESSAGE,
            MessageProcessingStatus.PROCESSED_CONFIRMATION,
        ):
            return ProcessedMessage.from_dict(data)

        elif status in (
            MessageProcessingStatus.FAILED_WILL_RETRY,
            MessageProcessingStatus.FAILED_REJECTED,
        ):
            return FailedMessage.from_dict(data)


class ProcessedMessage(MessageProcessingResult):
    def __init__(
        self,
        message: BaseMessage,
        is_confirmation: bool = False,
        origin: Optional[MessageOrigin] = None,
    ):
        self.message = message
        self.status = (
            MessageProcessingStatus.PROCESSED_CONFIRMATION
            if is_confirmation
            else MessageProcessingStatus.PROCESSED_NEW_MESSAGE
        )
        self.origin = origin

    @property
    def item_hash(self) -> str:
        return self.message.item_hash

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "message": self.message.model_dump(),
            "origin": self.origin.value if self.origin else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProcessedMessage":
        message = data.get("message")

        raw_status = data.get("status")
        try:
            status = MessageProcessingStatus(raw_status)
        except ValueError:
            raise ValueError(f"Invalid status value: {raw_status}")

        is_confirmation = status == MessageProcessingStatus.PROCESSED_CONFIRMATION
        new_message = format_message_dict(message)

        raw_origin = data.get("origin")
        origin = MessageOrigin(raw_origin) if raw_origin is not None else None
        return cls(message=new_message, is_confirmation=is_confirmation, origin=origin)


class FailedMessage(MessageProcessingResult):
    def __init__(
        self, pending_message: PendingMessage, error_code: ErrorCode, will_retry: bool
    ):
        self.pending_message = pending_message
        self.error_code = error_code
        self.origin = getattr(pending_message, "origin", None)

        self.status = (
            MessageProcessingStatus.FAILED_WILL_RETRY
            if will_retry
            else MessageProcessingStatus.FAILED_REJECTED
        )

    @property
    def item_hash(self) -> str:
        return self.pending_message.item_hash

    def to_dict(self) -> Dict[str, Any]:
        # Handle origin correctly whether it's a string or an enum
        origin_value = None
        if hasattr(self, "origin") and self.origin:
            if hasattr(self.origin, "value"):
                origin_value = self.origin.value
            else:
                origin_value = self.origin

        return {
            "status": self.status.value,
            "pending_message": self.pending_message.model_dump(),
            "origin": origin_value,
            "error_code": self.error_code.value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FailedMessage":
        raw_status = data.get("status")
        raw_item = data.get("pending_message")
        raw_error = data.get("error_code")

        try:
            status = MessageProcessingStatus(raw_status)
        except ValueError:
            raise ValueError(f"Invalid status: {raw_status}")
        will_retry = status == MessageProcessingStatus.FAILED_WILL_RETRY

        pending_message = None
        if raw_item:
            pending_message = PendingMessage.model_validate(raw_item)

        error_code = ErrorCode(raw_error)

        return cls(
            pending_message=pending_message if pending_message is not None else None,
            error_code=error_code,
            will_retry=will_retry,
        )


class WillRetryMessage(FailedMessage):
    def __init__(self, pending_message: PendingMessage, error_code: ErrorCode):
        super().__init__(pending_message, error_code, will_retry=True)


class RejectedMessage(FailedMessage):
    def __init__(self, pending_message: PendingMessage, error_code: ErrorCode):
        super().__init__(pending_message, error_code, will_retry=False)
