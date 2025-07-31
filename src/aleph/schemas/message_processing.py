import json
from typing import Any, List, Literal, TypeVar, Union

from pydantic import BaseModel, field_serializer, field_validator

from aleph.types.message_processing_result import MessageProcessingResult

T = TypeVar("T")


class BaseWorkerPayload(BaseModel):
    type: str


class SingleMessagePayload(BaseWorkerPayload):
    type: Literal["single"]
    message_id: int
    item_hash: str
    sender: str


class BatchMessagePayload(BaseWorkerPayload):
    type: Literal["batch"]
    message_ids: List[int]
    item_hashes: List[str]
    sender: str


WorkerPayload = Union[
    SingleMessagePayload,
    BatchMessagePayload,
]


class BaseResultPayload(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    type: str
    # Use Any for the model definition but provide type hints for serializers/validators
    result: Any  # type: MessageProcessingResult
    sender: str
    processing_time: float

    @field_serializer("result")
    def serialize_result(self, value: Any, _info):
        return value.to_dict()

    @field_validator("result", mode="before")
    def parse_result(cls, value):
        if isinstance(value, dict):
            return MessageProcessingResult.from_dict(value)
        return value


class SingleResultPayload(BaseResultPayload):
    model_config = {"arbitrary_types_allowed": True}
    type: Literal["single"]
    # Inherit the result field from parent


class BatchResultPayload(BaseResultPayload):
    model_config = {"arbitrary_types_allowed": True}
    type: Literal["batch"]
    is_last: bool
    # Inherit the result field from parent


ResultPayload = Union[SingleResultPayload, BatchResultPayload]


def parse_worker_payload(raw_data: str) -> WorkerPayload:
    """
    Parse a JSON string into the appropriate WorkerPayload model.

    Args:
        raw_data: JSON string containing the worker payload

    Returns:
        WorkerPayload instance (either SingleMessagePayload or BatchMessagePayload)

    Raises:
        ValueError: If the payload type is unknown or JSON is invalid
    """
    try:
        data = json.loads(raw_data)
        payload_type = data.get("type")

        if payload_type == "single":
            return SingleMessagePayload.model_validate(data)
        elif payload_type == "batch":
            return BatchMessagePayload.model_validate(data)
        else:
            raise ValueError(f"Unknown payload type: {payload_type}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")
    except Exception as e:
        raise ValueError(f"Failed to parse payload: {e}")


def parse_result_payload(raw_data: str) -> ResultPayload:
    """
    Parse a JSON string into the appropriate ResultPayload model.

    Args:
        raw_data: JSON string containing the result payload

    Returns:
        ResultPayload instance (either SingleResultPayload or BatchResultPayload)

    Raises:
        ValueError: If the payload type is unknown or JSON is invalid
    """
    try:
        data = json.loads(raw_data)
        payload_type = data.get("type")

        if payload_type == "single":
            return SingleResultPayload.model_validate(data)
        elif payload_type == "batch":
            return BatchResultPayload.model_validate(data)
        else:
            raise ValueError(f"Unknown payload type: {payload_type}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")
    except Exception as e:
        raise ValueError(f"Failed to parse payload: {e}")
