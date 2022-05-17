from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Union


class ContentSource(str, Enum):
    """
    Defines the source of the content of a message.

    Message content can be fetched from different sources depending on the procedure followed by the user sending
    a particular message. This enum determines where the node found the content.
    """

    DB = "DB"
    P2P = "P2P"
    IPFS = "IPFS"
    INLINE = "inline"


@dataclass
class StoredContent:
    hash: str
    source: Optional[ContentSource]


@dataclass
class RawContent(StoredContent):
    value: bytes

    def __len__(self):
        return len(self.value)


@dataclass
class MessageContent(StoredContent):
    value: Any
    raw_value: Union[bytes, str]
