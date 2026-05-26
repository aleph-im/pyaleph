from enum import Enum


class ContentFormat(str, Enum):
    """Level of message ``content`` detail returned by the messages API.

    * ``full``    - the complete content (default).
    * ``headers`` - a reduced, per-type metadata subset built from
                    denormalized columns; the content JSONB is not read.
    * ``none``    - content omitted entirely (the behaviour of the
                    deprecated ``excludeContent=true`` flag).
    """

    FULL = "full"
    HEADERS = "headers"
    NONE = "none"
