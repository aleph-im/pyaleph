from enum import Enum
from typing import NewType

FileTag = NewType("FileTag", str)


class FileType(str, Enum):
    FILE = "file"
    DIRECTORY = "dir"
