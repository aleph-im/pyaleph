from enum import Enum
from typing import NewType

ProgramVersion = NewType("ProgramVersion", str)


class CpuArchitecture(str, Enum):
    X86_64 = "x86_64"
    ARM64 = "arm64"
