from enum import Enum
from typing import NewType

VmVersion = NewType("VmVersion", str)


class VmType(str, Enum):
    INSTANCE = "instance"
    PROGRAM = "program"


class CpuArchitecture(str, Enum):
    X86_64 = "x86_64"
    ARM64 = "arm64"
