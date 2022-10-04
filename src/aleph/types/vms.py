from enum import Enum


class CpuArchitecture(str, Enum):
    X86_64 = "x86_64"
    ARM64 = "arm64"
