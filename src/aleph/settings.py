from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class Settings:
    use_executors: bool = True


# Singleton
settings = Settings()
