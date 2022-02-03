class AlephException(Exception):
    ...


class InvalidConfigException(AlephException):
    ...


class InvalidKeyDirException(AlephException):
    ...


class KeyNotFoundException(AlephException):
    ...
