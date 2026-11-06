"""Build the storage engine selected by ``storage.engine``."""

from configmanager import Config

from aleph.toolkit.credentials import read_credential

from .engine import StorageEngine
from .fileystem_engine import FileSystemStorageEngine
from .s3_engine import S3StorageEngine

STORAGE_ENGINES = ("filesystem", "s3")


def _make_s3_storage_engine(config: Config) -> S3StorageEngine:
    s3_config = config.storage.s3
    secret_access_key = read_credential(
        s3_config.secret_access_key.value,
        None,
        s3_config.secret_access_key_file.value,
        what="storage.s3.secret_access_key",
    )
    required = {
        "storage.s3.endpoint_url": s3_config.endpoint_url.value,
        "storage.s3.bucket": s3_config.bucket.value,
        "storage.s3.access_key_id": s3_config.access_key_id.value,
        "storage.s3.secret_access_key": secret_access_key,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(f"storage.engine is 's3' but {', '.join(missing)} is not set")

    return S3StorageEngine(
        endpoint_url=s3_config.endpoint_url.value,
        region=s3_config.region.value,
        bucket=s3_config.bucket.value,
        prefix=s3_config.prefix.value or "",
        access_key_id=s3_config.access_key_id.value,
        secret_access_key=secret_access_key,
        ca_bundle=s3_config.ca_bundle.value or None,
    )


def make_storage_engine(config: Config) -> StorageEngine:
    """The storage engine configured in ``storage.engine``.

    The S3 secret is resolved here, once (file > environment > inline).
    """
    engine_name = config.storage.engine.value
    if engine_name == "filesystem":
        return FileSystemStorageEngine(folder=config.storage.folder.value)
    if engine_name == "s3":
        return _make_s3_storage_engine(config)
    raise ValueError(
        f"Unknown storage.engine {engine_name!r}: expected one of "
        f"{', '.join(STORAGE_ENGINES)}"
    )
