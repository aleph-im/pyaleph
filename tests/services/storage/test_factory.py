from pathlib import Path

import pytest
from configmanager import Config

import aleph.config
from aleph.services.storage.factory import make_storage_engine
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.services.storage.s3_engine import S3StorageEngine


@pytest.fixture
def config(tmp_path: Path) -> Config:
    config = Config(aleph.config.get_defaults())
    config.storage.folder.value = str(tmp_path / "files")
    return config


def _configure_s3(config: Config) -> None:
    config.storage.engine.value = "s3"
    config.storage.s3.endpoint_url.value = "https://s3.garage.svc"
    config.storage.s3.bucket.value = "aleph-files"
    config.storage.s3.prefix.value = "node-1/"
    config.storage.s3.access_key_id.value = "GK0123"
    config.storage.s3.secret_access_key.value = "inline-secret"


def test_default_engine_is_filesystem(config: Config, tmp_path: Path):
    assert config.storage.engine.value == "filesystem"
    engine = make_storage_engine(config)
    assert isinstance(engine, FileSystemStorageEngine)
    assert engine.folder == tmp_path / "files"


def test_s3_defaults(config: Config):
    assert config.storage.s3.region.value == "garage"
    assert config.storage.s3.prefix.value == ""
    assert config.storage.s3.ca_bundle.value is None


@pytest.mark.asyncio
async def test_s3_engine_built_from_config(config: Config, tmp_path: Path):
    _configure_s3(config)
    secret_file = tmp_path / "secret"
    secret_file.write_text("secret-from-file\n")
    config.storage.s3.secret_access_key_file.value = str(secret_file)
    config.storage.s3.ca_bundle.value = "/etc/ssl/garage-ca.pem"

    engine = make_storage_engine(config)
    try:
        assert isinstance(engine, S3StorageEngine)
        assert engine.endpoint_url == "https://s3.garage.svc"
        assert engine.region == "garage"
        assert engine.bucket == "aleph-files"
        assert engine.prefix == "node-1/"
        assert engine.ca_bundle == "/etc/ssl/garage-ca.pem"
        # The file wins over the inline value, trailing newline stripped.
        assert engine._secret_access_key == "secret-from-file"
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_s3_engine_inline_secret(config: Config):
    _configure_s3(config)
    engine = make_storage_engine(config)
    try:
        assert isinstance(engine, S3StorageEngine)
        assert engine._secret_access_key == "inline-secret"
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_s3_credentials_from_environment(
    config: Config, monkeypatch: pytest.MonkeyPatch
):
    _configure_s3(config)
    config.storage.s3.access_key_id.value = None
    config.storage.s3.secret_access_key.value = None
    monkeypatch.setenv("STORAGE_S3_ACCESS_KEY_ID", "GKfromenv")
    monkeypatch.setenv("STORAGE_S3_SECRET_ACCESS_KEY", "secret-from-env")
    engine = make_storage_engine(config)
    try:
        assert isinstance(engine, S3StorageEngine)
        assert engine._access_key_id == "GKfromenv"
        assert engine._secret_access_key == "secret-from-env"
    finally:
        await engine.close()


def test_s3_engine_missing_secret_file_raises(config: Config, tmp_path: Path):
    from aleph.toolkit.credentials import CredentialFileError

    _configure_s3(config)
    config.storage.s3.secret_access_key_file.value = str(tmp_path / "absent")
    with pytest.raises(CredentialFileError):
        make_storage_engine(config)


def test_s3_engine_requires_bucket(config: Config):
    _configure_s3(config)
    config.storage.s3.bucket.value = None
    with pytest.raises(ValueError, match="storage.s3.bucket"):
        make_storage_engine(config)


def test_unknown_engine_raises(config: Config):
    config.storage.engine.value = "ftp"
    with pytest.raises(ValueError, match="storage.engine"):
        make_storage_engine(config)
