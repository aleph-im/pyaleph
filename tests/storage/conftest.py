import pytest
from configmanager import Config
from aleph.config import get_defaults
from aleph.web import app


@pytest.fixture
def mock_config(mocker):
    config = Config(get_defaults())
    # To test handle_new_storage
    config.storage.store_files.value = True

    mock_config = mocker.patch.dict(app, {"config": config})
    return mock_config
