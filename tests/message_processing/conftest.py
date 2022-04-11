import pytest

from .load_fixtures import load_fixture_messages


@pytest.fixture
def fixture_messages():
    return load_fixture_messages("test-data-pending-tx-messages.json")
