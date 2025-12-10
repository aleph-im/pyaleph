import json
from unittest.mock import MagicMock, patch

import pytest

from aleph.web.controllers.accounts import (
    get_account_channels,
    get_account_post_types,
    get_resource_consumed_credits_controller,
)


@pytest.mark.asyncio
async def test_get_resource_consumed_credits_controller_success():
    """Test successful retrieval of consumed credits for a resource."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"item_hash": "test_hash_123"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock consumed credits value
    expected_consumed_credits = 42

    with (
        patch(
            "aleph.web.controllers.accounts.get_item_hash_str_from_request"
        ) as mock_get_hash,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_resource_consumed_credits"
        ) as mock_get_credits,
    ):

        # Set up mocks
        mock_get_hash.return_value = "test_hash_123"
        mock_get_factory.return_value = mock_session_factory
        mock_get_credits.return_value = expected_consumed_credits

        # Call the controller
        response = await get_resource_consumed_credits_controller(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["item_hash"] == "test_hash_123"
        assert response_data["consumed_credits"] == expected_consumed_credits

        # Verify mocks were called correctly
        mock_get_hash.assert_called_once_with(mock_request)
        mock_get_factory.assert_called_once_with(mock_request)
        mock_get_credits.assert_called_once_with(
            session=mock_session, item_hash="test_hash_123"
        )


@pytest.mark.asyncio
async def test_get_resource_consumed_credits_controller_zero_credits():
    """Test retrieval when resource has zero consumed credits."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"item_hash": "empty_hash_456"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock zero consumed credits
    expected_consumed_credits = 0

    with (
        patch(
            "aleph.web.controllers.accounts.get_item_hash_str_from_request"
        ) as mock_get_hash,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_resource_consumed_credits"
        ) as mock_get_credits,
    ):

        # Set up mocks
        mock_get_hash.return_value = "empty_hash_456"
        mock_get_factory.return_value = mock_session_factory
        mock_get_credits.return_value = expected_consumed_credits

        # Call the controller
        response = await get_resource_consumed_credits_controller(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["item_hash"] == "empty_hash_456"
        assert response_data["consumed_credits"] == 0


@pytest.mark.asyncio
async def test_get_resource_consumed_credits_controller_large_credits():
    """Test retrieval with a large consumed credits value."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"item_hash": "large_hash_789"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock large consumed credits value
    expected_consumed_credits = 999999

    with (
        patch(
            "aleph.web.controllers.accounts.get_item_hash_str_from_request"
        ) as mock_get_hash,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_resource_consumed_credits"
        ) as mock_get_credits,
    ):

        # Set up mocks
        mock_get_hash.return_value = "large_hash_789"
        mock_get_factory.return_value = mock_session_factory
        mock_get_credits.return_value = expected_consumed_credits

        # Call the controller
        response = await get_resource_consumed_credits_controller(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["item_hash"] == "large_hash_789"
        assert response_data["consumed_credits"] == expected_consumed_credits


@pytest.mark.asyncio
async def test_get_account_post_types_controller_success():
    """Test successful retrieval of post_types for an address."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"address": "0xTestAddress123"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock post_types list
    expected_post_types = ["blog", "news", "tutorial"]

    with (
        patch(
            "aleph.web.controllers.accounts._get_address_from_request"
        ) as mock_get_address,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_distinct_post_types_for_address"
        ) as mock_get_post_types,
    ):

        # Set up mocks
        mock_get_address.return_value = "0xTestAddress123"
        mock_get_factory.return_value = mock_session_factory
        mock_get_post_types.return_value = expected_post_types

        # Call the controller
        response = await get_account_post_types(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["address"] == "0xTestAddress123"
        assert response_data["post_types"] == expected_post_types

        # Verify mocks were called correctly
        mock_get_address.assert_called_once_with(mock_request)
        mock_get_factory.assert_called_once_with(mock_request)
        mock_get_post_types.assert_called_once_with(
            session=mock_session, address="0xTestAddress123"
        )


@pytest.mark.asyncio
async def test_get_account_post_types_controller_empty():
    """Test retrieval when address has no post_types."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"address": "0xEmptyAddress456"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock empty post_types list
    expected_post_types = []

    with (
        patch(
            "aleph.web.controllers.accounts._get_address_from_request"
        ) as mock_get_address,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_distinct_post_types_for_address"
        ) as mock_get_post_types,
    ):

        # Set up mocks
        mock_get_address.return_value = "0xEmptyAddress456"
        mock_get_factory.return_value = mock_session_factory
        mock_get_post_types.return_value = expected_post_types

        # Call the controller
        response = await get_account_post_types(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["address"] == "0xEmptyAddress456"
        assert response_data["post_types"] == []


@pytest.mark.asyncio
async def test_get_account_post_types_controller_single_type():
    """Test retrieval when address has only one post_type."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"address": "0xSingleType789"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock single post_type
    expected_post_types = ["blog"]

    with (
        patch(
            "aleph.web.controllers.accounts._get_address_from_request"
        ) as mock_get_address,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_distinct_post_types_for_address"
        ) as mock_get_post_types,
    ):

        # Set up mocks
        mock_get_address.return_value = "0xSingleType789"
        mock_get_factory.return_value = mock_session_factory
        mock_get_post_types.return_value = expected_post_types

        # Call the controller
        response = await get_account_post_types(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["address"] == "0xSingleType789"
        assert response_data["post_types"] == ["blog"]


@pytest.mark.asyncio
async def test_get_account_channels_controller_success():
    """Test successful retrieval of channels for an address."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"address": "0xTestAddress123"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock channels list
    expected_channels = ["channel1", "channel2", "channel3"]

    with (
        patch(
            "aleph.web.controllers.accounts._get_address_from_request"
        ) as mock_get_address,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_distinct_channels_for_address"
        ) as mock_get_channels,
    ):

        # Set up mocks
        mock_get_address.return_value = "0xTestAddress123"
        mock_get_factory.return_value = mock_session_factory
        mock_get_channels.return_value = expected_channels

        # Call the controller
        response = await get_account_channels(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["address"] == "0xTestAddress123"
        assert response_data["channels"] == expected_channels

        # Verify mocks were called correctly
        mock_get_address.assert_called_once_with(mock_request)
        mock_get_factory.assert_called_once_with(mock_request)
        mock_get_channels.assert_called_once_with(
            session=mock_session, address="0xTestAddress123"
        )


@pytest.mark.asyncio
async def test_get_account_channels_controller_empty():
    """Test retrieval when address has no channels."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"address": "0xEmptyAddress456"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock empty channels list
    expected_channels = []

    with (
        patch(
            "aleph.web.controllers.accounts._get_address_from_request"
        ) as mock_get_address,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_distinct_channels_for_address"
        ) as mock_get_channels,
    ):

        # Set up mocks
        mock_get_address.return_value = "0xEmptyAddress456"
        mock_get_factory.return_value = mock_session_factory
        mock_get_channels.return_value = expected_channels

        # Call the controller
        response = await get_account_channels(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["address"] == "0xEmptyAddress456"
        assert response_data["channels"] == []


@pytest.mark.asyncio
async def test_get_account_channels_controller_single_channel():
    """Test retrieval when address has only one channel."""
    # Mock request object
    mock_request = MagicMock()
    mock_request.match_info = {"address": "0xSingleChannel789"}

    # Mock session factory and session
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session
    mock_session_factory.return_value.__exit__.return_value = None

    # Mock single channel
    expected_channels = ["single_channel"]

    with (
        patch(
            "aleph.web.controllers.accounts._get_address_from_request"
        ) as mock_get_address,
        patch(
            "aleph.web.controllers.accounts.get_session_factory_from_request"
        ) as mock_get_factory,
        patch(
            "aleph.web.controllers.accounts.get_distinct_channels_for_address"
        ) as mock_get_channels,
    ):

        # Set up mocks
        mock_get_address.return_value = "0xSingleChannel789"
        mock_get_factory.return_value = mock_session_factory
        mock_get_channels.return_value = expected_channels

        # Call the controller
        response = await get_account_channels(mock_request)

        # Verify the response
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data["address"] == "0xSingleChannel789"
        assert response_data["channels"] == ["single_channel"]
