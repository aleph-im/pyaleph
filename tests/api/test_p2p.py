import contextlib
import copy
import json
import logging
from typing import Any, Dict, Iterator, List

import pytest
from configmanager import Config

from aleph.web.middlewares.request_log import REQUEST_LOGGER_NAME

P2P_PUB_URI = "/api/v0/p2p/pubsub/pub"
POST_MESSAGES_URI = "/api/v0/messages"

MESSAGE_DICT = {
    "chain": "NULS2",
    "item_hash": "4bbcfe7c4775492c2e602d322d68f558891468927b5e0d6cb89ff880134f323e",
    "sender": "NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB",
    "type": "STORE",
    "channel": "MYALEPH",
    "item_content": '{"address":"NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB","item_type":"ipfs","item_hash":"QmUDS8mpQmpPyptyUEedHxHMkxo7ueRRiAvrpgvJMpjXwW","time":1577325086.513}',
    "item_type": "inline",
    "signature": "G7/xlWoMjjOr1NBN4SiZ8USYYVM9Q3JHXChR9hPw9/YSItfAplshWysqYDkvmBZiwbICG0IVB3ilMPJ/ZVgPNlk=",
    "time": 1608297193.717,
}


@pytest.mark.asyncio
async def test_pubsub_pub_valid_message(ccn_api_client, mock_config: Config):
    message_topic = mock_config.aleph.queue_topic.value

    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": json.dumps(MESSAGE_DICT)}
    )
    assert response.status == 200, await response.text()
    response_json = await response.json()

    assert response_json["status"] == "success"


@pytest.mark.asyncio
async def test_pubsub_pub_errors(ccn_api_client, mock_config: Config):
    # Invalid topic
    serialized_message_dict = json.dumps(MESSAGE_DICT)
    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": "random-topic", "data": serialized_message_dict}
    )
    assert response.status == 403, await response.text()

    message_topic = mock_config.aleph.queue_topic.value

    # Do not serialize the message
    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": MESSAGE_DICT}
    )
    assert response.status == 422, await response.text()

    # Invalid JSON
    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": serialized_message_dict[:-2]}
    )
    assert response.status == 422, await response.text()

    # Invalid message
    message_dict = copy.deepcopy(MESSAGE_DICT)
    del message_dict["item_content"]

    response = await ccn_api_client.post(
        P2P_PUB_URI, json={"topic": message_topic, "data": json.dumps(message_dict)}
    )
    assert response.status == 422, await response.text()


@pytest.mark.asyncio
@pytest.mark.skip(
    reason="this test is since the beginning and will be fixed in another PR"
)
async def test_post_message_sync(ccn_api_client, mocker):
    # Mock the functions used to create the RabbitMQ queue
    mocker.patch("aleph.web.controllers.utils.get_mq_channel_from_request")
    mocked_queue = mocker.patch(
        "aleph.web.controllers.utils.mq_make_aleph_message_topic_queue"
    )

    # Create a mock MQ response object
    mock_mq_message = mocker.Mock()
    mock_mq_message.routing_key = f"processed.{MESSAGE_DICT['item_hash']}"
    mocker.patch(
        "aleph.web.controllers.utils.mq_read_one_message", return_value=mock_mq_message
    )

    response = await ccn_api_client.post(
        POST_MESSAGES_URI,
        json={
            "message": MESSAGE_DICT,
            "sync": True,
        },
    )

    assert response.status == 200, await response.text()
    json_response = await response.json()
    pub_status = json_response["publication_status"]
    assert json_response["message_status"] == "processed"
    assert pub_status["status"] == "success"
    assert pub_status["failed"] == []

    # Check that we cleaned up the queue
    mocked_queue.return_value.delete.assert_awaited_once()


@contextlib.contextmanager
def captured_request_log() -> Iterator[List[Dict[str, Any]]]:
    """Collect the JSON lines written to the aleph.http request logger."""
    lines: List[Dict[str, Any]] = []

    class _Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(json.loads(record.getMessage()))

    handler = _Collect()
    logger = logging.getLogger(REQUEST_LOGGER_NAME)
    previous_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield lines
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


@pytest.mark.asyncio
async def test_pub_message_sets_sender(ccn_api_client):
    # Not mocked: broadcast_and_process_message is where the sender is set.
    with captured_request_log() as lines:
        response = await ccn_api_client.post(
            POST_MESSAGES_URI, json={"message": MESSAGE_DICT, "sync": False}
        )
        assert response.status == 202, await response.text()

    line = lines[-1]
    assert line["route"] == POST_MESSAGES_URI
    assert line["sender"] == MESSAGE_DICT["sender"]
    assert line["auth"] == "claimed"


@pytest.mark.asyncio
async def test_pub_json_sets_sender(ccn_api_client, mock_config: Config):
    message_topic = mock_config.aleph.queue_topic.value

    with captured_request_log() as lines:
        response = await ccn_api_client.post(
            P2P_PUB_URI,
            json={"topic": message_topic, "data": json.dumps(MESSAGE_DICT)},
        )
        assert response.status == 200, await response.text()

    line = lines[-1]
    assert line["route"] == P2P_PUB_URI
    assert line["sender"] == MESSAGE_DICT["sender"]
    assert line["auth"] == "claimed"


@pytest.mark.asyncio
async def test_rejected_pub_json_is_anonymous(ccn_api_client, mock_config: Config):
    with captured_request_log() as lines:
        response = await ccn_api_client.post(
            P2P_PUB_URI,
            json={"topic": "random-topic", "data": json.dumps(MESSAGE_DICT)},
        )
        assert response.status == 403, await response.text()

    line = lines[-1]
    assert line["sender"] is None
    assert line["auth"] == "anonymous"
