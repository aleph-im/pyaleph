import json

import pytest

SENDER = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"

MESSAGE = {
    "chain": "ETH",
    "channel": "TEST",
    "sender": SENDER,
    "type": "STORE",
    "time": 1652794362.573859,
    "item_type": "inline",
    "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652794362.5736332,"item_type":"storage","item_hash":"5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21","mime_type":"text/plain"}',
    "item_hash": "f6fc4884e3ec3624bd3f60a3c37abf83a130777086061b1a373e659f2bab4d06",
    "signature": "0x7b87c29388a7a452353f9cae8718b66158fb5bdc93f032964226745ee04919092550791b93f79e5ee1981f2d9d6e5ac0cae0d28b68bb63fe0fcbd79015a6f3ea1b",
}


@pytest.mark.asyncio
async def test_pub_message_rejects_blocked_sender(mock_config, ccn_api_client, mocker):
    mock_config.aleph.blocked_senders.value = [SENDER.lower()]
    pub_p2p = mocker.patch("aleph.web.controllers.utils.pub_p2p")

    response = await ccn_api_client.post("/api/v0/messages", json={"message": MESSAGE})

    assert response.status == 403, await response.text()
    pub_p2p.assert_not_called()


@pytest.mark.asyncio
async def test_pub_json_rejects_blocked_sender(mock_config, ccn_api_client, mocker):
    mock_config.aleph.blocked_senders.value = [SENDER.upper()]
    pub_p2p = mocker.patch("aleph.web.controllers.p2p.pub_p2p")

    response = await ccn_api_client.post(
        "/api/v0/ipfs/pubsub/pub",
        json={
            "topic": mock_config.aleph.queue_topic.value,
            "data": json.dumps(MESSAGE),
        },
    )

    assert response.status == 403, await response.text()
    pub_p2p.assert_not_called()
