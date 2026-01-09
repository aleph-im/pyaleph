from unittest.mock import MagicMock

import pytest
from aleph_message.models import ItemType, MessageType

from aleph.chains.common import get_verification_buffer
from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.schemas.pending_messages import BasePendingMessage, parse_message
from aleph.types.message_status import MessageProcessingStatus


@pytest.mark.asyncio
async def test_get_verification_buffer():
    item_hash = "7e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f"

    message = BasePendingMessage(
        chain="ETH",
        sender="SENDER",
        type=MessageType.store,
        item_hash=item_hash,
        signature="1234",
        item_content=None,
        item_type=ItemType.storage,
        time=1000000,
    )

    buffer = get_verification_buffer(message)
    expected_buffer = f"ETH\nSENDER\nSTORE\n{item_hash}".encode("utf-8")
    assert buffer == expected_buffer


@pytest.mark.skip("Signature verification of the fixture fails")
@pytest.mark.asyncio
async def test_incoming_inline(mocker):
    # from aleph import model
    # model.db = None
    # monkey patch MagicMock
    async def async_magic():
        pass

    MagicMock.__await__ = lambda x: async_magic().__await__()

    message_processor = MessageHandler(
        signature_verifier=MagicMock(), config=MagicMock()
    )

    mocker.patch("aleph.model.db")

    message_dict = {
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTapAav8g3fFjxQQCjwPd4ERPnai9oya",
        "type": "AGGREGATE",
        "time": 1564581054.0532622,
        "item_content": '{"key":"metrics","address":"TTapAav8g3fFjxQQCjwPd4ERPnai9oya","content":{"memory":{"total":12578275328,"available":5726081024,"percent":54.5,"used":6503415808,"free":238661632,"active":8694841344,"inactive":2322239488,"buffers":846553088,"cached":4989644800,"shared":172527616,"slab":948609024},"swap":{"total":7787769856,"free":7787495424,"used":274432,"percent":0.0,"swapped_in":0,"swapped_out":16384},"cpu":{"user":9.0,"nice":0.0,"system":3.1,"idle":85.4,"iowait":0.0,"irq":0.0,"softirq":2.5,"steal":0.0,"guest":0.0,"guest_nice":0.0},"cpu_cores":[{"user":8.9,"nice":0.0,"system":2.4,"idle":82.2,"iowait":0.0,"irq":0.0,"softirq":6.4,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":9.6,"nice":0.0,"system":2.9,"idle":84.6,"iowait":0.0,"irq":0.0,"softirq":2.9,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":7.2,"nice":0.0,"system":3.0,"idle":86.8,"iowait":0.0,"irq":0.0,"softirq":3.0,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":11.4,"nice":0.0,"system":3.0,"idle":84.8,"iowait":0.1,"irq":0.0,"softirq":0.7,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":9.3,"nice":0.0,"system":3.3,"idle":87.0,"iowait":0.1,"irq":0.0,"softirq":0.3,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":5.5,"nice":0.0,"system":4.4,"idle":89.9,"iowait":0.0,"irq":0.0,"softirq":0.1,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":8.7,"nice":0.0,"system":3.3,"idle":87.9,"iowait":0.0,"irq":0.0,"softirq":0.1,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":11.4,"nice":0.0,"system":2.3,"idle":80.3,"iowait":0.0,"irq":0.0,"softirq":6.1,"steal":0.0,"guest":0.0,"guest_nice":0.0}]},"time":1564581054.0358574}',
        "item_hash": "84afd8484912d3fa11a402e480d17e949fbf600fcdedd69674253be0320fa62c",
        "signature": "21027c108022f992f090bbe5c78ca8822f5b7adceb705ae2cd5318543d7bcdd2a74700473045022100b59f7df5333d57080a93be53b9af74e66a284170ec493455e675eb2539ac21db022077ffc66fe8dde7707038344496a85266bf42af1240017d4e1fa0d7068c588ca7",
    }
    message_dict["item_type"] = "inline"

    message = parse_message(message_dict)
    pending_message = PendingMessageDb.from_obj(message)
    status, ops = await message_processor.verify_and_fetch_pending_message(
        pending_message
    )
    assert status == MessageProcessingStatus.MESSAGE_HANDLED
