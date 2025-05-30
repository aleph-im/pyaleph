import datetime as dt

import pytest
from configmanager import Config

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.schemas.pending_messages import parse_message
from aleph.storage import StorageService
from aleph.types.db_session import AsyncDbSessionFactory
from aleph.types.message_status import InvalidMessageException


@pytest.mark.skip("TODO: NULS signature verification does not work with the fixture.")
@pytest.mark.asyncio
async def test_valid_message(mocker):
    sample_message_dict = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "item_type": "ipfs",
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "2103041b0b357446927d2c8c62fdddd27910d82f665f16a4907a2be927b5901f5e6c004730450221009a54ecaff6869664e94ad68554520c79c21d4f63822864bd910f9916c32c1b5602201576053180d225ec173fb0b6e4af5efb2dc474ce6aa77a3bdd67fd14e1d806b4",
    }

    signature_verifier = SignatureVerifier()
    sample_message = parse_message(sample_message_dict)
    await signature_verifier.verify_signature(sample_message)


@pytest.mark.asyncio
async def test_invalid_chain_message():
    sample_message_dict = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "item_type": "ipfs",
        "chain": "BAR",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "2103041b0b357446927d2c8c62fdddd27910d82f665f16a4907a2be927b5901f5e6c004730450221009a54ecaff6869664e94ad68554520c79c21d4f63822864bd910f9916c32c1b5602201576053180d225ec173fb0b6e4af5efb2dc474ce6aa77a3bdd67fd14e1d806b4",
    }

    with pytest.raises(InvalidMessageException):
        _ = parse_message(sample_message_dict)


@pytest.mark.asyncio
async def test_invalid_signature_message(mocker):
    sample_message_dict = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "item_type": "ipfs",
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "BAR",
    }

    signature_verifier = SignatureVerifier()

    sample_message = parse_message(sample_message_dict)
    with pytest.raises(InvalidMessageException):
        _ = await signature_verifier.verify_signature(sample_message)


@pytest.mark.asyncio
async def test_invalid_signature_message_2(mocker):
    sample_message_dict = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "item_type": "ipfs",
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "2153041b0b357446927d2c8c62fdddd27910d82f665f16a4907a2be927b5901f5e6c004730450221009a54ecaff6869664e94ad68554525c79c21d4f63822864bd910f9916c32c1b5602201576053180d225ec173fb0b6e4af5efb2dc474ce6aa77a3bdd67fd14e1d806b4",
    }

    signature_verifier = SignatureVerifier()

    sample_message = parse_message(sample_message_dict)
    with pytest.raises(InvalidMessageException):
        _ = await signature_verifier.verify_signature(sample_message)


@pytest.mark.asyncio
async def test_incoming_inline_content(
    mock_config: Config,
    session_factory: AsyncDbSessionFactory,
    test_storage_service: StorageService,
):
    message_dict = {
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTapAav8g3fFjxQQCjwPd4ERPnai9oya",
        "type": "AGGREGATE",
        "time": 1564581054.0532622,
        "item_type": "inline",
        "item_content": '{"key":"metrics","address":"TTapAav8g3fFjxQQCjwPd4ERPnai9oya","content":{"memory":{"total":12578275328,"available":5726081024,"percent":54.5,"used":6503415808,"free":238661632,"active":8694841344,"inactive":2322239488,"buffers":846553088,"cached":4989644800,"shared":172527616,"slab":948609024},"swap":{"total":7787769856,"free":7787495424,"used":274432,"percent":0.0,"swapped_in":0,"swapped_out":16384},"cpu":{"user":9.0,"nice":0.0,"system":3.1,"idle":85.4,"iowait":0.0,"irq":0.0,"softirq":2.5,"steal":0.0,"guest":0.0,"guest_nice":0.0},"cpu_cores":[{"user":8.9,"nice":0.0,"system":2.4,"idle":82.2,"iowait":0.0,"irq":0.0,"softirq":6.4,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":9.6,"nice":0.0,"system":2.9,"idle":84.6,"iowait":0.0,"irq":0.0,"softirq":2.9,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":7.2,"nice":0.0,"system":3.0,"idle":86.8,"iowait":0.0,"irq":0.0,"softirq":3.0,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":11.4,"nice":0.0,"system":3.0,"idle":84.8,"iowait":0.1,"irq":0.0,"softirq":0.7,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":9.3,"nice":0.0,"system":3.3,"idle":87.0,"iowait":0.1,"irq":0.0,"softirq":0.3,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":5.5,"nice":0.0,"system":4.4,"idle":89.9,"iowait":0.0,"irq":0.0,"softirq":0.1,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":8.7,"nice":0.0,"system":3.3,"idle":87.9,"iowait":0.0,"irq":0.0,"softirq":0.1,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":11.4,"nice":0.0,"system":2.3,"idle":80.3,"iowait":0.0,"irq":0.0,"softirq":6.1,"steal":0.0,"guest":0.0,"guest_nice":0.0}]},"time":1564581054.0358574}',
        "item_hash": "84afd8484912d3fa11a402e480d17e949fbf600fcdedd69674253be0320fa62c",
        "signature": "21027c108022f992f090bbe5c78ca8822f5b7adceb705ae2cd5318543d7bcdd2a74700473045022100b59f7df5333d57080a93be53b9af74e66a284170ec493455e675eb2539ac21db022077ffc66fe8dde7707038344496a85266bf42af1240017d4e1fa0d7068c588ca7",
    }

    signature_verifier = SignatureVerifier()
    message_handler = MessageHandler(
        signature_verifier=signature_verifier,
        storage_service=test_storage_service,
        config=mock_config,
    )

    # Signature validation fails for this fixture
    pending_message = PendingMessageDb.from_message_dict(
        message_dict,
        check_message=False,
        reception_time=dt.datetime(2022, 1, 1),
        fetched=True,
    )

    async with session_factory() as session:
        message = await message_handler.verify_and_fetch(
            session=session, pending_message=pending_message
        )
    assert message is not None
