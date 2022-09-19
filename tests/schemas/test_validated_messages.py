"""
Tests for the validated message schemas. These tests use the same fixtures
as the tests for the pending message schemas and check additional features.
"""

import json
from typing import Dict

from aleph_message.models import MessageConfirmation

from aleph.schemas.message_content import MessageContent, ContentSource
from aleph.schemas.pending_messages import (
    PendingAggregateMessage,
    PendingPostMessage,
    PendingStoreMessage,
    parse_message,
)
from aleph.schemas.validated_message import (
    validate_pending_message,
    BaseValidatedMessage,
    ValidatedAggregateMessage,
    ValidatedStoreMessage,
)


def check_basic_message_fields(message: BaseValidatedMessage, message_dict: Dict):
    assert message.chain == message_dict["chain"]
    assert message.item_hash == message_dict["item_hash"]
    assert message.sender == message_dict["sender"]
    assert message.type == message_dict["type"]
    assert message.channel == message_dict["channel"]
    assert message.signature == message_dict["signature"]
    assert message.channel == message_dict["channel"]


def test_parse_aggregate_inline_message():
    message_dict = {
        "chain": "ETH",
        "item_hash": "6127637a9415444e62843f62c81a9dda708363b3bb830a5b10fcc212cd586fa9",
        "sender": "0x51A58800b26AA1451aaA803d1746687cB88E0501",
        "type": "AGGREGATE",
        "channel": "UNSLASHED",
        "item_content": '{"address":"0x51A58800b26AA1451aaA803d1746687cB88E0501","key":"0x93463a4f3af42de28f6840e59de6111b4192cf8bbinance","content":{"1652716953956":{"version":"x25519-xsalsa20-poly1305","nonce":"eohzj0i+fiaduqOcRnKyHVoTN19Gdv1N","ephemPublicKey":"eAWtQ7A0qA1b/VpnuexR098LFzQhw4/wbneri+XuBgA=","ciphertext":"qsWudj1UrcC5qbdZgzks3h8OF+kOD/CpB5og7zZXNj3zDYoXA+0CLYErX8gsN7yuJQNd+MciEHoxfQZWKtulR9+RMxQrD7DnNUE4y0ick3aFKjXAcJLcbCKOXllo5p9hxq1o/VONJor4uiHc97UhRTK1RXQUNdKz+V+RknAPrlamDcv3LJopm9zdMoxw5hRYIpF3fKH5natLkvc7EzmeQ3Bvo3mUtcyHWZZqmWpmtjVNFf1I+OHgFz4SK4O8nLq2fDPW8EtJBwTTfKeUIhj9B34V4+gl4O+822e4Tnbi4sTFGREc2lSqwDME4u4qyYMaM7omFYcfVvLwBHtIOoSl21xtaPh7g5q8z9lqlWJQTE9ZE2z0kKUuDLPCcrLMm1ooc69pSKPn2W87ycAWyN1v88i5KIU8vIBraWlFY76ROlHScw1L05PKI+1S03demsgNaT+hNTxGwSBOuEq7P25aJjiL3eKXZZ+lZONtSbegxTfFBg==","sha256":"99b9660c915e6ba26fb52cc7ae2735e73b1aeebd167db762e6e4b3d6ec85235f"}},"time":1652716955.776}',
        "item_type": "inline",
        "signature": "0xb0b97e102f7f75091306ec3ac39639a9560783a1652ed9f1c79138cafc77b9a1519a5cccabf7eb64a8cf4de11394757d95f9d7218833f85010ee847b77716c201c",
        "time": 1652716955.776,
    }

    item_content = message_dict["item_content"]
    content = json.loads(item_content)

    pending_message = parse_message(message_dict)
    assert isinstance(pending_message, PendingAggregateMessage)

    message_content = MessageContent(
        pending_message.item_hash, ContentSource.INLINE, content, item_content
    )
    confirmations = []
    validated_message = validate_pending_message(
        pending_message=pending_message,
        content=message_content,
        confirmations=confirmations,
    )
    assert isinstance(validated_message, ValidatedAggregateMessage)

    check_basic_message_fields(validated_message, message_dict)

    assert validated_message.content.address == content["address"]
    assert validated_message.content.time == content["time"]
    assert validated_message.content.key == content["key"]
    assert validated_message.content.content == content["content"]

    assert validated_message.confirmations == confirmations
    assert not validated_message.confirmed


def test_parse_post_message_storage_content():
    message_dict = {
        "chain": "ETH",
        "item_hash": "QmWx3j1gSQUrBkYnA8wiuhmE5wGuVNKv5wW6L7RHgLi4H4",
        "sender": "0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8",
        "type": "POST",
        "channel": None,
        "item_content": None,
        "item_type": "ipfs",
        "signature": "0xa1d9fadcf5e6613f6929aa18720c216763a4c04d1462c6e10b81b37d8b2b7fd42618f7889fd2b29d4940d5cb68b6eb24243b51fa932dec6d96de9bbb7e64f91d1c",
        "time": 1608297192.085,
    }

    content = {
        "address": "0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8",
        "content": {"banner": None, "body": "d", "subtitle": "", "tags": []},
        "time": 1556200724.599,
        "type": "blog_pers",
    }

    pending_message = parse_message(message_dict)
    assert isinstance(pending_message, PendingPostMessage)

    message_content = MessageContent(
        pending_message.item_hash, ContentSource.INLINE, content, json.dumps(content)
    )

    confirmations = []
    validated_message = validate_pending_message(
        pending_message=pending_message,
        content=message_content,
        confirmations=confirmations,
    )

    check_basic_message_fields(validated_message, message_dict)
    assert not validated_message.confirmed
    assert validated_message.confirmations == []


def test_parse_store_message_inline_content():
    message_dict = {
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

    item_content = message_dict["item_content"]
    content = json.loads(item_content)

    pending_message = parse_message(message_dict)
    assert isinstance(pending_message, PendingStoreMessage)

    confirmations = [MessageConfirmation(chain="ETH", height=1234, hash="abcd")]
    message_content = MessageContent(
        pending_message.item_hash, ContentSource.INLINE, content, item_content
    )
    validated_message = validate_pending_message(
        pending_message=pending_message,
        content=message_content,
        confirmations=confirmations,
    )
    assert isinstance(validated_message, ValidatedStoreMessage)

    check_basic_message_fields(validated_message, message_dict)

    assert validated_message.content.address == content["address"]
    assert validated_message.content.time == content["time"]
    assert validated_message.content.item_hash == content["item_hash"]
    assert validated_message.content.item_type == content["item_type"]

    assert validated_message.confirmed
    assert validated_message.confirmations == confirmations
