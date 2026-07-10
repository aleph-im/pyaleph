import json

from aleph_message.models import MessageType, VerifiableProgramContent

from aleph.db.models.messages import CONTENT_TYPE_MAP, extract_tags
from aleph.schemas.api.messages import VProgramMessage, format_message_dict
from aleph.schemas.pending_messages import PendingVProgramMessage, parse_message

VPROGRAM_CONTENT = {
    "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
    "time": 1719502000.0,
    "allow_amend": False,
    "payment": {"type": "credit"},
    "environment": {"internet": True},
    "resources": {"vcpus": 2, "memory": 2048, "seconds": 30},
    "runtime": {
        "ref": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
        "comment": "compose-runner snp bundle",
    },
    "workload": {
        "ref": "beefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef",
        "hash_tree": "feedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed",
        "roothash": "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
    },
    "verification": {
        "backend": "sev_snp",
        "policy": 196608,
        "measurements": [
            {
                "platform": "sev_snp",
                "digest": "ab" * 48,
                "vcpu_type": "EPYC-v4",
            }
        ],
    },
    "volumes": [
        {
            "ref": "da" * 32,
            "hash_tree": "d5" * 32,
            "roothash": "ef" * 32,
            "comment": "model weights",
        }
    ],
}

VPROGRAM_ITEM_HASH = "4c319b6bdf98f1e90f2bf8c69da175679fa21ca27d4547bbfa32f77dd3b49fe6"

VPROGRAM_MESSAGE_DICT = {
    "chain": "ETH",
    "sender": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
    "type": "V-PROGRAM",
    "channel": "TEST",
    "signature": (
        "0x372da8230552b8c3e65c05b31a0ff3a24666d66c575f8e11019f62579bf48c2b"
        "7fe2f0bbe907a2a5bf8050989cdaf8a59ff8a1cbcafcdef0656c54279b4aa0c71b"
    ),
    "time": 1719502000.0,
    "item_type": "inline",
    "item_content": None,  # filled below from VPROGRAM_CONTENT
    "item_hash": VPROGRAM_ITEM_HASH,
    "content": VPROGRAM_CONTENT,
}


def _message_dict() -> dict:
    message = dict(VPROGRAM_MESSAGE_DICT)
    message["item_content"] = json.dumps(VPROGRAM_CONTENT, separators=(",", ":"))
    return message


def test_parse_vprogram_message():
    message = parse_message(_message_dict())
    assert isinstance(message, PendingVProgramMessage)
    assert message.type == MessageType.v_program
    assert isinstance(message.content, VerifiableProgramContent)
    assert message.content.verification.policy == 0x30000
    assert message.content.workload.roothash == "cd" * 32


def test_content_type_map_has_vprogram():
    assert CONTENT_TYPE_MAP[MessageType.v_program] is VerifiableProgramContent
    content = CONTENT_TYPE_MAP[MessageType.v_program].model_validate(VPROGRAM_CONTENT)
    assert isinstance(content, VerifiableProgramContent)


def test_extract_tags_vprogram():
    content = {**VPROGRAM_CONTENT, "metadata": {"tags": ["snp", "demo"]}}
    assert extract_tags(MessageType.v_program, content) == ["snp", "demo"]
    assert extract_tags(MessageType.v_program, VPROGRAM_CONTENT) is None


def test_format_message_dict_vprogram():
    message = _message_dict()
    message["confirmed"] = False
    message["confirmations"] = []
    formatted = format_message_dict(message)
    assert isinstance(formatted, VProgramMessage)
    assert formatted.type == MessageType.v_program
    assert formatted.content.runtime.ref == "cafe" * 16
