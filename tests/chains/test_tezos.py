import pytest

from aleph.network import check_message


@pytest.mark.asyncio
async def test_tezos_verify_signature():
    message_dict = {
        "chain": "TEZOS",
        "channel": "TEST",
        "sender": "tz2XYLFJ34YhoKkuAqRPU7z5Nh9tkwGJonRn",
        "type": "POST",
        "time": 1657271407.929373,
        "item_content": '{"address":"tz2XYLFJ34YhoKkuAqRPU7z5Nh9tkwGJonRn","time":1657271407.9225595,"content":{"status":"testing"},"type":"test"}',
        "item_hash": "f241d02e4b3476bc826aec7985cf4260f2a097582f83d34d80c08e2851865353",
        "signature": "spsig17yxaXxJZTANXFQATUfcg6PhaWzvJpbhV5U1zVAB3pP2K6mquuW4rqCguQXV3zSqmKQRbj8yYXvjwuh5CvpbVddyC6sfzc",
        "content": {
            "address": "tz2XYLFJ34YhoKkuAqRPU7z5Nh9tkwGJonRn",
            "time": 1657271407.9225595,
            "content": {"status": "testing"},
            "type": "test",
        },
    }

    _ = await check_message(message_dict)
