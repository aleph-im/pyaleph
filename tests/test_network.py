import pytest
import aleph.chains
from aleph.network import check_message

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"


@pytest.mark.asyncio
async def test_check_message_trusted():
    passed_msg = {'foo': 1, 'bar': 2}
    msg = await check_message(passed_msg, trusted=True)
    assert len(msg.keys()) == 2, "same key count as object should be untouched"
    assert msg is passed_msg, "same object should be returned"
