import pytest
from async_exit_stack import AsyncExitStack
from p2pclient.daemon import make_p2pd_pair_ip4


@pytest.fixture
async def p2p_clients(request):
    nb_p2p_daemons = request.param
    assert isinstance(nb_p2p_daemons, int)

    async with AsyncExitStack() as stack:
        p2pd_tuples = [
            await stack.enter_async_context(
                make_p2pd_pair_ip4(
                    daemon_executable="jsp2pd",
                    enable_control=True,
                    enable_connmgr=False,
                    enable_dht=False,
                    enable_pubsub=True,
                )
            )
            for _ in range(nb_p2p_daemons)
        ]
        yield tuple(p2pd_tuple.client for p2pd_tuple in p2pd_tuples)
