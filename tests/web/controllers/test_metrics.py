from dataclasses import dataclass

from aleph.web.controllers.metrics import format_dict_for_prometheus, \
    format_dataclass_for_prometheus, Metrics, BuildInfo


def test_format_dict_for_prometheus():
    assert format_dict_for_prometheus(
        {
            'a': 1,
            'b': 2.2,
            'c': "3",
        }
    ) == '{a=1,b=2.2,c="3"}'


def test_format_dataclass_for_prometheus():

    @dataclass
    class Simple:
        a: int
        b: float
        c: str

    assert format_dataclass_for_prometheus(
        Simple(1, 2.2, "3")
    ) == 'a 1\nb 2.2\nc "3"'

    @dataclass
    class Tagged:
        d: Simple
        e: str


    assert format_dataclass_for_prometheus(
        Tagged(Simple(1, 2.2, "3"), "e")
    ) == 'd{a=1,b=2.2,c="3"} 1\ne "e"'


def test_metrics():
    metrics = Metrics(
        pyaleph_build_info=BuildInfo(
            python_version='3.8.0',
            version='v999',
        ),
        pyaleph_status_peers_total=0,
        pyaleph_status_sync_messages_total=123,
        pyaleph_status_sync_pending_messages_total=456,
        pyaleph_status_sync_pending_txs_total=0,
        pyaleph_status_chain_eth_last_committed_height=0,
    )

    assert format_dataclass_for_prometheus(
        metrics
    ) == ('pyaleph_build_info{python_version="3.8.0",version="v999"} 1\n'
          'pyaleph_status_peers_total 0\n'
          'pyaleph_status_sync_messages_total 123\n'
          'pyaleph_status_sync_pending_messages_total 456\n'
          'pyaleph_status_sync_pending_txs_total 0\n'
          'pyaleph_status_chain_eth_last_committed_height 0'
    )
