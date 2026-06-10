from aleph.services.p2p.jobs import preferred_peers_from_aggregate


def test_extracts_peer_ids_and_multiaddrs():
    content = {
        "nodes": [
            {
                "multiaddress": "/ip4/51.1.2.3/tcp/4025/p2p/QmCcn1",
                "status": "active",
            },
            {
                "multiaddress": "/dns/node.example.org/tcp/4025/p2p/QmCcn2",
                "status": "active",
            },
        ]
    }
    assert preferred_peers_from_aggregate(content) == [
        ("QmCcn1", ["/ip4/51.1.2.3/tcp/4025/p2p/QmCcn1"]),
        ("QmCcn2", ["/dns/node.example.org/tcp/4025/p2p/QmCcn2"]),
    ]


def test_skips_nodes_without_usable_multiaddress():
    content = {
        "nodes": [
            {"multiaddress": "", "status": "active"},
            {"multiaddress": "garbage-without-p2p-part", "status": "active"},
            {"status": "active"},
            {"multiaddress": "/ip4/51.1.2.3/tcp/4025/p2p/QmGood", "status": "active"},
        ]
    }
    assert preferred_peers_from_aggregate(content) == [
        ("QmGood", ["/ip4/51.1.2.3/tcp/4025/p2p/QmGood"])
    ]


def test_deduplicates_peer_ids():
    content = {
        "nodes": [
            {"multiaddress": "/ip4/51.1.2.3/tcp/4025/p2p/QmDup"},
            {"multiaddress": "/ip4/51.1.2.4/tcp/4025/p2p/QmDup"},
        ]
    }
    result = preferred_peers_from_aggregate(content)
    assert len(result) == 1
    assert result[0][0] == "QmDup"
    assert result[0][1] == [
        "/ip4/51.1.2.3/tcp/4025/p2p/QmDup",
        "/ip4/51.1.2.4/tcp/4025/p2p/QmDup",
    ]


def test_empty_content():
    assert preferred_peers_from_aggregate({}) == []
    assert preferred_peers_from_aggregate({"nodes": []}) == []
