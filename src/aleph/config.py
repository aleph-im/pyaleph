import logging

from configmanager import Config


def get_defaults():
    return {
        "logging": {
            "level": logging.WARNING,
            "max_log_file_size": 1_000_000_000,  # 1GB,
        },
        "aleph": {
            "queue_topic": "ALEPH-TEST",
            "host": "0.0.0.0",
            "port": 8000,
            "reference_node_url": None,
            "indexer_url": "https://multichain.api.aleph.cloud",
            "balances": {
                "address": "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
                "post_type": "balances-update",
            },
            "jobs": {
                "pending_messages": {
                    "max_retries": 1000,
                    "max_concurrency": 10,
                    "store": 30,
                },
                "pending_txs": {
                    "max_concurrency": 20,
                },
            },
        },
        "p2p": {
            "http_port": 4024,
            "port": 4025,
            "control_port": 4030,
            "daemon_host": "p2p-service",
            "mq_host": "rabbitmq",
            "reconnect_delay": 60,
            "alive_topic": "ALIVE",
            "clients": ["http"],
            "peers": [
                "/dns/api1.aleph.im/tcp/4025/p2p/Qmaxufiqdyt5uVWcy1Xh2nh3Rs3382ArnSP2umjCiNG2Vs",
                "/dns/api2.aleph.im/tcp/4025/p2p/QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV",
            ],
            "topics": ["ALIVE", "ALEPH-TEST"],
        },
        "tezos": {
            "enabled": True,
            "indexer_url": "https://tezos-mainnet.api.aleph.cloud",
            "chain_id": "main",
            "sync_contract": "KT1FfEoaNvooDfYrP61Ykct6L8z7w7e2pgnT",
        },
        "storage": {"folder": "./data/", "store_files": True, "engine": "filesystem"},
        "nuls": {
            "chain_id": 8964,
            "enabled": False,
            "packing_node": False,
            "private_key": None,
            "commit_delay": 14,
        },
        "nuls2": {
            "chain_id": 1,
            "enabled": False,
            "packing_node": False,
            "api_url": "https://apiserver.nuls.io/",
            "explorer_url": "https://nuls.world",
            "private_key": None,
            "sync_address": None,
            "commit_delay": 14,
            "remark": "ALEPH-SYNC",
            "token_contract": None,
        },
        "bsc": {
            "enabled": True,
            "sync_contract": "0xdF270752C8C71D08acbae4372687DA65AECe2D5D",
        },
        "ethereum": {
            "enabled": False,
            "api_url": "http://127.0.0.1:8545",
            "packing_node": False,
            "chain_id": 1,
            "private_key": None,
            "sync_contract": None,
            "start_height": 11400000,
            "commit_delay": 35,
            "token_contract": None,
            "token_start_height": 10900000,
            "max_gas_price": 150000000000,
            "authorized_emitters": ["0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"],
        },
        "postgres": {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "aleph",
            "user": "aleph",
            "password": "decentralize-everything",
            "pool_size": 50,
        },
        "mail": {
            "email_sender": "aleph@localhost.localdomain",
            "smtp_url": "smtp://localhost",
        },
        "ipfs": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 5001,
            "gateway_port": 8080,
            "id": None,
            "alive_topic": "ALEPH_ALIVE",
            "reconnect_delay": 60,
            "peers": [
                "/dnsaddr/api1.aleph.im/ipfs/12D3KooWNgogVS6o8fVsPdzh2FJpCdJJLVSgJT38XGE1BJoCerHx",
                "/ip4/51.159.57.71/tcp/4001/p2p/12D3KooWBH3JVSBwHLNzxv7EzniBP3tDmjJaoa3EJBF9wyhZtHt2",
                "/ip4/62.210.93.220/tcp/4001/p2p/12D3KooWLcmvqojHzUnR7rr8YhFKGDD8z7fmsPyBfAm2rT3sFGAF",
            ],
        },
        "rabbitmq": {
            "host": "127.0.0.1",
            "port": 5672,
            "username": "aleph-p2p",
            "password": "change-me!",
            "pub_exchange": "p2p-publish",
            "sub_exchange": "p2p-subscribe",
            "message_exchange": "aleph-messages",
        },
        "sentry": {
            "dsn": None,
            "traces_sample_rate": None,
        },
    }


app_config = Config(schema=get_defaults())


def get_config() -> Config:
    return app_config
