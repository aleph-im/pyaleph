# settings.py
def get_defaults():
    return {
        'aleph': {
            'queue_topic': 'ALEPH-QUEUE',
            'host': '0.0.0.0',
            'port': 8000,
            'reference_node_url': None,
        },
        'p2p': {
            'port': 4025,
            'http_port': 4024,
            'host': '0.0.0.0',
            'key': None,
            'reconnect_delay': 60,
            'clients': ['http'],
            'peers': [
                '/ip4/51.159.57.71/tcp/4025/p2p/QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV',
                '/ip4/51.158.116.142/tcp/4025/p2p/Qmaxufiqdyt5uVWcy1Xh2nh3Rs3382ArnSP2umjCiNG2Vs',
                '/ip4/62.210.93.220/tcp/4025/p2p/QmXdci5feFmA2pxTg8p3FCyWmSKnWYAAmr7Uys1YCTFD8U'
            ]
        },
        'storage': {
            'folder': './data/',
            'store_files': False,
            'engine': 'rocksdb'
        },
        'nuls': {
            'chain_id': 8964,
            'enabled': False,
            'packing_node': False,
            'private_key': None,
            'commit_delay': 14
        },
        'nuls2': {
            'chain_id': 1,
            'enabled': False,
            'packing_node': False,
            'api_url': 'https://apiserver.nuls.io/',
            'explorer_url': 'https://nuls.world',
            'private_key': None,
            'sync_address': None,
            'commit_delay': 14,
            'remark': 'ALEPH-SYNC',
            'token_contract': None
        },
        'ethereum': {
            'enabled': False,
            'api_url': 'http://127.0.0.1:8545',
            'packing_node': False,
            'chain_id': 1,
            'private_key': None,
            'sync_contract': None,
            'start_height': 11400000,
            'commit_delay': 35,
            'token_contract': None,
            'token_start_height': 10900000,
            'authorized_emitters': [
                '0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC'
            ]
        },
        'binancechain': {
            'enabled': False,
            'packing_node': False,
            'private_key': None,
            'sync_address': None,
            'start_time': None,
            'commit_delay': 35
        },
        'mongodb': {
            'uri': 'mongodb://127.0.0.1:27017',
            'database': 'aleph'
        },
        'mail': {
            'email_sender': 'aleph@localhost.localdomain',
            'smtp_url': 'smtp://localhost'
        },
        'ipfs': {
            'enabled': True,        
            'host': '127.0.0.1',
            'port': 5001,
            'gateway_port': 8080,
            'id': None,
            'reconnect_delay': 60,
            'peers': [
              '/dnsaddr/api1.aleph.im/ipfs/QmVrrTCdRhjEDE8gftXNw2TihcAUy6d2TXcuNUeivBFpcA',
              '/ip4/51.159.57.71/tcp/4001/p2p/QmeqShhZnPZgNSAwPy3iKJcHVLSc4hBJfPv5vTNi784R75'
            ]
        },
        'sentry': {
            'dsn': None,
            'traces_sample_rate': None,
        }
    }
