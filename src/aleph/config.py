# settings.py
def get_defaults():
    return {
        'aleph': {
            'queue_topic': 'ALEPH-QUEUE',
            'host': '127.0.0.1',
            'port': 8080
        },
        'nulsexplorer': {
            'url': 'http://127.0.0.1:8080'
        },
        'nuls': {
          'chain_id': 8964,
          'packing_node': False,
          'private_key': None
        },
        'ethereum': {
          'enabled': False,
          'api_url': 'http://127.0.0.1:8545',
          'packing_node': False,
          'chain_id': 1,
          'private_key': None,
          'sync_contract': None
        },
        'mongodb': {
          'uri': 'mongodb://127.0.0.1:27006',
          'database': 'aleph'
        },
        'mail': {
            'email_sender': 'aleph@localhost.localdomain',
            'smtp_url': 'smtp://localhost'
        },
        'ipfs': {
            'host': '127.0.0.1',
            'port': 5001
        }
    }
