# settings.py
import pathlib
import yaml

def get_defaults():
    return {
        'nulsexplorer': {
            'host': '127.0.0.1',
            'port': 8080
        },
        'nuls': {
          'chain_id': 8964
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
