import json
import epicbox
import hashlib
from aleph.vms.base import BaseVM

class DockerizedBaseVM(BaseVM):
    
    __version__ = 0.1
    
    PROFILES = {
        'python': {
            'docker_image': 'python:3.8-alpine',
            'network_disabled': True,
            'user': '10000'
        }
    }
    
    LIMITS = {'cputime': 1, 'memory': 64}
    
    @classmethod
    def initialize(cls):
        epicbox.configure(
            profiles=cls.PROFILES
        )
    
    @classmethod
    def hash_state(cls, state, algo='sha256'):
        """ Takes a state object and returns a verifiable hash as hex string.
        """
        to_hash = json.dumps(state, indent=None,
                            separators=(',', ':'), sort_keys=True)
        if algo == 'sha256':
            return hashlib.sha256(to_hash.encode('utf-8')).hexdigest()
        elif algo == 'sha512':
            return hashlib.sha512(to_hash.encode('utf-8')).hexdigest()
        elif algo == 'blake2b':
            return hashlib.blake2b(to_hash.encode('utf-8')).hexdigest()
        else:
            raise ValueError('Algo %r not supported' % algo)
            

    @classmethod
    def verify_state_hash(cls, state, hexhash, algo='sha256'):
        """ Verifies that the hash is valid for the said state.
        """
        return cls.hash_state(state, algo=algo) == hexhash