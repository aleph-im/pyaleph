import hashlib
import json

import epicbox

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
    ENTRYPOINT = None
    FILES = []
    
    @classmethod
    def initialize(cls):
        epicbox.configure(
            profiles=cls.PROFILES
        )
    
    
    @classmethod
    def _run(cls, profile, command, stdin):
        return epicbox.run(
            profile, command, files=cls.FILES,
            limits=cls.LIMITS, stdin=stdin)
        
        
    @classmethod
    def create(cls, code, message, *args, **kwargs):
        """ Instanciate the VM. Returns a state.
        """
        raise NotImplementedError
        
        
    @classmethod
    def call(cls, code, message, *args, **kwargs):
        """ Call a fonction on the VM.
        """
        raise NotImplementedError      
        
    
    @classmethod
    def hash_state(cls, state, algo='sha256', previous_hash=None):
        """ Takes a state object and returns a verifiable hash as hex string.
        """
        if previous_hash is not None:
            state['previous_hash'] = previous_hash
            
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