import hashlib
import json
from pkg_resources import resource_filename, resource_string
import epicbox

from aleph.vms.register import register_vm_engine
from .base import DockerizedBaseVM

class RestrictedPythonVM(DockerizedBaseVM):
    
    __version__ = 0.1
    
    LIMITS = {'cputime': 2, 'memory': 128}
    FILES = [{
        'name': 'executor.py',
        'content': resource_string('aleph.vms.dockerized', 'tools/executor.py')
    }]
        
        
    @classmethod
    def create(cls, code, message, *args, **kwargs):
        """ Instanciate the VM. Returns a state.
        """
        payload = {
            'code': code,
            'action': 'create',
            'message': message,
            'args': args,
            'kwargs': kwargs
        }
    
    
        raise NotImplementedError      
        
        
    @classmethod
    def call(cls, code, message, *args, **kwargs):
        """ Call a fonction on the VM.
        """
        payload = {
            'code': code,
            'action': 'call',
            'function': 'transfer',
            'message': {
                'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
            },
            'state': {
                "owner": "NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W",
                "name": "Test",
                "symbol": "TST",
                "decimals": 18,
                "total_supply": 24000000000000000000000000,
                "balances": {"NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W": 24000000000000000000000000},
                "allowed": {}
            },        
            'args': ['blah', 1000*(10**18)]
        }
        
        output = cls._run('python', 'python3 executor.py',
                          stdin=json.dumps(payload).encode('utf-8'))
        
        if output['status'] != 0:
            return {'result': None, 'error': output['stderr']}
        
        try:
            out_payload = json.loads(output['stdout'].decode('utf-8'))
        assert out_payload['result'] is True
        assert out_payload['state']['owner'] == 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
        assert out_payload['state']['total_supply'] == 24000000 * (10**18)
        assert out_payload['state']['balances']['NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'] == (24000000 - 1000) * (10**18)
        assert out_payload['state']['balances']['blah'] == 1000 * (10**18)
        assert len(out_payload['state']['balances']) == 2
        assert not output.get('stderr')    

register_vm_engine('python_container', RestrictedPythonVM)