import json

from pkg_resources import resource_string

from aleph.vms.register import register_vm_engine
from .base import DockerizedBaseVM


class DockerizedPythonVM(DockerizedBaseVM):
    
    __version__ = 0.1
    
    LIMITS = {'cputime': 2, 'memory': 128}
    FILES = [{
        'name': 'executor.py',
        'content': resource_string('aleph.vms.dockerized', 'tools/executor.py')
    }]
        
        
    @classmethod
    def create(cls, code, message):
        """ Instanciate the VM. Returns a state.
        """
        payload = {
            'code': code,
            'action': 'create',
            'message': message,
            'args': message['content'].get('args', []),
            'kwargs': message['content'].get('kwargs', {})
        }
        
        output = cls._run('python', 'python3 executor.py',
                          stdin=json.dumps(payload).encode('utf-8'))
        
        if output['exit_code'] != 0:
            return {'result': None, 'error': output['stderr'].decode('utf-8')}
        
        try:
            out_payload = json.loads(output['stdout'].decode('utf-8'))
            return out_payload
        except Exception as e:
            return {'result': None, 'error': repr(e)}
        
        
    @classmethod
    def call(cls, code, state, message):
        """ Call a fonction on the VM.
        """
        payload = {
            'code': code,
            'action': 'call',
            'function': message['content']['function'],
            'message': message,
            'state': state,        
            'args': message['content'].get('args', []),
            'kwargs': message['content'].get('kwargs', {})
        }
        
        output = cls._run('python', 'python3 executor.py',
                          stdin=json.dumps(payload).encode('utf-8'))
        
        if output['exit_code'] != 0:
            return {'result': None, 'error': output['stderr'].decode('utf-8')}
        
        try:
            out_payload = json.loads(output['stdout'].decode('utf-8'))
            return out_payload
        except Exception as e:
            return {'result': None, 'error': repr(e)}

register_vm_engine('python_container', DockerizedPythonVM)