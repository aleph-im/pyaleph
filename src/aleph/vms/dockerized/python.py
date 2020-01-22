import hashlib
import json

from aleph.vms.register import register_vm_engine
from .base import DockerizedBaseVM

class RestrictedPythonVM(DockerizedBaseVM):
    
    __version__ = 0.1
    
    LIMITS = {'cputime': 2, 'memory': 128}

register_vm_engine('python_container', RestrictedPythonVM)