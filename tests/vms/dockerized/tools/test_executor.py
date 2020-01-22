

from subprocess import Popen, PIPE
from pkg_resources import resource_filename

def test_executor_1():
    process = Popen(['python',
                     resource_filename('aleph.vms.dockerized', 'tools/executor.py')],
                    stdout=PIPE, stderr=PIPE, stdin=PIPE)
    stdout, stderr = process.communicate(input=b"blahblah"*10)
    print(stdout)
    raise