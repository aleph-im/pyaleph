from pkg_resources import resource_string
from aleph.vms.dockerized.python import DockerizedPythonVM

def test_create():
    DockerizedPythonVM.initialize()
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    result = DockerizedPythonVM.create(
        example_code.decode('utf-8'),
        {
            'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W',
            'content': {
                'args': ['Test', 'TST', 24000000]
            }
        }
    )
    
    assert result['result'] is True
    assert result['state']['owner'] == 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
    assert result['state']['total_supply'] == 24000000 * (10**18)
    assert result['state']['balances']['NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'] == 24000000 * (10**18)
    assert len(result['state']['balances']) == 1
    

def test_call():
    DockerizedPythonVM.initialize()
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    result = DockerizedPythonVM.call(
        example_code.decode('utf-8'),
        {
            "owner": "NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W",
            "name": "Test",
            "symbol": "TST",
            "decimals": 18,
            "total_supply": 24000000000000000000000000,
            "balances": {"NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W": 24000000000000000000000000},
            "allowed": {}
        },
        {
            'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W',
            'content': {
                'function': 'transfer',
                'args': ['blah', 1000*(10**18)]
            }
        }
    )
    
    assert result['result'] is True
    assert result['state']['owner'] == 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
    assert result['state']['total_supply'] == 24000000 * (10**18)
    assert result['state']['balances']['NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'] == (24000000 - 1000) * (10**18)
    assert result['state']['balances']['blah'] == 1000 * (10**18)
    assert len(result['state']['balances']) == 2