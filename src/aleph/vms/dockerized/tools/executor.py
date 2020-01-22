
import sys
import select
import json

from subprocess import Popen, PIPE

def execute(self, msg):
    locs = {}
    globs = {'__builtins__': builtins}
    byte_code = compile(msg['body'], '<restricted-python>', 'exec')
    exec(byte_code, globs, locs)
    return locs

def do_create(payload):
    try:
        locs = execute(payload)
    except Exception as e:
        return {'error': repr(e), 'error_step': 'prepare', 'result': None}

    try:
        item_class = locs['SmartContract']
        obj = item_class(*msg['args'], **msg['kwargs'])
        return {'result': True, 'state': obj.__dict__}
    except Exception as e:
        return {'error': repr(e), 'error_step': 'call', 'result': None}

def handle(line):
    payload = json.loads(line)
    print(payload)
    
    try:
        if payload['action'] == 'create':
            resp = do_create(payload)
        elif payload['action'] == 'call':
            resp = do_call(payload)
        elif payload['action'] == 'exit':
            exit(0)
    except Exception as e:
        resp = {'error': repr(e), 'error_step': 'unhandled',
                'result': None}
    return resp

if __name__ == "__main__":
    # value = sys.stdin.readline()
    # sys.stdout.write(value)

    # If there's input ready, do something, else do something
    # else. Note timeout is zero so select won't block at all.
    while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
        line = sys.stdin.readline()
        if line:
            handle(line)
        else: # an empty line means stdin has been closed
            exit(0)