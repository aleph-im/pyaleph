
import builtins
import json
import select
import sys
import traceback


def execute(msg):
    locs = {}
    globs = {'__builtins__': builtins}
    byte_code = compile(msg['code'], '<smart-contract>', 'exec')
    exec(byte_code, globals(), locs)
    return locs

def do_create(payload):
    try:
        locs = execute(payload)
    except Exception as e:
        return {'error': repr(e), 'error_step': 'prepare',
                'traceback': traceback.format_exc(), 'result': None}

    try:
        item_class = locs['SmartContract']
        obj = item_class(
            payload['message'], *payload.get('args', []),
            **payload.get('kwargs', {}))
        return {'result': True, 'state': obj.__dict__}
    except Exception as e:
        return {'error': repr(e), 'error_step': 'call',
                'traceback': traceback.format_exc(), 'result': None}
    
def do_call(payload):
    try:
        locs = execute(payload)
    except Exception as e:
        return {'error': repr(e), 'error_step': 'prepare',
                'traceback': traceback.format_exc(), 'result': None}

    try:
        item_class = locs['SmartContract']
        instance = item_class.__new__(item_class)
        instance.__dict__ = payload['state']
        result = getattr(instance, payload['function'])(
            payload['message'],
            *payload.get('args', []), **payload.get('kwargs', {}))
        return {'result': result or True, 'state': instance.__dict__}
    except Exception as e:
        return {'error': repr(e), 'error_step': 'call',
                'traceback': traceback.format_exc(), 'result': None}

def handle(line):
    try:
        payload = json.loads(line)
        if payload['action'] == 'create':
            resp = do_create(payload)
        elif payload['action'] == 'call':
            resp = do_call(payload)
        elif payload['action'] == 'exit':
            exit(0)
    except Exception as e:
        resp = {'error': repr(e), 'error_step': 'unhandled',
                'traceback': traceback.format_exc(),
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
            resp = handle(line)
            print(json.dumps(resp))
        else: # an empty line means stdin has been closed
            exit(0)