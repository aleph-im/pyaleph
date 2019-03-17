# We will register here processors for the chains by name
VERIFIER_REGISTER = dict()
INCOMING_WORKERS = dict()
OUTGOING_WORKERS = dict()


def register_verifier(chain_name, handler):
    VERIFIER_REGISTER[chain_name] = handler


def register_incoming_worker(chain_name, handler):
    INCOMING_WORKERS[chain_name] = handler


def register_outgoing_worker(chain_name, handler):
    OUTGOING_WORKERS[chain_name] = handler
