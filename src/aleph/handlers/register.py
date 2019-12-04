# We will register here processors for the message types by name
VERIFIER_REGISTER = dict()
INCOMING_HANDLER = dict()
GARBAGE_COLLECTORS = dict()


def register_verifier(message_type, handler):
    """ Verifies a message is valid before forwarding it,
    handling it (should it be different?).
    """
    VERIFIER_REGISTER[message_type] = handler

def register_incoming_handler(message_type, handler):
    """ Registers a function as an incoming message handler for a specific type.
    """
    INCOMING_HANDLER[message_type] = handler

def register_garbage_collector(message_type, handler):
    """ Registers a function as a periodical garbage collector
    for a specific type.
    """
    GARBAGE_COLLECTORS[message_type] = handler
    
async def handle_incoming_message(message, content):
    if message['type'] in INCOMING_HANDLER:
        return await INCOMING_HANDLER[message['type']](message, content)
    else:
        return True