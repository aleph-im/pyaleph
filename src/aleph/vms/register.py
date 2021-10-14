# We will register here processors for the message types by name
VM_ENGINE_REGISTER = dict()


def register_vm_engine(engine_name, engine_class):
    """Verifies a message is valid before forwarding it,
    handling it (should it be different?).
    """
    VM_ENGINE_REGISTER[engine_name] = engine_class
