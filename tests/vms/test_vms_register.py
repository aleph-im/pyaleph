from aleph.vms import register

def test_registered_count():
    assert len(register.VM_ENGINE_REGISTER) == 1 # we only have 1 for now
    assert "python_container" in register.VM_ENGINE_REGISTER