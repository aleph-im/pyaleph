class BaseVM:
    @classmethod
    def hash_state(state, algo='sha256'):
        """ Takes a state object and returns a verifiable hash as hex string.
        """
        raise NotImplementedError
        
    @classmethod
    def verify_state_hash(state, hexhash, algo='sha256'):
        """ Verifies that the hash is valid for the said state.
        """
        raise NotImplementedError