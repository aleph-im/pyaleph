

async def check_sender_authorization(message, content):
    """ Checks a content against a message to verify if sender is authorized.

    TODO: implement "security" aggregate key check.
    """

    # for now, only support direct signature
    # (no 3rd party or multiple address signing)
    if message['sender'] == content['address']:
        return True

    return False
