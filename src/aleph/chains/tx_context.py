from aleph.schemas.message_confirmation import MessageConfirmation


# At the moment, confirmation = chain transaction. This might change, but in the meantime
# having TxContext inherit MessageConfirmation avoids code duplication.
class TxContext(MessageConfirmation):
    pass
