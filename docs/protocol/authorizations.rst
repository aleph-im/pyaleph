**************
Authorizations
**************

Inside most message types there is an "address" field.
This is the address for which the message applies (for which address apply this
aggregate, who posted that item...).

The client validates that the message sender (the one signing the message) has
the right to publish on this address behalf.

1. obvious case: if the sender == the content address, it is authorized.
2. the "security" key in the address aggregate has an entry for this address

Aggregate "security" key
========================

This key is a special case in the Aggregate system. It can only be changed
by sending an AGGREGATE message on the "security" channel.

For now, only the address itself (sender == content.address) has the right
to send an AGGREGATE message on this channel ("security") with this key ("security").
This behaviour might change in the future.

"authorizations" subkey
-----------------------

It's an array of objects being built like this:

=============== =======================================================
address         the address to authorize
chain           optional. only accept this address on a specific chain
channels        optional. authorized channel list
types           optional. the authorized message types
post_types      optional. specific post types authorized
aggregate_keys  optional. specific aggregate keys authorized
=============== =======================================================

.. note::

   If some filter is set, it is exclusive, only those will be accepted.
   All filters specified must pass with one exclusion: type specific filters only apply
   to this type (post_types only apply to POST, aggregate_keys only apply to AGGREGATE type)

   Example::

     channels: ['blog'] => only post in this channel will be accepted
     types: ['POST'] => only POST will be accepted from this sender
     aggregate_keys: ['profile', 'preferences'] => only those keys will be writeable.
