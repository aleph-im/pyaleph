*******
Payment
*******

.. warning::

  Currently being implemented.


Storage and message processing payment providers can be installed as modules.

There will be a "recurring" one added as well in the future. Details need to be decided.

- `INCOMING_REGISTER` holds the incoming message providers
  They are processed after checking message is valid but before signature verification
  Any provider returning true and the message will be processed further
- `PROCESSED_REGISTER` holds the processed message providers (actually charge the amounts)

The MVP network (current implementation of Aleph.im network code) has three providers:

- **ChannelEndorsement**: an address holding Aleph tokens in one of the underlying chains
  endorses this channel and incentivizes (X being the Aleph token count):
  - up to X post/aggregates messages (of up to 100kb each)
  - up to Xmb of files
- **PersonalStorage**: and address holding aleph tokens can post itself (or someone on it behalf),
  messages with (X being the Aleph token count):
  - up to X POST/AGGREGATES messages (of up to 100kb each)
  - up to Xmb of files in STORE messages
- **Core**: Core channels (dedicated to identity and security) have their messages free
  if they belong to correct types and pass "anti-spam" checks.

Ideally (not done yet), a garbage collecting process will come and clean the data if the checks
don't pass anymore.

Those amounts while high are here because the MVP network has a full replication and doesn't actually "spend" those tokens.
Once the Aleph token is held on the network, and nodes can actually make the addresses pay
for storage, much smaller amounts will be requested in an "open market" fashion.
