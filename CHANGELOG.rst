=========
Changelog
=========

Version 0.5.0
=============

* Feature: Migrated from Pydantic v1 to Pydantic v2
* Removed dependency on dataclasses-json in favor of Pydantic models
* Updated the JSON serialization to work with Pydantic v2
* Updated schema validation to use the new Pydantic v2 API

Version 0.4.7
=============

* Fix: convert indexer timestamps to seconds.

Version 0.4.6
=============

* Fix: use correct chain name in indexer messages.

Version 0.4.5
=============

* Feature: fetch messages from the Aleph smart contract on Ethereum and BSC.

Version 0.4.4
=============

* Fix: Messages sent to the `pubsub` endpoint were only forwarded to other nodes.
* Fix: The ref field on STORE messages was not stored
* Upgrade dependencies p2p-service and aleph-client
* Support disabling IPFS in configuration

Version 0.4.3
=============

* Fix: check all messages by default.

Version 0.4.2
=============

* Updated default configuration variables to simplify the upgrade to 0.4.x.

Version 0.4.1
=============

* Fixed a message processing bug where some messages could be retried indefinitely.

Version 0.4.0
=============

* CCNs now use the Aleph.im P2P service for P2P communication instead of libp2p's P2P daemon.
* Fixed a general 500 error on the posts GET endpoint.
* Syntactically invalid pending messages are now dropped and not retried.
* CCNs now support PROGRAM messages creating persistent VMs.
* CCNs now use Pydantic models internally to validate and process messages.

Version 0.3.3
=============

* Users can now sign Aleph messages with a Tezos private key.
* The API now checks the validity of Aleph messages as soon as they are posted and
  returns a 422 error code if the message is incorrect. Note that the signature is not
  checked.

Version 0.3.2
=============

This release fixes a synchronisation delay issue. The pending message job blocked
while waiting for the last pending messages in the queue to be processed. This caused
a delay of several hours until the job could loop again on the pending messages collection
and start processing new pending messages. We removed the blocking synchronisation point
and now let the job pick up new pending messages while ignoring the ones that are already
being processed.

Version 0.3.1
=============

This release fixes a synchronisation issue introduced in 0.3.0. Messages where the "item_type"
field was not specified could not be processed in real-time and had to wait for the on-chain
confirmation to be processed.

Version 0.3.0
=============

Breaking changes
****************

- The variables exposed on the metrics endpoint were seriously refactored. The following metrics
  were removed:
    * pyaleph_processing_pending_messages_gtasks_total
    * pyaleph_processing_pending_messages_action_total
    * pyaleph_processing_pending_messages_messages_actions_total
    * pyaleph_processing_pending_messages_i_total
    * pyaleph_processing_pending_messages_j_total
- The `allow_amend` and `code.comment` fields are now mandatory for PROGRAM messages.

New features
************

- Users can now query the programs that respond to Aleph messages using the new /programs/on/messages endpoint.
- Users can now forget aggregates by specifying one or more aggregate keys in the `aggregates` field
  of the FORGET message.
- The message processing loop was heavily refactored. Users can now set parallel processing
  limits by message type to tune the CCN to their network abilities.
- A lot of improvements were made regarding code quality, by validating user data earlier in the process and using
  classes after the validation. This results in minor performance improvements and increases maintainability of
  the code base.
- We now use rotating log files. This resolves an issue where nodes could go out of disk space because of the size of
  the log files.
- Resolved a 500 error that occurred when feeding an invalid hash format to /storage/raw/{hash}.


Internal changes
****************

- A lot more tests!.
- The throughput of the TXs and message processing loops was improved. The jobs are now guaranteed to execute
  the same number of tasks over time, instead of a varying amount before.
- We now use unordered bulk inserts when writing to the DB. This potentially improves performance.

Version 0.2.1
=============

- Improved the code for the storage module and improved test coverage.
- Fixed a major synchronisation issue between CCNs. A shared variable was not updated correctly, making it impossible
  for CCNs to fetch the content linked to random messages.
- Fixed an issue where IPFS/P2P jobs in charge of listening to the "alive" topics would not restart
  once an error occurred.
- Fixed an issue resulting in a KeyError if an IPFS pin timed out.
- CCNs now required Python 3.8+ instead of Python 3.6.

Version 0.2.0
=============

- Replaced the P2P service by jsp2pd, an official libp2p daemon. This lifts the dependency on py-libp2p.
- The `--gen-key` option is renamed to `--gen-keys`. It now stores the public key along with the private key,
  and a serialized version of the private key for use by the P2P daemon.
- The private key for the P2P host can no longer be provided through the config.yml file using the `p2p.key`
  field. The key must be provided as a serialized file in the `keys` directory.
- Decommissioned the support for RocksDB. The only supported storage engine is now MongoDB.
- Decommissioned the dockerized VMs as they were replaced by the micro-VMs.
- The message API now supports filtering by content key, start date and end date.
- The "protocol" P2P config is disabled until further notice as it is not working properly.
- Fixed minor issues in the index page of the web service.

Version 0.1
===========

- First version!
