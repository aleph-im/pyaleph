Stores
======

STORE messages tell the Aleph network to store data on behalf of the user.
The data can either be pinned to IPFS or stored in the native Aleph storage system depending
on the content item type.

Content format
--------------

The `content` field of a STORE message must contain the following fields:

* `address` [str]: The address to which the aggregate belongs. Reserved for future developments.
* `time` [float]: The epoch timestamp of the message.
* `item_type` [str]: `storage` or `ipfs`. Determines the network to use to fetch and store the file.
* `item_hash` [str]: Hash of the file to store. Must be a CIDv0 for IPFS, or a SHA256 hash for native storage.

Retrieve stored content
-----------------------

Users can retrieve uploaded files by using the `/api/v0/storage/raw/{hash}` endpoint.
