Forgets
=======

FORGET messages are meant to make the Aleph network forget/drop one or more messages
sent previously.
Users can forget any type of message, except for FORGET messages themselves.

When a FORGET message is received by a node, it will immediately:
* remove the ‘content’ and ‘item_content’ sections of the targeted messages
* add a field ‘removed_by’ that references to the processed FORGET message

In addition, any content related to the forgotten message currently stored in the DB
will be deleted, if no other message points to the same content. For example, a file
stored in local storage will be deleted, or a file pinned in IPFS will be unpinned.

Content format
--------------

The `content` field of a FORGET message must contain the following fields:

* `address` [str]: The address to which the aggregate belongs. Reserved for future developments.
* `time` [float]: The epoch timestamp of the message.
* `hashes` [List[str]]: The list of message hashes to forget
* `reason` [Optional[str]]: An optional explanation of why the user wants to forget these hashes.

Limitations
-----------

* At the moment, a user can only forget messages he sent himself.
