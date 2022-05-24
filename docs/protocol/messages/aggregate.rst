Aggregates
==========

AGGREGATE messages are a global key/value store mechanism.

Content format
--------------

The `content` field of a FORGET message must contain the following fields:

* `address` [str]: The address to which the aggregate belongs. Reserved for future developments.
* `time` [float]: The epoch timestamp of the message.
* `key` [str]: The user-defined ID of the aggregate.
* `content` [Dict]: The key/value pairs making up the aggregate, as a dictionary.

Update aggregates
-----------------

Users can update aggregates by sending additional AGGREGATE messages with the same content key.
Updates are ordered by their content time field to match the order in which the user sent
the messages originally.

Retrieve aggregates
-------------------

Users can retrieve aggregates by using the `/api/v0/aggregates/{address}.json` endpoint.
Specify the `keys` URL parameter to restrict the response to one or more aggregates.
