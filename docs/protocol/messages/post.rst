Posts
=====

Posts are unique data entries, that can be amended later on, like blog posts, comments, events...
Internally, POST messages are similar to STORE messages but differ in that they support amending
and only support JSON content.


Content format
--------------

The `content` field of a POST message must contain the following fields:

* `address` [str]: The address to which the aggregate belongs. Reserved for future developments.
* `time` [float]: The epoch timestamp of the message.
* `content` [Dict]: The JSON content of the post.
* `ref` [Optional[str]]: Used for amending. If specified, must be set to the item hash of the original
  message that created the post to modify.
* `type` [str]: User-defined content type.

Amend posts
-----------

Users can amend posts by sending additional POST messages referencing the original message.
To do so, the user must send a new POST message with the content `ref` field set to
the item hash of the original POST message.
Note that even if the user amends the message multiple times, the `ref` field must always
reference the original message, not the amendments.
Amendments are applied in the order of the content `time` field.

Retrieve posts
--------------

Users can retrieve posts by using the `/api/v0/posts.json` endpoint.
