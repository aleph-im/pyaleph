********
Messages
********

All data transferred over the aleph.im network are Aleph messages and represent the core of the Aleph networking model.

Messages can be:

- sent and received on the REST or other API interfaces
- exchanged on the peer-to-peer network
- stored on the underlying chains

.. uml::

    @startuml
        entity Message {
            .. Message info ..
            *type : text
            one of: POST, AGGREGATE, STORE
            *channel : text
            (channel of the message, one application ideally has one channel)
            *time : timestamp
            .. Sender info ..
            *sender : text <<address>>
            *chain : text
            (chain of sender: NULS, NULS2, ETH, DOT, CSDK, SOL...)
            -- Content --
            *item_hash <<hash>>
            if IPFS: multihash of json serialization of content
            if inline: hash of item_content using hash_type (sha256 only for now)
            *item_content : text <<json>>
            mandatory if of inline type, json serialization of the message
            #item_type : text (optional)
            one of: IPFS, inline.
            default: IPFS if no item_content, inline if there is
            #hash_type : text (optional)
            default: sha256 (only supported value for now)
        }

        hide circle
    @enduml

Actual content sent by regular users can currently be of three types:

- AGGREGATE: a key-value storage specific to an address
- POST: unique data posts (unique data points, events)
- STORE: file storage


.. uml::
   
   @startuml
    object Message {
        ...
    }

    object Aggregate <<message content>> {
        key : text
        address : text <<address>>
        ~ content : object
        time : timestamp
    }

    object Post <<message content>> {
        type : text
        address : text <<address>>
        ~ content : object
        time : timestamp
    }

 object Store <<message content>> {
        address : text <<address>>
        item_type : same as Message.item_type (note: it does not support inline)
        Item_hash: same as Message.item_hash
        time : timestamp
    }



    Message ||--o| Aggregate
    Message ||--o| Post
    Message ||--o| Post
    Message ||--o| Store
   @enduml


Message types
=============

.. toctree::
   :maxdepth: 1

   aggregate
   forget
   post
   program
   store

Item hash, type and content
===========================

Messages are uniquely identified by the `item_hash` field.
This value is obtained by computing the hash of the `content` field.
Currently, the hash can be obtained in one of two ways.
If the content of the message is stored on IPFS, the `item_hash` of the message will be the CIDv0
of this content.
Otherwise, if the message is stored on Aleph native storage or is included in the message, the item hash
will be the SHA256 hash of the message in hexadecimal encoding.
In the first case, the item type will be set to "ipfs". In the second case, the item type will either
be "inline" if the content is included in the message (serialized as a string in the `item_content` field)
or "storage".
Inline storage will be used for content up to 200kB.
Beyond this size, users must update the content prior to uploading the message on IPFS or Aleph storage.

Signature
=========

Aleph messages are cryptographically signed with the private key of the user.
The signature covers the `sender`, `chain`, `type` and `item_hash` fields of the message.
