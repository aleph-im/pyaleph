=====================================
Setting up a private aleph.im network
=====================================

For testing some people and organizations might want to setup a private
Aleph network.

Please note that it is not the recommended course of action: you will
be completely separated from the network and lose all public P2P features.

The best way is to work in your own test channel with no incentives applied to it.

Peers
-----

To make your Aleph network private, you must exclude it from the official network.
This can be achieved by changing the default bootstrap nodes and changing the P2P
channels.

Changing bootstrap nodes
^^^^^^^^^^^^^^^^^^^^^^^^

Bootstrap nodes are specified in the configuration under the `p2p.peers` section.
If you only set up one node, just make this section an empty list:

.. code-block:: yaml

    p2p:
        peers: []

If you wish to connect several nodes together, first set up one node with no peers
and then configure the other nodes to point to the first node.

Changing the P2P channels
^^^^^^^^^^^^^^^^^^^^^^^^^

Aleph nodes communicate with other nodes using P2P and IPFS pubsub channels.
To create a private network, we must make sure that our new network may not communicate
with nodes from another network using these channels.

While changing bootstrap nodes is enough to guarantee isolation for P2P channels,
IPFS channels are global. We recommend to rename the P2P channels as well as an additional
guarantee of isolation.

Here are the configuration options to change (use your own names, obviously):

.. code-block:: yaml

    aleph:
        queue_topic: TESTNET_P2P_MESSAGES

    ipfs:
        alive_topic: TESTNET_IPFS_ALIVE

    p2p:
        alive_topic: TESTNET_P2P_ALIVE
        topics:
            - TESTNET_P2P_ALIVE
            - TESTNET_P2P_MESSAGES

Chains
------

The last step is to disable the retrieval of archived messages on the supported blockchains.
To do this, you simply need to disable the sync from the config:

.. code-block:: yaml

    ethereum:
        enabled: false

    nuls2:
        enabled: false

IPFS
----

You might want your IPFS daemon to be in a private net too. We leave this topic to the IPFS documentation.

Config file example
-------------------

Here is a config file that summarizes the different requirements to create your own private Aleph network.

.. code-block:: yaml

    aleph:
        queue_topic: TESTNET_P2P_MESSAGES

    ethereum:
        enabled: false

    ipfs:
        alive_topic: TESTNET_IPFS_ALIVE

    nuls2:
        enabled: false

    p2p:
        alive_topic: TESTNET_P2P_ALIVE
        peers: []
        topics:
            - TESTNET_P2P_ALIVE
            - TESTNET_P2P_MESSAGES


Refer to the install guide for explanations on how to set up your node(s).
