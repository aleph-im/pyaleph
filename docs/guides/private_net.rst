=====================================
Setting up a private aleph.im network
=====================================

For testing some people and organization might want to setup a private
aleph.im network.

Please note that it is not the recommended course of action: you will
be completely separated from the network and lose all public P2P features.

The best way is to work in your own test channel with no incentives applied to it.

Peers
-----

You will need to setup a first pyaleph client that will serve as a seed node.
Copy the sample_config.yaml file to privatenet.yml, then:

- Set the enabled keys on all chains to false.
- Ensure your peers list is empty
- Use a specific queue topic so even if you connect to public network, messages don't slip.

Example of config file at this point (I disabled IPFS but you can leave it enabled):

.. code-block:: yaml

    nuls2:
        chain_id: 1
        enabled: False
        packing_node: False

    ethereum:
        enabled: False
        api_url: http://127.0.0.1:8545
        chain_id: 4
        packing_node: False

    mongodb:
        uri: "mongodb://127.0.0.1"
        database: alephtest

    storage:
        store_files: true
        engine: mongodb

    ipfs:
        enabled: False
        host: 127.0.0.1
        port: 5001
        gateway_port: 8080

    aleph:
        queue_topic: PRIVATENET

    p2p:
        host: 0.0.0.0
        control_port: 4020
        listen_port: 4021
        http_port: 4024
        port: 4025
        peers: []
        reconnect_delay: 60
        key: null

You then need to generate a private key to identify the node.
PyAleph provides a command-line option to do so.

.. code-block:: bash

    pyaleph --gen-keys --gen-key <your-key-dir>

This command creates a new directory that contains 3 keys: the private and public keys in PEM format,
as well as the key serialized for compatibility with the P2P daemon.

.. code-block:: bash

    ls <your-key-dir>
        node-pub.key  node-secret.key  serialized-node-secret.key

This key directory must be provided to the PyAleph daemon on startup using the `--key-dir <your-key-dir>` option.
It must also be passed to the P2P daemon using the `--id <your-key-dir>/serialized-node-secret.key` option.

Your seed node will need to have the 4025 and 4024 ports open (those ports are
configurable and you can change them).

Now restart the pyaleph daemon the same way, and you will see lines like this appear:

.. code-block:: 

    2020-04-01 12:31:54 [INFO] P2P.host: Listening on /ip4/0.0.0.0/tcp/4025/p2p/QmesN1F17tkEUx8bQY7Sayxmq8GXHZm9cXV7QpE1gt4n3D
    2020-04-01 12:31:54 [INFO] P2P.host: Probable public on /ip4/x.x.x.x/tcp/4025/p2p/QmesN1F17tkEUx8bQY7Sayxmq8GXHZm9cXV7QpE1gt4n3D

`x.x.x.x` being your public IP, `/ip4/x.x.x.x/tcp/4025/p2p/QmesN1F17tkEUx8bQY7Sayxmq8GXHZm9cXV7QpE1gt4n3D`
is your p2p multiaddress.

Other nodes will need to have this string in the peers section to be able to find each other. Example:

.. code-block:: yaml

    p2p:
        host: 0.0.0.0
        port: 4025
        http_port: 4024
        reconnect_delay: 60
        peers:
            - /ip4/x.x.x.x/tcp/4025/p2p/QmesN1F17tkEUx8bQY7Sayxmq8GXHZm9cXV7QpE1gt4n3D

For a healthy network it is recommended to have at least 2 seed nodes connected between each others,
and all other clients having them in their peer lists.

IPFS
----

You might want your IPFS daemon to be in a private net too, I'll leave that to IPFS documentation.

Synchronisation
---------------

To be able to keep your data synced you will need to write to at least one of the
supported chains. Either NULS2 or ETH.

The easiest one is NULS2, just use the sample sync info in the sample_config.yml,
using a target address (`sync_address` in config) you own, and using
a private key of an address that has a few nuls inside.
