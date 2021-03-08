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
        port: 4025
        http_port: 4024
        peers: []
        reconnect_delay: 60
        key: null

You can now run the PyAleph daemon for the first time:

.. code-block:: bash

    pyaleph -c privatenet.yml

You will soon see in the logs something like this appear:

.. code-block:: 

    2020-04-01 12:26:56 [INFO] P2P.host: -----BEGIN RSA PRIVATE KEY-----
    MIIEowIBAAKCAQEAg45OZHmQqllE895YRuI+Qk+h+4VULuHRfwvR2v0qf3qI+ZAC
    LpmUYjIm7E5ia5Nj99cBumsCpG0+SAGZlMQi7lzWEiYwNV7jhrrUh6wV+4k9BESr
    vwhe59rtueKopZZJTvukBTAkIA99oyfHD8fq2fUf4RxzC3dd/2rm2EdqsGshAcHS
    UJHutu946+SfxyUvxQIk5jX+uupcClF37/gUia82sGkm6uTPCjhdrHqI/DTh17l/
    va1ptjSnTqgKY9HA8j761wVaHdkwgw632C2GhMCn1UokG2yvRqJsOq6EIp9c/fuH
    s6ggWJbbXkaqefdYB8ljhE0p5+C/oB1BbJ18vwIDAQABAoIBADoUKE25UYGzOXrE
    bYqVtVDHIUcOfLTZ4whIqpQYcpum+DPdPOlfyh9z7rUigdbmUhsHo+6t8ZOv2vAl
    LK19zcIX4DZQ/7WAN8iyUMO42FedJf/tZTlIM8X+ZDdNdpDsAV9KPwY/U6OH0zql
    g/9Wjjs9OZ7DVZL5Vtk9U76mANbzQqpTjfvCV1tB6wT7JQUjXIwlMyHxtTsvDlKo
    0KHfohuTxJAugDcAaVCmt1QnVUZdEkizJdusPPvWxA1Rmparx2IRVazNHKKjDToZ
    cc/IytGnblMjdL6staPuqnavr2ZEVlpAgfl0jcxx4a1XcNNh4Fw+jaatqN5xIEQX
    x1Xn5ukCgYEAtcCRPCH07wr3rmB/QfLPB+mBprugD7tff81BJ7IHKQFIp9jvl8VP
    z+XfnHlghlshTJZ3hL4yXuvPyBIvKaL7toFMAXSB6S0LyZ99RdzksZ85U5ithPX3
    0WO+oWEm9gaqfeT4EJKtRSMaF2m79lMDNTNRRtkxJJIKQABNZn2KWgUCgYEAuUxD
    /NWJjLsGXIduO3PNGj6jMT2FmTw4O6GzMbgRbYj3zlOlwqoX6MYLTJn4xfYBcpup
    vsViNXI+S6sFwc8s7Y3Cw3a3Iuc7RyZUVSudrcsP3PgafGPd0bt11Z1aTjfP/McS
    vCuaCfZA2rggXdvhelO46DKR7MEsYUzVsO0eAvMCgYAZXJKnmnFsPdKMAakgUbpz
    9zCBTKMsLtBHrCOQX3ZCUYyK52mfewgFEaWfVwySEvtVjZWF72hl+G/ZEjiEjdqj
    /+zUMybBm+iOLPQ1IHrFElvUf3SPHieDj3CVYlImeI2n3aCD54PIJvrIE5gH6lOD
    Q/LuePYzjTFi9ufWCmSY5QKBgBy+PdWccifIYyY7Q9gpEGm/yaS7vFuWwcpOPPO7
    b8ij9Hym8RGPPQI4pkwNnk9m57aVevFCwQc1X4BxWQVFU9zNnqafZa0eXU2eHnrP
    tzfcReuq+MDO5PvBrneiXv2/Hp5BayCRSuW8sza6VRr6HrHRBt/N6GDnXjEBsCwv
    u/YNAoGBAJGykosSP6R4kmff8ZB+tCbB5eHR/O6Da7U5JTolYiU0N2zlrbgCG/Im
    0ZLGdCBOUO2EXOojAo+Y+Abxmc7QszT9azS8XRDnKp6R0AhjuR8QvjiqjB4bfVFr
    pLi4ta+YG2JtnHIJXFpmnTpful2sx0ioZbDq8fAYLZXQ7n8VDceA
    -----END RSA PRIVATE KEY-----

This is your private key, now add it to your config file like this:

.. code-block:: yaml

    p2p:
        host: 0.0.0.0
        port: 4025
        http_port: 4024
        peers: []
        reconnect_delay: 60
        key: |
            -----BEGIN RSA PRIVATE KEY-----
            MIIEowIBAAKCAQEAg45OZHmQqllE895YRuI+Qk+h+4VULuHRfwvR2v0qf3qI+ZAC
            LpmUYjIm7E5ia5Nj99cBumsCpG0+SAGZlMQi7lzWEiYwNV7jhrrUh6wV+4k9BESr
            vwhe59rtueKopZZJTvukBTAkIA99oyfHD8fq2fUf4RxzC3dd/2rm2EdqsGshAcHS
            UJHutu946+SfxyUvxQIk5jX+uupcClF37/gUia82sGkm6uTPCjhdrHqI/DTh17l/
            va1ptjSnTqgKY9HA8j761wVaHdkwgw632C2GhMCn1UokG2yvRqJsOq6EIp9c/fuH
            s6ggWJbbXkaqefdYB8ljhE0p5+C/oB1BbJ18vwIDAQABAoIBADoUKE25UYGzOXrE
            bYqVtVDHIUcOfLTZ4whIqpQYcpum+DPdPOlfyh9z7rUigdbmUhsHo+6t8ZOv2vAl
            LK19zcIX4DZQ/7WAN8iyUMO42FedJf/tZTlIM8X+ZDdNdpDsAV9KPwY/U6OH0zql
            g/9Wjjs9OZ7DVZL5Vtk9U76mANbzQqpTjfvCV1tB6wT7JQUjXIwlMyHxtTsvDlKo
            0KHfohuTxJAugDcAaVCmt1QnVUZdEkizJdusPPvWxA1Rmparx2IRVazNHKKjDToZ
            cc/IytGnblMjdL6staPuqnavr2ZEVlpAgfl0jcxx4a1XcNNh4Fw+jaatqN5xIEQX
            x1Xn5ukCgYEAtcCRPCH07wr3rmB/QfLPB+mBprugD7tff81BJ7IHKQFIp9jvl8VP
            z+XfnHlghlshTJZ3hL4yXuvPyBIvKaL7toFMAXSB6S0LyZ99RdzksZ85U5ithPX3
            0WO+oWEm9gaqfeT4EJKtRSMaF2m79lMDNTNRRtkxJJIKQABNZn2KWgUCgYEAuUxD
            /NWJjLsGXIduO3PNGj6jMT2FmTw4O6GzMbgRbYj3zlOlwqoX6MYLTJn4xfYBcpup
            vsViNXI+S6sFwc8s7Y3Cw3a3Iuc7RyZUVSudrcsP3PgafGPd0bt11Z1aTjfP/McS
            vCuaCfZA2rggXdvhelO46DKR7MEsYUzVsO0eAvMCgYAZXJKnmnFsPdKMAakgUbpz
            9zCBTKMsLtBHrCOQX3ZCUYyK52mfewgFEaWfVwySEvtVjZWF72hl+G/ZEjiEjdqj
            /+zUMybBm+iOLPQ1IHrFElvUf3SPHieDj3CVYlImeI2n3aCD54PIJvrIE5gH6lOD
            Q/LuePYzjTFi9ufWCmSY5QKBgBy+PdWccifIYyY7Q9gpEGm/yaS7vFuWwcpOPPO7
            b8ij9Hym8RGPPQI4pkwNnk9m57aVevFCwQc1X4BxWQVFU9zNnqafZa0eXU2eHnrP
            tzfcReuq+MDO5PvBrneiXv2/Hp5BayCRSuW8sza6VRr6HrHRBt/N6GDnXjEBsCwv
            u/YNAoGBAJGykosSP6R4kmff8ZB+tCbB5eHR/O6Da7U5JTolYiU0N2zlrbgCG/Im
            0ZLGdCBOUO2EXOojAo+Y+Abxmc7QszT9azS8XRDnKp6R0AhjuR8QvjiqjB4bfVFr
            pLi4ta+YG2JtnHIJXFpmnTpful2sx0ioZbDq8fAYLZXQ7n8VDceA
            -----END RSA PRIVATE KEY-----

In YAML the pipe symbol shows a multiline string will follow.

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

For q heqlthy network it is recommended to have at least 2 seed nodes connected between each others,
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