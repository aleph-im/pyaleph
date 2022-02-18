Native install
==============


To install PyAleph, you must first install its dependencies.
Here are the requirements on Ubuntu 20.04:

.. code-block:: bash

    sudo apt install python3-dev build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libgmp-dev libsecp256k1-dev

Then, you need to clone the repository / download the code of PyAleph and install the package.
Ideally, you should create a clean virtual environment.

.. code-block:: bash

    git clone https://github.com/aleph-im/pyaleph.git
    cd pyaleph
    python3 -m virtualenv venv
    source venv/bin/activate
    pip install .

The next step is to generate your configuration file and node secret keys.
Refer to the configuration section at the start of the install guide.

To run PyAleph, run this command:

.. code-block:: bash

    pyaleph -c config.yaml # where config.yaml is the configuration file you edited earlier

Running the required services
-----------------------------

PyAleph requires certain services to operate properly.
We require an IPFS daemon to connect to fetch IPFS files, a P2P daemon to connect to other nodes of the Aleph
network and a database (MongoDB) to store data.

IPFS
^^^^

You can have a running go IPFS instance running and linked in the configuration file (TODO: write details), if you don't you need to set ipfsd.enabled to false in configuration.

PubSub should be active and configured to use GossipSub.
More info there: https://github.com/ipfs/go-ipfs/blob/master/docs/experimental-features.md#ipfs-pubsub

You can add our bootstrap node and connect to it on your ipfs node to be connected to the aleph network faster:

.. code-block:: bash

    ipfs bootstrap add /dnsaddr/bootstrap.aleph.im/ipfs/QmPR8m8WCmYKuuxg5Qnadd4LbnTCD2L93cV2zPW5XGVHTG
    ipfs swarm connect /dnsaddr/bootstrap.aleph.im/ipfs/QmPR8m8WCmYKuuxg5Qnadd4LbnTCD2L93cV2zPW5XGVHTG


P2P daemon
^^^^^^^^^^

We use the official `JavaScript P2P daemon <https://github.com/libp2p/js-libp2p-daemon>`_ to manage P2P communication.
Run the following command to install it on your machine:

.. code-block:: bash

    npm install --global libp2p-daemon@0.10.2

You can then launch the daemon:

.. code-block:: bash

    jsp2pd --id <serialized_key_file> --listen=/ip4/0.0.0.0/tcp/4030 --hostAddrs=/ip4/0.0.0.0/tcp/4025 --pubsub=true --pubsubRouter=floodsub

Where <serialized_key_file> is the path to the serialized key file in the keys directory that you generated.

Mongodb
^^^^^^^

A local running mongodb instance is required, by default it's connected to localhost port 27017, you can change
the configuration file if needed.

To install mongodb:

.. code-block:: bash

    sudo apt install mongodb
