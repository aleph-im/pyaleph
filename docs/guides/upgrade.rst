************************
Upgrading a PyAleph node
************************

This tutorial explains how to upgrade your PyAleph node to a newer version.

Update the configuration and keys
=================================

Version 0.2.0 requires a new way to store the private key on disk.
We provide an automated tool to keep your configuration file and keys up to date.
You must stop the node before running the configuration updater.

Create the keys directory
-------------------------

The updater expects that you already created a keys/ directory to hold your private and public keys.
If you do not already have one, you need to create the directory and adjust the ownership of the folder:

.. code-block:: bash

    mkdir keys
    docker run --rm -ti --user root -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:beta chown aleph:aleph /opt/pyaleph/keys

Download the latest image
-------------------------

Upgrade the docker-compose file:

.. code-block:: bash

    mv docker-compose.yml docker-compose-old.yml
    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/master/deployment/samples/docker-compose/docker-compose.yml"

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] pull

Upgrade your node
-----------------

.. code-block:: bash

    docker run --rm -ti \
        -v $(pwd)/keys:/opt/pyaleph/keys \
        -v $(pwd)/node-secret.key:/opt/pyaleph/node-secret.key:ro \
        -v $(pwd)/config.yml:/opt/pyaleph/config.yml:ro \
        alephim/pyaleph-node:beta \
        python3 /opt/pyaleph/migrations/config_updater.py \
            --key-dir /opt/pyaleph/keys \
            --key-file /opt/pyaleph/node-secret.key \
            --config /opt/pyaleph/config.yml \
            upgrade

Check that your node's secret key has been migrated

.. code-block:: bash

    ls ./keys

This should output:

    node-pub.key  node-secret.key  serialized-node-secret.key

Finally, restart your node:

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] down
    docker-compose [-f <docker-compose-file>] up -d
