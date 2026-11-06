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
If you do not already have one, you need to create the directory, copy the key of your node in it
and adjust the ownership of the folder:

.. parsed-literal::

    mkdir keys
    cp node-secret.key keys/
    docker run --rm --user root --entrypoint "" -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:|pyaleph_version| chown -R aleph:aleph /opt/pyaleph/keys

Download the latest image
-------------------------

Upgrade the docker-compose file:

.. parsed-literal::

    mv docker-compose.yml docker-compose-old.yml
    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/|pyaleph_version|/deployment/samples/docker-compose/docker-compose.yml"
    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/|pyaleph_version|/deployment/samples/docker-compose/rabbitmq.conf"

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] pull

Copy your customizations to the new docker-compose if relevant:

.. parsed-literal::

    diff --color --side-by-side docker-compose-old.yml docker-compose.yml

Upgrade your node
-----------------

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] down

    docker-compose [-f <docker-compose-file>] \
            run \
            --entrypoint /opt/pyaleph/migrations/config_updater.py \
            pyaleph \
            --key-dir /opt/pyaleph/keys \
            --key-file /opt/pyaleph/keys/node-secret.key \
            --config /opt/pyaleph/config.yml \
            upgrade

    docker-compose [-f <docker-compose-file>] down

Check that your node's secret key has been migrated

.. code-block:: bash

    ls ./keys

This should output:

    node-pub.key  node-secret.key  serialized-node-secret.key

Finally, restart your node:

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] up -d

RabbitMQ 4
==========

The sample docker-compose files now use RabbitMQ 4.3 (``rabbitmq:4.3.7-management``) instead of
3.11, which is end of life and no longer receives security fixes.

RabbitMQ does not support upgrading a 3.11 data directory straight to 4.x (the supported path is
3.11, 3.12, 3.13, then 4.x, with all stable feature flags enabled at each step). You do not need
that path for PyAleph: the compose files declare no RabbitMQ data volume, and the broker only carries
wake-up signals, so you can simply recreate the container:

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] up -d rabbitmq

PyAleph declares its exchanges and queues again when it connects. Messages that were still
queued at that moment are dropped, which is harmless: each one only carries the ID of a row of the
pending messages/transactions tables in PostgreSQL, and the fetch, processing and transaction
jobs also poll the database periodically (every 1 to 5 seconds), so they pick up the pending rows
on their own.

RabbitMQ 4.3 refuses transient non-exclusive queues by default. PyAleph now declares its work
queues as durable, and at start it deletes and re-creates, as durable, a queue of the same name
left over by an older version (a warning is logged). The p2p-service 0.1.x component still
declares transient queues, so the compose files mount a small ``rabbitmq.conf`` (permitting the
``transient_nonexcl_queues`` deprecated feature) into the RabbitMQ container. If you copy only
``docker-compose.yml`` from the sample, also download ``rabbitmq.conf`` next to it (see the
``wget`` command above): without it Docker creates an empty directory at the mount path and
RabbitMQ starts without the permit.

PyAleph does not use classic queue mirroring or HA policies, so its removal in RabbitMQ 4 does not
affect you. If you run your own RabbitMQ with such policies, migrate them to quorum queues first.
