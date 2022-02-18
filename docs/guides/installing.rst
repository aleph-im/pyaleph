*************************
Installing a PyAleph node
*************************

Configuration
=============

PyAleph requires two configuration items:

- A configuration file, usually named config.yml
- A private key to identify the node on the P2P network.

Configuration file
------------------

This section describes how to create and customize the PyAleph configuration file.
First, download the PyAleph configuration template:

.. code-block:: bash

    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/master/deployment/samples/docker-compose/sample-config.yml"

Ethereum API URL
^^^^^^^^^^^^^^^^

Register on `infura.io <https://infura.io/>`_, then create a new Ethereum project.
In the settings, get the hosted https:// endpoint URL for your project.

The endpoint should look like:
`https://rinkeby.infura.io/v3/<project-id>` for the test network or
`https://mainnet.infura.io/v3/<project-id>` for production.

Edit the `config.yml` file to add the endpoint URL in the field [ethereum > api_url].

Sentry DNS
^^^^^^^^^^

`Sentry <https://sentry.io/>`_ can be used to track errors and receive alerts if an issue
occurs on the node.

To enable Sentry, add the corresponding
`DSN <https://docs.sentry.io/product/sentry-basics/dsn-explainer/>`_ in the configuration.

.. code-block:: yaml

    sentry:
        dsn: "https://<SECRET_ID>@<SENTRY_HOST>/<PROJECT_ID>"

Node secret keys
----------------

An Aleph.im node should have a persistent public-private keypair to authenticate to the network.

These keys can be created using the Docker image or the PyAleph package directly, depending
on the install method you choose (read below).
To check that the generation of the keys succeeded, print your private key:

.. code-block:: bash

    cat keys/node-secret.key

We strongly advise to back up your keys once generated.

Using Docker
^^^^^^^^^^^^

.. code-block:: bash

    mkdir keys
    docker run --rm -ti --user root -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:beta chown aleph:aleph /opt/pyaleph/keys
    docker run --rm -ti -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:beta pyaleph --gen-keys --key-dir /opt/pyaleph/keys

Using the native install
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    pyaleph --gen-keys --key-dir keys

Install
=======

There are currently two supported methods to install PyAleph:
- Docker Compose
- Native install.

While the native install is supported, we strongly advise to use the Docker Compose install as it is faster,
simpler and less prone to issues.


.. toctree::
   :maxdepth: 1

   docker-compose
   native-install

Updates
=======

To upgrade to a new version of PyAleph, please follow the procedure below.
We provide an automated tool to keep your configuration file and keys up to date.
You must stop the node before running the configuration updater.

Using Docker
^^^^^^^^^^^^

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] down
    docker run --rm -ti \
        -v $(pwd)/keys:/opt/pyaleph/keys \
        -v $(pwd)/node-secret.key:/opt/pyaleph/node-secret.key:ro \
        -v $(pwd)/config.yml:/opt/pyaleph/config.yml \
        alephim/pyaleph-node:beta \
        python3 /opt/pyaleph/migrations/config_updater.py --key-dir /opt/pyaleph/keys --config /opt/pyaleph/config.yml
    docker-compose [-f <docker-compose-file>] up -d

Using the native install
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    # From the git repository
    python3 deployment/migrations/config_updater.py --key-dir <your-key-dir> --config <your-config-file>
