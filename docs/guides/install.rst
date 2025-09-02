*************************
Installing a PyAleph node
*************************

This tutorial is aimed at people wanting to run an Aleph node with little experience in system administration.

0. Hardware requirements
========================

A Linux server with the following requirements:
 - Ability to install Docker and Docker Compose (eg: recent Debian or Ubuntu)
 - Public IP address
 - The following ports open from the internet:
   - 4001/tcp
   - 4001/udp
   - 4024/tcp
   - 4025/tcp
 - Shell with `sudo` access
 - A text editor

1. Software requirements
========================

Install Docker.

On a Debian-based system (Debian, Ubuntu, Linux Mint, ...), you can use the following commands:

.. code-block:: bash

    sudo apt update
    sudo apt upgrade
    sudo apt install docker.io docker-compose
    sudo systemctl enable docker && sudo systemctl start docker

Add your user to the Docker group

.. code-block:: bash

    sudo usermod -a -G docker $(whoami)

Logout, and login again to apply the new group membership.

2. Configuration
================

PyAleph requires two configuration items:

- A configuration file, usually named config.yml
- A private key to identify the node on the P2P network.

Configuration file
------------------

This section describes how to create and customize the PyAleph configuration file.
First, download the PyAleph configuration template:

.. parsed-literal::

    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/|pyaleph_version|/deployment/samples/docker-compose/sample-config.yml"

Then rename the file to config.yml:

.. code-block:: bash

    mv sample-config.yml config.yml

Ethereum API URL
^^^^^^^^^^^^^^^^

Your Aleph node needs to connect to an Ethereum API.
If you do not run your own Ethereum node, we suggest you can use Infura or a similar service.

Register on `infura.io <https://infura.io/>`_, then create a new Ethereum project.
In the settings, get the hosted https:// endpoint URL for your project.

The endpoint should look like:
`https://rinkeby.infura.io/v3/<project-id>` for the test network or
`https://mainnet.infura.io/v3/<project-id>` for production.

Edit the `config.yml` file to add the endpoint URL in the field [ethereum > api_url].

Sentry DSN
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

An Aleph.im node must have a persistent public-private keypair to authenticate to the network.
These keys can be created using the Docker image.
We strongly advise to back up your keys once generated.

.. parsed-literal::

    mkdir keys
    docker run --rm --user root --entrypoint "" -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:|pyaleph_version| chown aleph:aleph /opt/pyaleph/keys
    docker run --rm --entrypoint "" -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:|pyaleph_version| pyaleph --gen-keys --key-dir /opt/pyaleph/keys

To check that the generation of the keys succeeded, check the content of your keys directory:

.. code-block:: bash

    ls keys/
    # node-pub.key  node-secret.pkcs8.der

3. Run the node with Docker Compose
===================================

Download the Kubo config file script and Docker Compose file that defines how to run PyAleph and IPFS together.

.. parsed-literal::
    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/|pyaleph_version|/deployment/scripts/001-update-ipfs-config.sh"
    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/|pyaleph_version|/deployment/samples/docker-compose/docker-compose.yml"

At this stage, you will need your configuration file and your keys.
Check the configuration section to see how you can generate them.

Modify the Docker Compose file you just downloaded to update the paths to your configuration file
and keys directory.
Then start running the node:

.. code-block:: bash

    docker-compose up -d

4. Check that everything is working well
========================================

Check the containers
---------------------

Check that all the containers have started.

.. code-block:: bash

    docker-compose ps

You should see the following three containers with a State of "Up":

.. list-table:: docker-compose ps
    :header-rows: 1

    * - Name
      - Command
      - State
      - Ports

    * - nfuser_ipfs_1
      - /sbin/tini -- /usr/local/b ...
      - Up
      - 0.0.0.0:4001->4001/tcp, 0.0.0.0:4001->4001/udp, 5001/tcp, 8080/tcp, 8081/tcp

    * - nfuser_pyaleph_1
      - pyaleph --config /opt/pyal ...
      - Up
      - 0.0.0.0:4024->4024/tcp, 0.0.0.0:4025->4025/tcp, 127.0.0.1:8000->8000/tcp

Check the metrics
------------------

Check that messages are being processed by the node by looking on the metric endpoint, by default http://127.0.0.1:4024/metrics .

The number of messages should change when you refresh the page, starting with the variable pyaleph_status_sync_pending_messages_total

This endpoint can be ingested by a monitoring solution such as `Prometheus <https://prometheus.io/>`_ to watch the dynamic of the node starting.

Check the logs
--------------

Make sure that no error is displayed in the logs.

You can use `docker-compose logs` and `docker logs` for this purpose.

Check IPFS
----------

IPFS Web UI: http://127.0.0.1:5001/webui

.. warning::
    This web interface is only accessible on localhost in the default Docker Compose configuration.
    The API running on port 5001 gives complete control over the IPFS daemon without authentication.
    Never expose this port on the Internet!

5. Register your node
=====================

To get rewards, you will need to register your new node on `the aleph.im account page <account.aleph.im>`_.
To do so, you will need the `multiaddress <https://multiformats.io/multiaddr/>`_ of your node.
To retrieve it, run the following command (assuming that NODE_IP_ADDR is the IP address of your node):

.. code-block:: bash

    curl -s http://NODE_IP_ADDRESS:4024/api/v0/info/public.json | jq -r .node_multi_addresses[0]

Simply copy-paste this address on the account page when registering your node.
