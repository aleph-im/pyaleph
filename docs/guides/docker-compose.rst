====================================
Easy deployment using Docker-Compose
====================================

Introduction
------------

This tutorial is aimed at people wanting to run an Aleph node with little experience in system administration.

----------
Components
----------

- `PyAleph <https://github.com/aleph-im/pyaleph>`_ is the official Aleph.im node software
- `Docker <https://www.docker.com>`_ is used to pack and deploy PyAleph and sotfware it relies upon
- `Docker Compose <https://docs.docker.com/compose/>`_ is used to run multiple software together using Docker
- `MongoDB <https://www.mongodb.com>`_ is the database used by PyAleph to store it's data
- `IPFS <https://ipfs.io/>`_ is used by PyAleph to store large files

The procedure below explains how to install and run PyAleph, with MongoDB and IPFS on a Linux server using
Docker and Docker Compose.

0. Requirements
---------------

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

1. Server setup
---------------

Install Docker.

On a Debian-based system (Debian, Ubuntu, Linux Mint, ...), you can use the following commands:

.. code-block:: bash

    sudo apt update
    sudo apt upgrade
    sudo apt install docker.io docker-compose gnupg2 pass ufw
    sudo systemctl enable docker && sudo systemctl start docker
    sudo ufw allow 22,4001,4024/tcp,4025/tcp

.. note::
    gnupg2 and pass are required for `docker login` below.

Add your user to the Docker group

.. code-block:: bash

    sudo usermod -a -G docker $(whoami)

Logout, and login again to apply the new group membership.

2. Configuration files
----------------------

Create and customize the PyAleph configuration file.

Download the PyAleph configuration template:

.. code-block:: bash

    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/master/deployment/docker-build/config.yml"


----------------
Ethereum API URL
----------------

Register on `infura.io <https://infura.io/>`_, then create a new Ethereum project.
In the settings, get the hosted https:// endpoint URL for your project.

The endpoint should look be in the form:
`https://rinkeby.infura.io/v3/<project-id>` for the test network or
`https://mainnet.infura.io/v3/<project-id>` for production.

Edit the `config.yml` file to add the endpoint URL in the field [ethereum > api_url].

---------------
Node secret key
---------------

An Aleph.im node should have a persistent public-private keypair to authenticate to the network.

Create a file that will be used by the Aleph.im node to store it's private key.

.. code-block:: bash

    touch node-secret.key


.. code-block:: bash

    docker run --rm -ti --user root -v $(pwd)/node-secret.key:/opt/pyaleph/node-secret.key alephim/pyaleph-node:beta chown aleph:aleph /opt/pyaleph/node-secret.key

.. code-block:: bash

    docker run --rm -ti -v $(pwd)/node-secret.key:/opt/pyaleph/node-secret.key alephim/pyaleph-node:beta pyaleph --gen-key


Optional: Check that the key file is not empty and make a backup of the key:

.. code-block:: bash

    cat node-secret.key


..
    ## Docker setup

    ### Create a personal access token on GitHub:
    - https://github.com/settings/tokens/new
    - Select `read:packages` then the button "Generate token"

    Login within Docker using the above access token:
    ```shell script
    docker login https://docker.pkg.github.com
    ```
    -->

3. Run the node with Docker Compose
-----------------------------------

Download the Docker Compose file that defines how to run PyAleph, MongoDB and IPFS together.

.. code-block:: bash

    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/master/deployment/docker-compose/docker-compose.yml"

The start running the node:

.. code-block:: bash

    docker-compose up -d

4. Check that everything is working well
----------------------------------------

---------------------
Check the containers
---------------------
Check that all the containers have started.

.. code-block:: bash

    docker-compose ps

You should see the following three containers with a State of "Up":

          Name                    Command               State                                      Ports                                    
----------------------------------------------------------------------------------------------------------------------------------------
nfuser_ipfs_1      /sbin/tini -- /usr/local/b ...   Up      0.0.0.0:4001->4001/tcp, 0.0.0.0:4001->4001/udp, 5001/tcp, 8080/tcp, 8081/tcp
nfuser_mongodb_1   docker-entrypoint.sh mongo ...   Up      27017/tcp                                                                   
nfuser_pyaleph_1   pyaleph --config /opt/pyal ...   Up      0.0.0.0:4024->4024/tcp, 0.0.0.0:4025->4025/tcp, 127.0.0.1:8000->8000/tcp 
    


------------------
Check the metrics
------------------

Check that messages are being processed by the node by looking on the metric endpoint, by default http://localhost:4024/metrics .

The number of messages should change when you refresh the page, starting with the variable pyaleph_status_sync_pending_messages_total

This endpoint can be ingested by a monitoring solution such as `Prometheus https://prometheus.io/`_ to watch the dynamic of the node starting.

--------------
Check the logs
--------------

Make sure that no error is displayed in the logs.

You can use `docker-compose logs` and `docker logs` for this purpose.

----------
Check IPFS
----------

IPFS Web UI: http://localhost:5001/webui

------------------------------
Check PyAleph data via MongoDB
------------------------------

MongoDB message counts

.. code-block:: bash

    docker exec -ti --user mongodb debian_mongodb_1 bash
    $ mongo
    > use aleph
    > show collections
    > db.messages.count()
    1468900
    > db.pending_messages.count()
    63
    > db.pending_messages.count()
    4

-----------------------------
Get alerted in case of errors
-----------------------------

You can use `Sentry https://sentry.io/`_, on premise or hosted, to get alerted if any exception occur.

Add the DSN given by Sentry in your configuration to enable it.
