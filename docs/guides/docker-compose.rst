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
- `Docker <https://www.docker.com>`_ is used to pack and deploy PyAleph and software it relies upon
- `Docker Compose <https://docs.docker.com/compose/>`_ is used to run multiple software together using Docker
- `MongoDB <https://www.mongodb.com>`_ is the database used by PyAleph to store it's data
- `IPFS <https://ipfs.io/>`_ is used by PyAleph to store large files
- `The libp2p daemon <https://github.com/libp2p/js-libp2p-daemon>`_ is used to manage P2P connections between nodes

The procedure below explains how to install and run PyAleph, with MongoDB, IPFS and the P2P daemon on a Linux server using
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
    sudo apt install docker.io docker-compose gnupg2 pass
    sudo systemctl enable docker && sudo systemctl start docker

.. note::
    gnupg2 and pass are required for `docker login` below.

Add your user to the Docker group

.. code-block:: bash

    sudo usermod -a -G docker $(whoami)

Logout, and login again to apply the new group membership.

------------------------------
Optional: Configure a firewall
------------------------------

A firewall can help you protect against unauthorized access on ports that should not be
exposed publicly. This guide section shows how to use the `UFW <https://launchpad.net/ufw>`_
simple and popular firewall.

.. code-block:: bash

    sudo apt install docker.io docker-compose gnupg2 pass ufw
    sudo ufw allow 22,4001,4024,4025/tcp
    sudo ufw allow 4001/udp

2. Run the node with Docker Compose
-----------------------------------

Download the Docker Compose file that defines how to run PyAleph, MongoDB and IPFS together.

.. code-block:: bash

    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/master/deployment/samples/docker-compose/docker-compose.yml"

At this stage, you will need your configuration file and your keys.
Check the configuration section to see how you can generate them.

Modify the Docker Compose file you just downloaded to update the paths to your configuration file
and keys directory.
Then start running the node:

.. code-block:: bash

    docker-compose up -d

3. Check that everything is working well
----------------------------------------

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

    * - nfuser_mongodb_1
      - docker-entrypoint.sh mongo ...
      - Up
      - 27017/tcp

    * - nfuser_pyaleph_1
      - pyaleph --config /opt/pyal ...
      - Up
      - 0.0.0.0:4024->4024/tcp, 0.0.0.0:4025->4025/tcp, 127.0.0.1:8000->8000/tcp

------------------
Check the metrics
------------------

Check that messages are being processed by the node by looking on the metric endpoint, by default http://localhost:4024/metrics .

The number of messages should change when you refresh the page, starting with the variable pyaleph_status_sync_pending_messages_total

This endpoint can be ingested by a monitoring solution such as `Prometheus <https://prometheus.io/>`_ to watch the dynamic of the node starting.

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
