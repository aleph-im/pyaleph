# Aleph Core Channel Node (CCN) Docker (Beta)

This directory contains the `Dockerfile` to build and run the CCN in production,
as well as a Docker Compose file for use during development.

## Build the Docker image

You can build the Docker image simply using:
```shell script
./deployment/docker-build/build.sh
```

or by running the Docker build command from the root of the repository:
```shell script
docker build -t alephim/pyaleph-node:v0.5.3 -f deployment/docker-build/pyaleph.dockerfile .
```

## Configure the CCN

We provide a template configuration in the file `deployment/docker-build/config.yml`,
which you will want to customize for your system.

Change the Ethereum API URL to the endpoint you want the CCN to use.

To run the local dev environment, you will need to set the P2P daemon and IPFS hosts to `127.0.0.1`.

### Generate your node's private key

Please refer to the installation documentation for that:
https://pyaleph.readthedocs.io/en/latest/guides/install.html#node-secret-keys

## Start the dev environment

Please refer to the installtation documentation for that:
https://pyaleph.readthedocs.io/en/latest/guides/install.html#run-the-node-with-docker-compose
