# PyAleph Docker (Beta)

This directory contains the `Dockerfile` to build and run PyAleph in production.

## Build the Docker image

You can build the Docker image simply using:
```shell script
./deployment/docker-build/build.sh
```

or by running the Docker build command from the root of the repository:
```shell script
docker build -t alephim/pyaleph-node -f deployment/docker/pyaleph.dockerfile .
```

## Configure PyAleph

We provide a template configuration in the file `deployment/docker/config.yml`,
which you will want to customize for your system.

Change the Ethereum API URL to the endpoint you want PyAleph to use.

### Generate your node's private key

An Aleph node needs an asymmetric key pair to communicate with other nodes on the network.

You can generate this key using the following commands after building the Docker image:
```shell script
docker run --rm -ti --user root -v $(pwd)/node-secret.key:/opt/pyaleph/node-secret.key alephim/pyaleph-node:latest pyaleph --gen-keys
```

## Running with Docker Compose

You can run PyAleph and it's dependencies MongoDB and IPFS using Docker Compose.

The configuration we provide allows you to run a reverse-proxy for HTTPS termination
on a docker network named `reverse-proxy`, so you will need to create it first:
 
```shell script
docker network create reverse-proxy
```

You can then run PyAleph using Docker Compose:
```shell script
docker-compose -f deployment/docker/docker-compose.yml up
```

## Running with another infrastructure

Have a look at the `docker-compose.yml` configuration to understand how PyAleph
can be run.
