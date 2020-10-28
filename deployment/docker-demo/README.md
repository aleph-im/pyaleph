# PyAleph Docker Demo

This directory contains deployment configuration to run PyAleph 
in a monolithic Docker image for easy setup in demo scenarios. 

The image runs [Supervisord](http://supervisord.org/), which in turn
starts and supervises PyAleph, MongoDB and IPFS.  

All scripts below are expecting to run from the root of the repository, 
not from this directory.

## Initial setup

You will need an initial configuration file to run the node.
See `sample-config.yml` for an example of configuration file. 
This configuration can then be mounted on `/opt/pyaleph/config.yml`
when starting the Docker image.

### Ethereum

Ethereum is one of the blockchains that can be used with Aleph.

To connect to Ethereum, you can either use an access provider or 
host your own Ethereum node.

https://infura.io/ provides an easy access to Ethereum with a free plan.

[Geth](https://geth.ethereum.org/) can be used to run an Ethereum node.

Update your `config.yml`, section `ethereum` adequately.

## Operations

### Building the Docker image

```
./deployment/docker-demo/build.sh
```  

### Running with Docker

```
./deployment/docker-demo/run.sh
```

This script will persist data from IPFS and MongoDB in Docker volumes
named `pyaleph-mongodb` and `pyaleph-ipfs`. Logs are written in a 
tmpfs (RAM) to lower disk wear.

## Debugging

### Logs

Logs for PyAleph and IPFS can be found in `/var/log/supervisor`, 
and for MongoDB in `/var/log/mongodb/`.

### Process control

The command `supervisorctl` can be used to start/stop processes.
See [supervisord.org](http://supervisord.org/).

The file [supervisord.conf](supervisord.conf) describes how the services
are launched. 