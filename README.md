# pyaleph
Next generation network of decentralized big data applications

## Dependencies

PyAleph requires Python v3.6+ (it won't work with older python versions).

## Installation

You need to install the requirements, ideally in an empty virtualenv (I let
that part to you):

`$ python setup.py develop`

Then, once it's installed, you need to copy the sample-config.yaml file elsewhere,
and edit it to your liking (see configuration section).

To run PyAleph, run this command:

`$ nulsexplorer -c config.yaml` (where config.yaml is your configuration file you
edited earlier)

## Running services required

### IPFS

You need to have a running go IPFS instance running and linked in the configuration file (TODO: write details).

PubSub should be active and configured to use GossipSub.
More info there: https://github.com/ipfs/go-ipfs/blob/master/docs/experimental-features.md#ipfs-pubsub

### NULS

If you want to run with a local NULS instance (and not light client mode), you need to have a local fully synced NULS blockchain instance.

The first proof of concept uses a nulsexplorer instance, being a light client of it.
For maximum security, run your own with your own local NULS wallet.

### Mongodb

A local running mongodb instance is required.

## Configuration

TODO
