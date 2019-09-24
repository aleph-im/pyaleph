# PyAleph (Python node for the Aleph network)

Next generation network of decentralized big data applications. Developement follows the [Aleph Whitepaper](https://github.com/moshemalawach/aleph-whitepaper).

Documentation (albeit still vastly incomplete as it is a work in progress) can be found at http://pyaleph.readthedocs.io/ or built from this repo with `$ python setup.py docs`

## Dependencies

PyAleph requires Python v3.6+ (it won't work with older python versions).

## Installation

To use the rocksdb dependency you will need to build it, here are the requirements on ubuntu:

`$ sudo apt install python3-dev build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev librocksdb-dev`

You need to install the requirements, ideally in an empty virtualenv (I let
that part to you):

`$ python setup.py develop`

Then, once it's installed, you need to copy the sample-config.yaml file elsewhere,
and edit it to your liking (see configuration section).

To run PyAleph, run this command:

`$ pyaelph -c config.yaml` (where config.yaml is your configuration file you
edited earlier)

## Running tests

Install in develop with all extras:

`$ pip install -e ".[bnb,testing]"`

Then run the tests:

`$ pytest`

## Running services required

### IPFS

You need to have a running go IPFS instance running and linked in the configuration file (TODO: write details).

PubSub should be active and configured to use GossipSub.
More info there: https://github.com/ipfs/go-ipfs/blob/master/docs/experimental-features.md#ipfs-pubsub

You can add our bootstrap node and connect to it on your ipfs node to be connected to the aleph network faster:

```
$ ipfs bootstrap add /dnsaddr/bootstrap.aleph.im/ipfs/QmPR8m8WCmYKuuxg5Qnadd4LbnTCD2L93cV2zPW5XGVHTG
$ ipfs swarm connect /dnsaddr/bootstrap.aleph.im/ipfs/QmPR8m8WCmYKuuxg5Qnadd4LbnTCD2L93cV2zPW5XGVHTG
```

### Mongodb

A local running mongodb instance is required, by default it's connected to localhost port 27017, you can change
the configuration file if needed.

## Configuration

TODO
