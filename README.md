# Aleph Core Channel Node (CCN)

Next generation network of decentralized big data applications. Development follows the [Aleph Whitepaper](https://github.com/moshemalawach/aleph-whitepaper).

## Documentation

Albeit still incomplete as it is a work in progress, documentation
can be found at http://pyaleph.readthedocs.io/ or 
built from this repository with `$ python setup.py docs`

## Deployment

We recommend following the 
[Easy deployment using Docker-Compose](https://pyaleph.readthedocs.io/en/latest/guides/docker-compose.html)
section of the documentation to install a node.

## Development

Do you want to contribute to the development of the CCN?
Here is the procedure to install the development environment.
We recommend using Ubuntu 20.04.

### 1. Install dependencies

```bash
sudo apt install python3 python3-pip python3-venv build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libgmp-dev libsecp256k1-dev
```

### 2. Install Python requirements

Clone the repository and run the following commands from the root directory:

```
python3 -m virtualenv venv
source venv/bin/activate
pip install -e .[testing,docs]
```

You're ready to go!

## Software used

The Aleph CCN is written in Python and requires Python v3.8+. It will not work with older versions of Python.

It also relies on [MongoDB](https://www.mongodb.com/) and [IPFS](https://ipfs.io/).

## License

The Aleph CCN is open-source software, released under [The MIT License (MIT)](LICENSE.txt).
