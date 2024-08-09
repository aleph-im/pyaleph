# Aleph Core Channel Node (CCN)

Next generation network of decentralized big data applications. Development follows the [Aleph Whitepaper](https://github.com/moshemalawach/aleph-whitepaper).

## Documentation

Albeit still incomplete as it is a work in progress, documentation
can be found at http://pyaleph.readthedocs.io/ or 
built from this repository with `$ python setup.py docs`

## Deployment

We recommend following the 
[Installing a Core Channel Node](https://pyaleph.readthedocs.io/en/latest/guides/install.html)
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

### Developer setup using Nix

We started to add Nix as an easy way to setup a development environment.
This is still a work in progress and not all dependencies are covered yet.

To use it, you need to [have Nix installed on your system](https://nixos.org/download.html). Then you can run:

```bash
nix-shell
```
This will provide you with a shell with PostgreSQL, Redis, and IPFS running.

## Test

To run test you can run:

```bash
nix-shell --run "hatch run testing:test"
```

Or you can run the command in the nix shell:
```bash
nix-shell

# inside of nix shell
hatch run testing:test
```

### Run test locally

We do not recommed that you run tests directly on your machine because of the
complexity of doing so but if you want to do it you need to:

- install [hatch](https://github.com/pypa/hatch), you can use pipx for that (`pipx install hatch`) or install it in a virtualenv
- install postgresql, at least version 15.1, `apt install postgresql`
- install redis, at least version 7, `apt install redis`
- have de nightly version of rust install, you can use [rustup](https://rustup.rs/) for that and do a `rustup default nightly`

Then configure PostgreSQL for your local application using a `config.yml` file in the root of the project.

Here is an not extensible example configuration:

```yaml
postgres:
  port: 5432
  user: username
  password: password
  host:  # leave empty to use unix socket
```

All overloadable and default values can be found in `src/aleph/config.py` and
you can also customize the redis connection this way.

The you can run:

```bash
hatch run testing:test
```

Or any of the env you can see using `hatch env show`.

In case of doubt you can refer to the file `.github/workflows/pyaleph-ci.yml`,
this is how it runs on our CI.

## Software used

The Aleph CCN is written in Python and requires Python v3.8+. It will not work with older versions of Python.

It also relies on [IPFS](https://ipfs.io/).

## License

The Aleph CCN is open-source software, released under [The MIT License (MIT)](LICENSE.txt).
