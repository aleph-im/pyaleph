{ }:
let
  pkgs = import (fetchTarball "https://nixos.org/channels/nixos-24.11/nixexprs.tar.xz") {};
in
pkgs.mkShell {
  buildInputs = [
    pkgs.glibcLocales
    pkgs.libiconv  # for macos

    pkgs.ps

    pkgs.postgresql
    pkgs.postgresql.lib
    pkgs.redis
    pkgs.kubo
    pkgs.hatch
    pkgs.rustup

    pkgs.python312
    pkgs.python312Packages.virtualenv
    pkgs.python312Packages.pip
    pkgs.python312Packages.setuptools
    pkgs.python312Packages.distutils

    pkgs.python312Packages.fastecdsa
    pkgs.python312Packages.libnacl
    pkgs.python312Packages.greenlet
  ];

  shellHook = ''
    set -eu
    echo "Setting up PostgreSQL environment..."
    export PGDATA=$(mktemp -d)
    PG_SOCKET_DIR=$(mktemp -d)
    # avoid coliding with possible existing PostgreSQL instance
    PG_PORT=5434
    echo "Initializing database..."
    initdb $PGDATA

    echo "Starting PostgreSQL with custom socket directory..."
    pg_ctl -D $PGDATA -o "-k $PG_SOCKET_DIR -p $PG_PORT" -l logfile start

    # Wait a bit for the server to start
    sleep 1

    # Create the 'aleph' role and a database
    createuser -h $PG_SOCKET_DIR -p $PG_PORT aleph
    createdb -h $PG_SOCKET_DIR -p $PG_PORT aleph -O aleph

    # Create a temporary directory for Redis
    export REDIS_DATA_DIR=$(mktemp -d)
    redis-server --daemonize yes --dir $REDIS_DATA_DIR --bind 127.0.0.1 --port 6379
    echo "Redis server started. Data directory is $REDIS_DATA_DIR"

    echo "Starting IPFS Kubo..."
    export IPFS_PATH=$(mktemp -d)
    ipfs init
    ipfs daemon &
    echo "IPFS Kubo started. Data directory is $IPFS_PATH"

    # Trap the EXIT signal to stop services when exiting the shell
    trap 'echo "Stopping PostgreSQL..."; pg_ctl -D "$PGDATA" stop; echo "Stopping Redis..."; redis-cli -p 6379 shutdown; echo "Stopping IPFS Kubo..."; ipfs shutdown; deactivate' EXIT

    # Create a virtual environment in the current directory if it doesn't exist
    if [ ! -d "venv" ]; then
      python3 -m virtualenv venv
    fi

    # Install the required Python packages
    ./venv/bin/pip install -e ".[testing]"

    # PyO3 requires a nightly or dev version of Rust.
    rustup default nightly

    # If config.yml does not exist, create it with the port specified in this shell. 
    echo -e "postgres:\n  host: "localhost"\n  port: $PG_PORT" > config.yml

    # bold
    echo -e "\e[1m"
    echo "PostgreSQL started. Data directory is $PGDATA, Socket directory is $PG_SOCKET_DIR" | sed 's/./=/g'
    echo "PostgreSQL started. Data directory is $PGDATA, Socket directory is $PG_SOCKET_DIR"
    echo "Redis started. Data directory is $REDIS_DATA_DIR"
    echo "Use 'psql -h $PG_SOCKET_DIR -p $PG_PORT -U aleph aleph' to connect to the database."
    echo "Use 'redis-cli -p 6379' to connect to the Redis server."
    echo "To stop PostgreSQL: 'pg_ctl -D $PGDATA -o "-p $PG_PORT" stop'"
    echo "To manually stop Redis: 'redis-cli -p 6379 shutdown'"
    echo "PostgreSQL started. Data directory is $PGDATA, Socket directory is $PG_SOCKET_DIR" | sed 's/./=/g'
    echo -e "\033[0m"
    set +eu

    # Activate the virtual environment
    source venv/bin/activate

    # Ensure libpq.so.5 can be found. 
    export LD_LIBRARY_PATH=${pkgs.postgresql.lib}/lib:$LD_LIBRARY_PATH
  '';
}
