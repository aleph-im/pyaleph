{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.postgresql
    pkgs.redis
    pkgs.kubo
    pkgs.rabbitmq-server

    pkgs.python311
    pkgs.python311Packages.virtualenv
    pkgs.python311Packages.pip
    pkgs.python311Packages.setuptools

    pkgs.python311Packages.secp256k1
    pkgs.python311Packages.fastecdsa
    pkgs.python311Packages.greenlet
  ];

  shellHook = ''
    echo "Setting up PostgreSQL environment..."
    export PGDATA=$(mktemp -d)
    PG_SOCKET_DIR=$(mktemp -d)
    echo "Initializing database..."
    initdb $PGDATA

    echo "Starting PostgreSQL with custom socket directory..."
    pg_ctl -D $PGDATA -o "-k $PG_SOCKET_DIR" -l logfile start

    # Wait a bit for the server to start
    sleep 1

    # Create the 'aleph' role and a database
    createuser -h $PG_SOCKET_DIR aleph
    createdb -h $PG_SOCKET_DIR aleph -O aleph

    # Create a temporary directory for Redis
    export REDIS_DATA_DIR=$(mktemp -d)
    redis-server --daemonize yes --dir $REDIS_DATA_DIR --bind 127.0.0.1 --port 6379
    echo "Redis server started. Data directory is $REDIS_DATA_DIR"

    echo "Starting IPFS Kubo..."
    export IPFS_PATH=$(mktemp -d)
    ipfs init
    ipfs daemon &
    echo "IPFS Kubo started. Data directory is $IPFS_PATH"

    echo "Starting RabbitMQ..."
    export RABBITMQ_MNESIA_BASE=$(mktemp -d)
    export RABBITMQ_LOG_BASE=$(mktemp -d)
    export RABBITMQ_CONFIG_FILE=$(mktemp)
    export RABBITMQ_ENABLED_PLUGINS_FILE=$(mktemp)
    echo "[
      {rabbit, [
        {mnesia_base, \"$RABBITMQ_MNESIA_BASE\"},
        {log_base, \"$RABBITMQ_LOG_BASE\"}
      ]}
    ]." > $RABBITMQ_CONFIG_FILE.config
    echo "[rabbitmq_management,rabbitmq_management_agent,rabbitmq_shovel]." > $RABBITMQ_ENABLED_PLUGINS_FILE
    rabbitmq-server -detached
    sleep 5 # Wait for RabbitMQ server to start fully before enabling plugins
    echo "RabbitMQ started. Data directory is $RABBITMQ_MNESIA_BASE, Logs directory is $RABBITMQ_LOG_BASE"
    echo "RabbitMQ Management UI enabled. Access it at http://localhost:15672/"

    echo
    echo "PostgreSQL started. Data directory is $PGDATA, Socket directory is $PG_SOCKET_DIR"
    echo "Redis started. Data directory is $REDIS_DATA_DIR"
    echo "Use 'psql -h $PG_SOCKET_DIR' to connect to the database."
    echo "Use 'redis-cli -p 6379' to connect to the Redis server."
    echo "To stop PostgreSQL: 'pg_ctl -D $PGDATA stop'"
    echo "To manually stop Redis: 'redis-cli -p 6379 shutdown'"
    echo "To stop RabbitMQ: 'rabbitmqctl stop'"

    # Trap the EXIT signal to stop services when exiting the shell
    trap 'echo "Stopping PostgreSQL..."; pg_ctl -D "$PGDATA" stop; echo "Stopping Redis..."; redis-cli -p 6379 shutdown; echo "Stopping IPFS Kubo..."; ipfs shutdown; echo "Stopping RabbitMQ..."; rabbitmqctl stop; deactivate' EXIT

    # Create a virtual environment in the current directory if it doesn't exist
    if [ ! -d "venv" ]; then
      python3 -m virtualenv venv
    fi

    # Install the required Python packages
    ./venv/bin/pip install -e .\[testing\]

    # Activate the virtual environment
    source venv/bin/activate
  '';
}
