{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [ pkgs.postgresql pkgs.redis ];

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

    # Trap the EXIT signal to ensure PostgreSQL is stopped when exiting the shell
    trap 'echo "Stopping PostgreSQL..."; pg_ctl -D "$PGDATA" stop; echo "Stopping Redis..."; redis-cli -p 6379 shutdown' EXIT

    echo
    echo "PostgreSQL started. Data directory is $PGDATA, Socket directory is $PG_SOCKET_DIR"
    echo "Redis started. Data directory is $REDIS_DATA_DIR"
    echo "Use 'psql -h $PG_SOCKET_DIR' to connect to the database."
    echo "Use 'redis-cli -p 6379' to connect to the Redis server."
    echo "To stop PostgreSQL: 'pg_ctl -D $PGDATA stop'"
    echo "To manually stop Redis: 'redis-cli -p 6379 shutdown'"
  '';
}
