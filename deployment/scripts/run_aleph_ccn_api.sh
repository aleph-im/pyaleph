#!/bin/bash
# Starts an Aleph Core Channel Node API server.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CONFIG_FILE="/var/pyaleph/config.yml"

while test $# -gt 0; do
  case "$1" in
  --help)
    help
    ;;
  --config)
    CONFIG_FILE="$2"
    shift
    ;;
  esac
  shift
done

source ${SCRIPT_DIR}/wait_for_services.sh
wait_for_services "${CONFIG_FILE}"

NB_WORKERS="${CCN_CONFIG_API_NB_WORKERS:-4}"
PORT=${CCN_CONFIG_API_PORT:-4024}
TIMEOUT="${CCN_CONFIG_API_TIMEOUT:-300}"

echo "Starting aleph.im CCN API server on port ${PORT} (${NB_WORKERS} workers)"

exec gunicorn \
  "aleph.api_entrypoint:create_app" \
  --bind 0.0.0.0:${PORT} \
  --worker-class aiohttp.worker.GunicornUVLoopWebWorker \
  --workers ${NB_WORKERS} \
  --timeout ${TIMEOUT} \
  --access-logfile "-"
