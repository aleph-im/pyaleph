#!/bin/bash
# Starts an Aleph Core Channel Node API server.
#
# Environment variables:
#   CCN_CONFIG_API_PORT         Listening port (default 4024).
#   CCN_CONFIG_API_NB_WORKERS   Number of gunicorn workers (default 4).
#   CCN_CONFIG_API_TIMEOUT      Worker timeout in seconds (default 300).
#   CCN_CONFIG_API_ACCESS_LOG   Set to 1 to enable gunicorn's own access log
#                               (default 0). The application already logs one
#                               JSON line per request on the "aleph.http"
#                               logger, so the access log is redundant.
#   PROMETHEUS_MULTIPROC_DIR    Directory for the per-worker Prometheus metric
#                               files. Defaults to a fresh directory under /tmp.
#                               A directory you provide must be empty at start:
#                               files left by a previous run would be read and
#                               added to the metrics.

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

ACCESS_LOG="${CCN_CONFIG_API_ACCESS_LOG:-0}"

# Must be exported before gunicorn (and thus prometheus_client) starts, so the
# workers aggregate their HTTP metrics in this directory.
# Assigned, then exported: "export VAR=$(...)" would hide a mktemp failure from
# set -e.
PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-$(mktemp -d /tmp/pyaleph-metrics.XXXXXX)}"
export PROMETHEUS_MULTIPROC_DIR

GUNICORN_ARGS=()
if [ "${ACCESS_LOG}" = "1" ]; then
  GUNICORN_ARGS+=(--access-logfile "-")
fi

echo "Starting aleph.im CCN API server on port ${PORT} (${NB_WORKERS} workers)"

exec gunicorn \
  "aleph.api_entrypoint:create_app" \
  --bind 0.0.0.0:${PORT} \
  --worker-class aiohttp.worker.GunicornUVLoopWebWorker \
  --workers ${NB_WORKERS} \
  --timeout ${TIMEOUT} \
  -c "${SCRIPT_DIR}/gunicorn_conf.py" \
  ${GUNICORN_ARGS[@]+"${GUNICORN_ARGS[@]}"}
