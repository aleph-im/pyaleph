#!/bin/bash
# Starts an Aleph Core Channel Node.

set -euo pipefail

function help() {
  pyaleph -h
  exit 1
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CONFIG_FILE="/var/pyaleph/config.yml"

PYALEPH_ARGS=("$@")

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

exec pyaleph "${PYALEPH_ARGS[@]}"
