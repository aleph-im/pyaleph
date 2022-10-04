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

function get_config()
{
  config_key="$1"
  config_value=$(python3 "${SCRIPT_DIR}/get_config_value.py" --config-file "${CONFIG_FILE}" "${config_key}")
  echo "${config_value}"
}

function wait_for_it()
{
  "${SCRIPT_DIR}"/wait-for-it.sh "$@"
}

POSTGRES_HOST=$(get_config postgres.host)
POSTGRES_PORT=$(get_config postgres.port)
IPFS_HOST=$(get_config ipfs.host)
IPFS_PORT=$(get_config ipfs.port)
RABBITMQ_HOST=$(get_config rabbitmq.host)
RABBITMQ_PORT=$(get_config rabbitmq.port)

if [ "$(get_config ipfs.enabled)" = "True" ]; then
  wait_for_it -h "${IPFS_HOST}" -p "${IPFS_PORT}"
fi

wait_for_it -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}"
wait_for_it -h "${RABBITMQ_HOST}" -p "${RABBITMQ_PORT}"

exec pyaleph "${PYALEPH_ARGS[@]}"
