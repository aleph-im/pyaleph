SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function get_config() {
  config_file="$1"
  config_key="$2"
  config_value=$(python3 "${SCRIPT_DIR}/get_config_value.py" --config-file "${CONFIG_FILE}" "${config_key}")
  echo "${config_value}"
}

function wait_for_it() {
  "${SCRIPT_DIR}"/wait-for-it.sh "$@"
}

function wait_for_services() {
  config_file="$1"

  POSTGRES_HOST=$(get_config "${config_file}" postgres.host)
  POSTGRES_PORT=$(get_config "${config_file}" postgres.port)
  IPFS_HOST=$(get_config "${config_file}" ipfs.host)
  IPFS_PORT=$(get_config "${config_file}" ipfs.port)
  RABBITMQ_HOST=$(get_config "${config_file}" rabbitmq.host)
  RABBITMQ_PORT=$(get_config "${config_file}" rabbitmq.port)
  REDIS_HOST=$(get_config "${config_file}" redis.host)
  REDIS_PORT=$(get_config "${config_file}" redis.port)
  P2P_SERVICE_HOST=$(get_config "${config_file}" p2p.daemon_host)
  P2P_SERVICE_CONTROL_PORT=$(get_config "${config_file}" p2p.control_port)

  if [ "$(get_config "${config_file}" ipfs.enabled)" = "True" ]; then
    wait_for_it -h "${IPFS_HOST}" -p "${IPFS_PORT}"
  fi

  wait_for_it -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}"
  wait_for_it -h "${RABBITMQ_HOST}" -p "${RABBITMQ_PORT}"
  wait_for_it -h "${REDIS_HOST}" -p "${REDIS_PORT}"
  wait_for_it -h "${P2P_SERVICE_HOST}" -p "${P2P_SERVICE_CONTROL_PORT}"
}
