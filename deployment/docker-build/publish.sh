#!/bin/bash

# Use this script to publish the current Docker image of the CCN on Docker Hub

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$SCRIPT_DIR/../.."

# Use Podman if installed, else use Docker
if hash podman 2> /dev/null
then
  DOCKER_COMMAND=podman
else
  DOCKER_COMMAND=docker
fi

# Sets IMAGE_TAG
source "${SCRIPT_DIR}/get_version.sh"
get_version

DOCKER_IMAGE="alephim/pyaleph-node:${IMAGE_TAG}"
$DOCKER_COMMAND push "${DOCKER_IMAGE}"
echo "Successfully published ${DOCKER_IMAGE}"
