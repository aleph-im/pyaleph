#!/bin/bash

# Use this script to build the Docker image of the Core Channel Node

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

$DOCKER_COMMAND  build -t pyaleph-node:v0.5.3 -f "$SCRIPT_DIR/pyaleph.dockerfile" .
