#!/bin/bash

# Use this script to build the Docker image of PyAleph

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$SCRIPT_DIR/../.."

# Use Podman if installed, else use Docker
if hash podman 2> /dev/null
then
  podman build -t aleph.im/pyaleph -f "$SCRIPT_DIR/Dockerfile" .
else
  docker build -t aleph.im/pyaleph -f "$SCRIPT_DIR/Dockerfile" .
fi
