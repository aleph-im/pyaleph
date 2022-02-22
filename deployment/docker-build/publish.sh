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

VERSION=$(git describe --tags)

$DOCKER_COMMAND tag alephim/pyaleph-node alephim/pyaleph-node:$VERSION
$DOCKER_COMMAND push alephim/pyaleph-node:$VERSION docker.io/alephim/pyaleph-node:$VERSION
echo docker.io/alephim/pyaleph-node:$VERSION
