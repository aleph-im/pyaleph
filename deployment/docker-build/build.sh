#!/bin/bash

# Build the Docker image of the Core Channel Node.

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

# Sets IMAGE_TAG and PEP440_VERSION
# Sets IMAGE, IMAGE_TAG, and PEP440_VERSION
source "${SCRIPT_DIR}/get_version.sh"
get_version

echo "Building ${IMAGE}:${IMAGE_TAG} (PEP440: ${PEP440_VERSION})"

"${DOCKER_COMMAND}" build \
  -f "${SCRIPT_DIR}/pyaleph.dockerfile" \
  -t "${IMAGE}:${IMAGE_TAG}" \
  --build-arg "VERSION=${PEP440_VERSION}" \
  .

echo
echo "Build complete: ${IMAGE}:${IMAGE_TAG}"
echo "To publish: ${SCRIPT_DIR}/publish.sh"
