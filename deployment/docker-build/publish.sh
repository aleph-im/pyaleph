#!/bin/bash

# Publish the current Docker image of the CCN to Docker Hub.
#
# Tagging strategy:
#   0.10.0        → exact version (always)
#   0.10          → latest patch in the minor series (stable only)
#   stable        → latest stable release (stable only)
#   latest        → alias for stable (stable only)
#   0.10.1-rc0    → exact pre-release version (always)
#   rc            → latest release candidate (pre-release only)
#
# Node operators can use:
#   alephim/pyaleph-node:stable   — always get the latest stable
#   alephim/pyaleph-node:0.10.0   — pin to an exact version
#   alephim/pyaleph-node:0.10     — auto-update within a minor series
#   alephim/pyaleph-node:rc       — opt into release candidate testing

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

# Sets IMAGE, IMAGE_TAG
source "${SCRIPT_DIR}/get_version.sh"
get_version

# Verify the image was built
if ! "${DOCKER_COMMAND}" image inspect "${IMAGE}:${IMAGE_TAG}" > /dev/null 2>&1; then
  echo "Error: ${IMAGE}:${IMAGE_TAG} not found locally."
  echo "Run build.sh first."
  exit 1
fi

echo "Publishing ${IMAGE}:${IMAGE_TAG}"
echo

# Always push the exact version tag
"${DOCKER_COMMAND}" push "${IMAGE}:${IMAGE_TAG}"
echo "Pushed ${IMAGE}:${IMAGE_TAG}"

# Determine if this is a pre-release.
# Match version tags with a dash-separated pre-release suffix (e.g. 0.10.1-rc0, 0.5.3-a1).
# The suffix must be ONLY letters + digits (no hex hash chars after), so that dev
# builds like 0.10.0-a1b2c3d don't match.
if [[ "${IMAGE_TAG}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-(rc|a|alpha|beta)[0-9]+$ ]]; then
  # Pre-release: tag the rolling :rc alias
  "${DOCKER_COMMAND}" tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:rc"
  "${DOCKER_COMMAND}" push "${IMAGE}:rc"
  echo "Pushed ${IMAGE}:rc"

elif [[ "${IMAGE_TAG}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  # Stable release (X.Y.Z with no suffix): tag minor, stable, latest
  MINOR=$(echo "${IMAGE_TAG}" | cut -d. -f1,2)

  "${DOCKER_COMMAND}" tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:${MINOR}"
  "${DOCKER_COMMAND}" push "${IMAGE}:${MINOR}"
  echo "Pushed ${IMAGE}:${MINOR}"

  "${DOCKER_COMMAND}" tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:stable"
  "${DOCKER_COMMAND}" push "${IMAGE}:stable"
  echo "Pushed ${IMAGE}:stable"

  "${DOCKER_COMMAND}" tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:latest"
  "${DOCKER_COMMAND}" push "${IMAGE}:latest"
  echo "Pushed ${IMAGE}:latest"

else
  # Dev build (e.g. 0.10.0-abc1234): only the exact tag, already pushed above
  echo "Dev build — no channel tags applied"
fi

echo
echo "Done."
