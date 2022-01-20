#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
JSP2PD_VERSION=0.10.2
DOCKERFILE_VERSION=1.0.0

docker build \
  -f "${SCRIPT_DIR}/jsp2pd.dockerfile" \
  --build-arg JSP2PD_VERSION=${JSP2PD_VERSION} \
  -t alephim/jsp2pd:${JSP2PD_VERSION}-${DOCKERFILE_VERSION} \
  "${SCRIPT_DIR}"
