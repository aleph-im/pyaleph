#!/bin/bash

# Use this script to run the Docker image of PyAleph

set -euo pipefail

podman run --name pyaleph \
  -p 8081:8081 \
  -p 0.0.0.0:4024:4024 \
  -p 0.0.0.0:4025:4025 \
  -p 4001:4001 \
  -p 5001:5001 \
  -p 8080:8080 \
  --mount type=tmpfs,destination=/var/log \
  -v pyaleph-mongodb:/var/lib/mongodb \
  -v pyaleph-ipfs:/var/lib/ipfs \
  -v "$(pwd)/config.yml:/opt/pyaleph/config.yml" \
  --rm -ti \
  aleph.im/pyaleph-demo "$@"
