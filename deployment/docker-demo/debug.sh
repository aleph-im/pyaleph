#!/bin/bash

# Use this script to run the Docker image of PyAleph

set -euo pipefail

#podman run --name pyaleph \
#  -p 8081:8081 \
#  -p 0.0.0.0:4024:4024 \
#  -p 0.0.0.0:4025:4025 \
#  -p 4001:4001 \
#  -p 5001:5001 \
#  -p 8080:8080 \
#  --mount type=tmpfs,destination=/var/log \
#  -v pyaleph-mongodb:/var/lib/mongodb \
#  -v pyaleph-ipfs:/var/lib/ipfs \
#  -v "$(pwd)/config.yml:/opt/pyaleph/config.yml" \
#  -v "$(pwd)/src:/opt/pyaleph/src" \
#  --user aleph \
#  --rm -ti \
#  aleph.im/pyaleph-demo "$@"

if [ ! -f "$(pwd)/node-secret.key" ]; then
    touch "$(pwd)/node-secret.key"
fi

podman run --rm --name pyaleph --user root \
  -v "$(pwd)/node-secret.key:/opt/pyaleph/node-secret.key" \
  aleph.im/pyaleph-demo chown aleph:aleph /opt/pyaleph/node-secret.key

podman run --rm -ti --name pyaleph --user aleph \
  -v "$(pwd)/src:/opt/pyaleph/src" \
  -v "$(pwd)/node-secret.key:/opt/pyaleph/node-secret.key" \
  aleph.im/pyaleph-demo "$@"
