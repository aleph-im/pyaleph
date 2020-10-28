#!/bin/bash

# Use this script to build the Docker image of PyAleph

set -euo pipefail

podman build -t aleph.im/pyaleph-demo -f deployment/docker-demo/Dockerfile .
