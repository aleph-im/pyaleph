#!/bin/bash
# Regenerates the gRPC stubs for the P2P service client.
# Usage: scripts/gen_grpc_stubs.sh [python-interpreter]
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${1:-venv/bin/python}"
OUT=src/aleph/services/p2p/grpc_generated

mkdir -p "$OUT"
"$PYTHON" -m grpc_tools.protoc \
    -I proto \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    proto/aleph_p2p.proto

# The generated grpc module uses an absolute import; make it relative.
sed -i 's/^import aleph_p2p_pb2/from . import aleph_p2p_pb2/' "$OUT/aleph_p2p_pb2_grpc.py"
touch "$OUT/__init__.py"
echo "Generated stubs in $OUT"
