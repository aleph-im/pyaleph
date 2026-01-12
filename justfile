start-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml up -d

stop-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml down

reset-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml down -v
    docker compose -f deployment/docker-build/docker-compose.yml up -d

generate-migration:
    alembic revision --autogenerate

format:
    hatch run linting:fmt

check-typing:
    hatch run linting:typing

start-db:
    #!/bin/bash
    if ! docker compose -f deployment/docker-build/docker-compose.yml ps postgres | grep -q "running"; then
        docker compose -f deployment/docker-build/docker-compose.yml up -d postgres
    fi

upgrade-db: start-db
    alembic upgrade head
    
downgrade-db revision: start-db
    alembic downgrade {{revision}}

run-tests: start-dev-env
    pytest -v .

run: start-dev-env
    python3 -m aleph.commands

run-api: start-dev-env
    python3 -m aleph.api_entrypoint

build-docker-image:
    #!/usr/bin/env bash
    commit_hash=$(git rev-parse --short HEAD)
    tag=$(git describe --tags --exact-match 2>/dev/null)
    if [ -z "$tag" ]; then
        latest_release=$(git describe --tags --abbrev=0 2>/dev/null || echo "0.0.0")
        tag="${latest_release}-${commit_hash}"
    fi
    docker build -f deployment/docker-build/pyaleph.dockerfile -t alephim/pyaleph-node:$tag --build-arg VERSION=$tag .