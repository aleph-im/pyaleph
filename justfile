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
    bash deployment/docker-build/build.sh

publish-docker-image:
    bash deployment/docker-build/publish.sh
