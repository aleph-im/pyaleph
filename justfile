start-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml up -d

stop-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml down

reset-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml down -v
    docker compose -f deployment/docker-build/docker-compose.yml up -d

generate-migration:
    alembic revision --autogenerate

start-db:
    #!/bin/bash
    if ! docker compose -f deployment/docker-build/docker-compose.yml ps postgres | grep -q "running"; then
        docker compose -f deployment/docker-build/docker-compose.yml up -d postgres
    fi


upgrade-db: start-db
    alembic upgrade head
    
downgrade-db revision: start-db
    alembic downgrade {{revision}}

run: start-dev-env
    python3 -m aleph.commands

run-api: start-dev-env
    python3 -m aleph.api_entrypoint
