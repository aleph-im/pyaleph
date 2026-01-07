start-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml up -d

stop-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml down

reset-dev-env:
    docker compose -f deployment/docker-build/docker-compose.yml down -v
    docker compose -f deployment/docker-build/docker-compose.yml up -d

generate-migration:
    alembic revision --autogenerate

run: start-dev-env
    python3 -m aleph.commands

run-api: start-dev-env
    python3 -m aleph.api_entrypoint
