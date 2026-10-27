# One-word commands for the RiskCoin stack, e.g. `make up` or `make test`.
.PHONY: up down ps logs test test-db lint backfill catalog psql dashboard dbt-setup dbt-build dbt dbt-docs

# Use the project's virtual environment when there is one
PYTHON := $(if $(wildcard venv/bin/python),venv/bin/python,python3)

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f producer consumer live-risk

test:
	$(PYTHON) -m pytest -q

# The database tests TRUNCATE live_prices and price_bars_1m, so they run against a
# throwaway PostgreSQL container (the same setup as CI), never the real database.
test-db:
	-@docker rm -f riskcoin_test_db > /dev/null 2>&1
	docker run -d --rm --name riskcoin_test_db -p 55432:5432 \
		-e POSTGRES_USER=riskcoin -e POSTGRES_PASSWORD=riskcoin -e POSTGRES_DB=riskcoin_test \
		postgres:18 > /dev/null
	@for i in $$(seq 30); do \
		docker exec riskcoin_test_db pg_isready -q -h 127.0.0.1 -U riskcoin -d riskcoin_test && break; \
		sleep 1; \
	done
	TEST_DATABASE_URL=postgresql+psycopg2://riskcoin:riskcoin@localhost:55432/riskcoin_test \
		$(PYTHON) -m pytest -q tests/test_db_upserts.py; \
		status=$$?; docker stop riskcoin_test_db > /dev/null; exit $$status

lint:
	$(PYTHON) -m ruff check .

backfill:
	docker compose run --rm batch

catalog:
	docker compose run --rm coin-catalog python -m src.jobs.refresh_coin_catalog

# Uses the user and database the postgres container was created with (.env)
psql:
	docker exec -it riskcoin_postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# macOS; on Linux use xdg-open
dashboard:
	open http://localhost:8501

# dbt (transform/) runs in its own virtualenv, see requirements-dbt.txt. python-dotenv loads
# .env for it the same way the Python code reads it (spaces around = and quotes are fine).
DBT := cd transform && ../.venv-dbt/bin/dotenv -f ../.env run -- ../.venv-dbt/bin/dbt

dbt-setup:
	$(PYTHON) -m venv .venv-dbt
	.venv-dbt/bin/pip install -r requirements-dbt.txt

dbt-build:
	$(DBT) build --profiles-dir .

# Any other dbt command, e.g. make dbt ARGS="source freshness"
dbt:
	$(DBT) $(ARGS) --profiles-dir .

dbt-docs:
	$(DBT) docs generate --profiles-dir .
	$(DBT) docs serve --profiles-dir . --port 8080
