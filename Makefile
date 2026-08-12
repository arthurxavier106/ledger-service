.PHONY: up down migrate test test-concurrency lint fmt logs psql

up:            ## sobe tudo com um comando
	docker compose up -d --build

down:
	docker compose down -v

migrate:
	docker compose run --rm migrate

test:          ## suite completa (precisa do postgres de teste no ar)
	docker compose exec api pytest -q

test-concurrency:
	docker compose exec api pytest tests/test_concurrency.py -v

lint:
	ruff check src tests migrations loadtest

fmt:
	ruff check --fix src tests migrations

logs:
	docker compose logs -f api

psql:
	docker compose exec postgres psql -U ledger -d ledger

loadtest:      ## seed + locust (cenario via LOADTEST_SCENARIO)
	python loadtest/seed.py
	LOADTEST_SCENARIO=$${LOADTEST_SCENARIO:-baseline} locust -f loadtest/locustfile.py \
	  --headless -u 50 -r 50 -t 30s -H http://127.0.0.1:8000 --csv=results
