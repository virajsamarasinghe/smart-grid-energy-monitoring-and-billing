# Convenience wrappers; every target is a plain docker compose command (see README).
.PHONY: up down reset test verify logs ps psql bad-tariff rerun-billing

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose --profile test down -v --remove-orphans
	find data -mindepth 1 ! -name .gitkeep -exec rm -rf {} +

test:
	docker compose --profile test run --rm --build tests

verify:
	docker compose exec simulator python scripts/verify_idempotency.py

logs:
	docker compose logs -f --tail=50 simulator spark-streaming airflow-scheduler

ps:
	docker compose ps -a

psql:
	docker compose exec postgres psql -U smartgrid -d smartgrid

bad-tariff:
	docker compose exec simulator python -m simulator.tariff_simulator --day 2030-01-01 --drop-household H003

rerun-billing:
	docker compose exec airflow-scheduler airflow dags trigger smartgrid_billing -c '{"tariff_file": "tariffs_2026-01-01.csv"}'
