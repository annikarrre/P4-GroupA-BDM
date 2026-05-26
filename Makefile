.PHONY: up down reset logs ps airflow db migrate clean

COMPOSE=docker compose

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v
	$(COMPOSE) up --build -d

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

airflow:
	$(COMPOSE) exec airflow-webserver airflow dags list

db:
	docker compose exec postgres psql -U rico -d rico

migrate:
	docker compose exec postgres psql -U rico -d rico -f /docker-entrypoint-initdb.d/001_schema.sql

clean:
	$(COMPOSE) down -v --remove-orphans