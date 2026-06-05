SHELL := /bin/bash

APP_VENV := .venv-app
AGENTS_VENV := .venv-agents

COMPOSE := docker compose
COMPOSE_DEV := $(COMPOSE) -f docker-compose.yml -f docker-compose.dev.yml
COMPOSE_CLOUDSQL := $(COMPOSE) -f docker-compose.yml -f docker-compose.cloudsql.yml --profile cloudsql

.PHONY: help setup-env setup-app setup-agents run-api run-worker run-feedback run-all run-prod run-prod-cloudsql clean-legacy stop-all logs-all ps restart-all

help:
	@echo "Available targets:"
	@echo "  make setup-env      # create app/backendapi/.env, app/agents/.env, .env from *.example if missing"
	@echo "  make setup-app      # create .venv-app and install app deps"
	@echo "  make setup-agents   # create .venv-agents and install agent deps"
	@echo "  make run-api        # run platform API on :8000"
	@echo "  make run-worker     # run RQ worker"
	@echo "  make run-feedback   # run feedback agent on :5055"
	@echo "  make run-all        # local dev: compose + postgres + hot reload"
	@echo "  make run-prod       # GCP VM: api/worker/feedback-agent/redis"
	@echo "  make run-prod-cloudsql  # GCP VM + Cloud SQL proxy (stops legacy sheet-mcp-* first)"
	@echo "  make clean-legacy   # remove old sheet-mcp-* containers (frees :8000 / :5055)"
	@echo "  make stop-all       # stop all docker services"
	@echo "  make logs-all       # tail docker compose logs"
	@echo "  make ps             # show docker compose service status"
	@echo "  make restart-all    # restart all docker services"

setup-env:
	@test -f app/backendapi/.env || (cp app/backendapi/.env.example app/backendapi/.env && echo "Created app/backendapi/.env")
	@test -f app/agents/.env || (cp app/agents/.env.example app/agents/.env && echo "Created app/agents/.env")
	@test -f .env || (cp .env.example .env && echo "Created .env")
	@echo "Env files OK. Edit app/backendapi/.env and app/agents/.env (set OPENAI_API_KEY, OAuth, etc.)."

setup-app:
	python3 -m venv "$(APP_VENV)"
	"$(APP_VENV)/bin/pip" install -r requirements.txt

setup-agents:
	python3 -m venv "$(AGENTS_VENV)"
	"$(AGENTS_VENV)/bin/pip" install -r app/agents/requirements.txt

run-api:
	PYTHONPATH=app "$(APP_VENV)/bin/uvicorn" backendapi.main:app --reload --host 0.0.0.0 --port 8000

run-worker:
	PYTHONPATH=app "$(APP_VENV)/bin/python" -m backendapi.workers.run_worker

run-feedback:
	PYTHONPATH=app "$(AGENTS_VENV)/bin/uvicorn" agents.feedback.main:app --host 0.0.0.0 --port 5055

run-all: setup-env
	$(COMPOSE_DEV) up -d --build

run-prod: setup-env
	$(COMPOSE) up -d --build

clean-legacy:
	-docker rm -f sheet-mcp-api sheet-mcp-worker sheet-mcp-feedback-agent sheet-mcp-redis sheet-mcp-cloud-sql-proxy sheet-mcp-postgres 2>/dev/null
	@echo "Legacy sheet-mcp-* containers removed (if any were running)."

run-prod-cloudsql: setup-env clean-legacy
	$(COMPOSE_CLOUDSQL) down --remove-orphans 2>/dev/null || true
	$(COMPOSE_CLOUDSQL) up -d --build

stop-all:
	$(COMPOSE_DEV) down --remove-orphans 2>/dev/null || $(COMPOSE) down --remove-orphans

logs-all:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

restart-all:
	$(COMPOSE) restart
