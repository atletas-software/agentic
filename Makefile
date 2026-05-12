SHELL := /bin/bash

APP_VENV := .venv-app
AGENTS_VENV := .venv-agents

.PHONY: help setup-env setup-app setup-agents run-api run-worker run-feedback run-all stop-all logs-all ps restart-all

help:
	@echo "Available targets:"
	@echo "  make setup-env      # create app/.env, agents/.env, .env from *.example if missing"
	@echo "  make setup-app      # create .venv-app and install app deps"
	@echo "  make setup-agents   # create .venv-agents and install agent deps"
	@echo "  make run-api        # run platform API on :8000"
	@echo "  make run-worker     # run RQ worker"
	@echo "  make run-feedback   # run feedback agent on :5055"
	@echo "  make run-all        # setup-env + docker compose up (api/worker/feedback/redis/postgres)"
	@echo "  make stop-all       # stop all docker services"
	@echo "  make logs-all       # tail docker compose logs"
	@echo "  make ps             # show docker compose service status"
	@echo "  make restart-all    # restart all docker services"

setup-env:
	@test -f app/.env || (cp app/.env.example app/.env && echo "Created app/.env")
	@test -f agents/.env || (cp agents/.env.example agents/.env && echo "Created agents/.env")
	@test -f .env || (cp .env.example .env && echo "Created .env")
	@echo "Env files OK. Edit app/.env and agents/.env (set OPENAI_API_KEY, OAuth, etc.)."

setup-app:
	python3 -m venv "$(APP_VENV)"
	"$(APP_VENV)/bin/pip" install -r app/requirements.txt

setup-agents:
	python3 -m venv "$(AGENTS_VENV)"
	"$(AGENTS_VENV)/bin/pip" install -r agents/requirements.txt

run-api:
	"$(APP_VENV)/bin/uvicorn" app.main:app --reload --host 0.0.0.0 --port 8000

run-worker:
	"$(APP_VENV)/bin/python" -m app.workers.run_worker

run-feedback:
	PYTHONPATH=. "$(AGENTS_VENV)/bin/uvicorn" agents.feedback.main:app --host 0.0.0.0 --port 5055

run-all: setup-env
	docker compose up -d --build

stop-all:
	docker compose down

logs-all:
	docker compose logs -f

ps:
	docker compose ps

restart-all:
	docker compose restart
