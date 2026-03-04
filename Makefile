# =============================================================================
# Makefile — Prefect SPCS task runner
#
# Common operations for local development, testing, building, and deployment.
# Run `make help` to see all available targets.
# =============================================================================

.DEFAULT_GOAL := help
SHELL := /bin/bash

# --- Configuration -----------------------------------------------------------
CONNECTION      ?= aws_spcs
PYTHON_VERSION  ?= 3.12
COMPOSE_FILE    := docker-compose.yaml
COMPOSE_MON     := docker-compose.monitoring-local.yml

# --- Help --------------------------------------------------------------------
.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

# =============================================================================
# Development
# =============================================================================

.PHONY: install
install: ## Install project dependencies with uv
	uv sync

.PHONY: lint
lint: ## Run ruff check + format check
	uv run ruff check flows/ scripts/ tests/
	uv run ruff format flows/ scripts/ tests/ --check

.PHONY: lint-fix
lint-fix: ## Auto-fix lint issues
	uv run ruff check flows/ scripts/ tests/ --fix
	uv run ruff format flows/ scripts/ tests/

.PHONY: validate
validate: ## Validate all FlowSpec registry entries
	uv run python flows/deploy.py --validate

# =============================================================================
# Testing
# =============================================================================

.PHONY: test
test: ## Run all tests (excludes E2E)
	uv run pytest tests/ \
		--ignore=tests/test_e2e_hybrid.py \
		--ignore=tests/test_e2e_spcs.py \
		-q

.PHONY: test-verbose
test-verbose: ## Run all tests with verbose output
	uv run pytest tests/ \
		--ignore=tests/test_e2e_hybrid.py \
		--ignore=tests/test_e2e_spcs.py \
		-v

.PHONY: test-cov
test-cov: ## Run tests with coverage report
	uv run pytest tests/ \
		--ignore=tests/test_e2e_hybrid.py \
		--ignore=tests/test_e2e_spcs.py \
		--cov=flows --cov=scripts \
		--cov-report=term-missing

.PHONY: test-e2e
test-e2e: ## Run E2E tests against live infrastructure
	uv run pytest tests/test_e2e_spcs.py tests/test_e2e_hybrid.py -v -m e2e

.PHONY: test-monitoring
test-monitoring: ## Run monitoring-related tests only
	uv run pytest tests/test_monitoring.py -v

.PHONY: test-dashboard
test-dashboard: ## Run dashboard tests only
	uv run pytest tests/test_dashboard.py -v

.PHONY: test-ci
test-ci: ## Run CI/CD pipeline tests only
	uv run pytest tests/test_gitlab_ci.py -v

# =============================================================================
# Local Development Stack
# =============================================================================

.PHONY: local-up
local-up: ## Start local Prefect stack (server, worker, redis, postgres)
	docker compose -f $(COMPOSE_FILE) up -d

.PHONY: local-down
local-down: ## Stop local Prefect stack
	docker compose -f $(COMPOSE_FILE) down

.PHONY: local-logs
local-logs: ## Tail local stack logs
	docker compose -f $(COMPOSE_FILE) logs -f

.PHONY: local-ps
local-ps: ## Show local stack status
	docker compose -f $(COMPOSE_FILE) ps

.PHONY: local-monitoring-up
local-monitoring-up: ## Start local stack + monitoring (Prometheus, Grafana, Loki)
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_MON) up -d

.PHONY: local-monitoring-down
local-monitoring-down: ## Stop local stack + monitoring
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_MON) down

.PHONY: local-monitoring-logs
local-monitoring-logs: ## Tail monitoring container logs
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_MON) logs -f prometheus grafana loki

# =============================================================================
# Auth Proxy (local → SPCS)
# =============================================================================

.PHONY: auth-proxy
auth-proxy: ## Start local auth-proxy for SPCS (fixes CSRF for prefect deploy)
	docker compose -f docker-compose.auth-proxy.yaml up -d
	@echo ""
	@echo "Auth proxy running at http://localhost:4202/api"
	@echo "Run:  export PREFECT_API_URL=http://localhost:4202/api"

.PHONY: auth-proxy-down
auth-proxy-down: ## Stop local auth-proxy
	docker compose -f docker-compose.auth-proxy.yaml down

.PHONY: deploy-prefect
deploy-prefect: ## Deploy all Prefect flows via auth-proxy (prefect deploy --all)
	@docker compose -f docker-compose.auth-proxy.yaml up -d --wait 2>/dev/null || true
  cd flows && PREFECT_API_URL=http://localhost:4202/api uv run prefect deploy --all --prefect-file ../prefect.yaml

# =============================================================================
# SPCS Build & Deploy
# =============================================================================

.PHONY: build
build: ## Build and push Docker images to SPCS registry
	bash scripts/build_and_push.sh --connection $(CONNECTION)

.PHONY: deploy
deploy: ## Full SPCS deployment (specs + flows + deployments + automations)
	bash scripts/deploy.sh --connection $(CONNECTION)

.PHONY: deploy-flows
deploy-flows: ## Sync flows to SPCS stage and register deployments
	bash scripts/deploy_flows.sh --connection $(CONNECTION)

.PHONY: deploy-monitoring
deploy-monitoring: ## Deploy SPCS monitoring stack (Prometheus + Grafana + Loki)
	bash monitoring/deploy_monitoring.sh --connection $(CONNECTION)

.PHONY: deploy-dashboard
deploy-dashboard: ## Deploy Streamlit observability dashboard to SiS
	bash dashboard/deploy.sh --connection $(CONNECTION)

.PHONY: deploy-diff
deploy-diff: ## Show what deployments would change (dry-run)
	uv run python flows/deploy.py --diff --all

# =============================================================================
# SPCS Operations
# =============================================================================

.PHONY: status
status: ## Show SPCS service status
	snow sql -q "SELECT SYSTEM\$$GET_SERVICE_STATUS('PREFECT_DB.PREFECT_SCHEMA.PF_SERVER')" --connection $(CONNECTION)
	snow sql -q "SELECT SYSTEM\$$GET_SERVICE_STATUS('PREFECT_DB.PREFECT_SCHEMA.PF_WORKER')" --connection $(CONNECTION)
	snow sql -q "SELECT SYSTEM\$$GET_SERVICE_STATUS('PREFECT_DB.PREFECT_SCHEMA.PF_MONITOR')" --connection $(CONNECTION)

.PHONY: endpoints
endpoints: ## Show SPCS service endpoints
	snow sql -q "SHOW ENDPOINTS IN SERVICE PREFECT_DB.PREFECT_SCHEMA.PF_SERVER" --connection $(CONNECTION)
	snow sql -q "SHOW ENDPOINTS IN SERVICE PREFECT_DB.PREFECT_SCHEMA.PF_MONITOR" --connection $(CONNECTION)

.PHONY: logs-server
logs-server: ## Show PF_SERVER container logs
	snow spcs service logs PREFECT_DB.PREFECT_SCHEMA.PF_SERVER --container-name server --connection $(CONNECTION)

.PHONY: logs-worker
logs-worker: ## Show PF_WORKER container logs
	snow spcs service logs PREFECT_DB.PREFECT_SCHEMA.PF_WORKER --container-name worker --connection $(CONNECTION)

# =============================================================================
# Maintenance
# =============================================================================

.PHONY: update-versions
update-versions: ## Check for Prefect/Redis version updates
	bash scripts/update_versions.sh

.PHONY: rotate-secrets
rotate-secrets: ## Rotate Snowflake secrets
	bash scripts/rotate_secrets.sh --connection $(CONNECTION)

.PHONY: teardown
teardown: ## Tear down all SPCS resources (DESTRUCTIVE)
	@echo "WARNING: This will destroy all SPCS services, pools, and stages."
	@read -r -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	bash scripts/teardown.sh --connection $(CONNECTION)

.PHONY: teardown-monitoring
teardown-monitoring: ## Tear down SPCS monitoring stack only
	bash monitoring/teardown_monitoring.sh --connection $(CONNECTION)

.PHONY: clean
clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	rm -f coverage.xml report.xml
