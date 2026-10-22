.DEFAULT_GOAL := help
.PHONY: help setup check-tools install run test lint datasets benchmark scenarios server-stopped reset reset-all

JQ_MIN_VERSION := 1.7

help:  ## list the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

setup: check-tools install  ## make the project ready: check tools, install, small datasets, run tests
	uv run python -m app.cli generate --sizes 5k 50k 500k
	uv run pytest -q
	@echo "Ready. Start the app with: make run"

check-tools:  ## check that uv and jq >= 1.7 are installed
	@command -v uv >/dev/null || { echo "uv is missing: https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
	@command -v jq >/dev/null || { echo "jq is missing: brew install jq (macOS) or apt install jq (Debian/Ubuntu)"; exit 1; }
	@version=$$(jq --version | sed 's/^jq-//'); \
	 lowest=$$(printf '%s\n%s\n' "$(JQ_MIN_VERSION)" "$$version" | sort -V | head -1); \
	 [ "$$lowest" = "$(JQ_MIN_VERSION)" ] || { echo "jq $$version is too old; need $(JQ_MIN_VERSION) or newer"; exit 1; }; \
	 echo "uv $$(uv --version | cut -d' ' -f2), jq $$version: ok"

install:  ## install Python dependencies
	uv sync

run:  ## API + dashboard on http://127.0.0.1:8000
	uv run uvicorn app.main:app --host 127.0.0.1 --port 8000

test:  ## run the test suite
	uv run pytest -q

lint:  ## ruff + mypy
	uv run ruff check app tests && uv run ruff format --check app tests && uv run mypy app

datasets:  ## all preset sizes, deterministic (seed 42); 5M needs ~700 MB of disk
	uv run python -m app.cli generate --sizes 5k 50k 500k 1m 5m

benchmark:  ## 5k..1M, verified, outputs discarded afterwards
	uv run python -m app.cli benchmark --sizes 5k 50k 500k 1m

scenarios:  ## stress the hard parts: deep nesting and long back-reference chains
	uv run python -m app.cli benchmark --sizes 50k --depth 200
	uv run python -m app.cli benchmark --sizes 50k --chain 100

# The API keeps state in memory and would write it back, so never wipe state under a live server.
server-stopped:
	@! pgrep -f "uvicorn app.main:app" >/dev/null || { echo "The API server is running. Stop it first."; exit 1; }

reset: server-stopped  ## delete temporary files: run outputs, saved state, logs, caches (keeps datasets and reports)
	rm -rf data/runs data/state
	rm -f data/*.log
	rm -rf .pytest_cache .ruff_cache .mypy_cache
	find app tests -type d -name __pycache__ -prune -exec rm -rf {} +
	@echo "Temporary files removed. Datasets in data/datasets and reports in data/reports were kept."

reset-all: reset  ## also delete datasets, reports and the virtualenv (back to a fresh checkout)
	rm -rf data .venv
	@echo "Everything generated is gone. Run: make setup"
