.DEFAULT_GOAL := help
.PHONY: help setup check-tools jq install run test lint datasets benchmark scenarios server-stopped reset clean fresh

# The jq engine is tested with exactly this jq; other versions can lack builtins it uses (macOS's own
# jq 1.7.1 has no trim). make jq downloads it from the official release and checks it against jq.sha256.
JQ_VERSION := $(shell cat .jq-version)
JQ := .tools/jq

help:  ## list the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

setup: check-tools install  ## make the project ready: check tools, install, small datasets, run tests
	uv run python -m app.cli generate --sizes 5k 50k 500k
	uv run pytest -q
	@echo "Ready. Start the app with: make run"

check-tools: $(JQ)  ## check that uv is installed and the pinned jq is in .tools/
	@command -v uv >/dev/null || { echo "uv is missing: https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
	@[ "$$($(JQ) --version)" = "jq-$(JQ_VERSION)" ] || { echo "$(JQ) is not jq $(JQ_VERSION); delete it and run make jq"; exit 1; }
	@echo "uv $$(uv --version | cut -d' ' -f2), jq $(JQ_VERSION): ok"

jq: $(JQ)  ## download the pinned jq (version in .jq-version) into .tools/

$(JQ): .jq-version jq.sha256
	@command -v curl >/dev/null || { echo "curl is missing; it is needed to download jq"; exit 1; }
	@case "$$(uname -s)-$$(uname -m)" in \
	   Darwin-arm64) asset=jq-macos-arm64 ;; \
	   Darwin-x86_64) asset=jq-macos-amd64 ;; \
	   Linux-x86_64) asset=jq-linux-amd64 ;; \
	   Linux-aarch64|Linux-arm64) asset=jq-linux-arm64 ;; \
	   *) echo "No pinned jq for $$(uname -s) $$(uname -m). Install jq $(JQ_VERSION) and set POC_ENGINE__JQ_BINARY to it."; exit 1 ;; \
	 esac; \
	 mkdir -p .tools; \
	 echo "Downloading jq $(JQ_VERSION) ($$asset)"; \
	 curl -fsSL --retry 3 -o .tools/$$asset.part "https://github.com/jqlang/jq/releases/download/jq-$(JQ_VERSION)/$$asset" || exit 1; \
	 expected=$$(awk -v a="$$asset" '$$2 == a { print $$1 }' jq.sha256); \
	 actual=$$( (shasum -a 256 .tools/$$asset.part 2>/dev/null || sha256sum .tools/$$asset.part) | cut -d' ' -f1); \
	 if [ -z "$$expected" ] || [ "$$actual" != "$$expected" ]; then \
	   rm -f .tools/$$asset.part; echo "Checksum of $$asset does not match jq.sha256; nothing installed"; exit 1; \
	 fi; \
	 chmod +x .tools/$$asset.part && mv .tools/$$asset.part $(JQ); \
	 echo "jq $(JQ_VERSION) installed in $(JQ)"

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

clean: server-stopped  ## delete everything generated: datasets, reports, outputs, virtualenv, jq, caches
	rm -rf data .venv .tools .pytest_cache .ruff_cache .mypy_cache
	find app tests -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -name .DS_Store -not -path "./.git/*" -delete
	@echo "Back to a fresh checkout. Your code, docs, .vscode and description are untouched. Run: make setup"

fresh: clean setup  ## start from zero: clean everything, then set the project up again
