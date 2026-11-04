# DuckDB vs jq

I wanted to know which tool is the better fit for turning large tables of trade data into clean,
nested JSON: DuckDB or jq. So I built the same pipeline twice, ran both on the same data from 5
thousand to 1 million rows, and checked that they produce exactly the same output.

**Report:** [hamidbasri.github.io/duckdb-jq-poc](https://hamidbasri.github.io/duckdb-jq-poc/)

**How it's built:** [architecture in ten minutes](https://hamidbasri.github.io/duckdb-jq-poc/architecture.html)
and the [visual blueprint](https://hamidbasri.github.io/duckdb-jq-poc/blueprint.html) (every part in
pictures, a one-minute summary and a glossary)

## Results

| Trades | DuckDB | jq | jq takes | Peak memory (DuckDB / jq) |
|---:|---:|---:|---:|---:|
| 5,000 | 0.23 s | 1.7 s | 7× longer | 165 MB / 102 MB |
| 50,000 | 0.88 s | 15 s | 17× longer | 362 MB / 129 MB |
| 500,000 | 3.7 s | 2 min 30 s | 40× longer | 1.8 GB / 359 MB |
| 1,000,000 | 11 s | 5 min 14 s | 29× longer | 2.6 GB / 632 MB |

DuckDB wins on speed by a wide margin, and it isn't only about cores: on a single thread it still
did 1M rows in 15.6 s. jq used less memory by default, but two DuckDB settings fix that (1.4 GB and
3.2 s at 1M rows). Both engines gave identical output in every run.

Measured on an Apple Silicon laptop (10 cores, 16 GB), Parquet input, one run per size.

## Quick start

You need Python 3.12+, [uv](https://docs.astral.sh/uv/) and jq 1.7 or newer.

```bash
make setup       # check tools, install, create small datasets, run the tests
make run         # dashboard on http://127.0.0.1:8000, trade tracker on /tracker
make benchmark   # 5k to 1M rows, both engines, results in data/reports/
```

## Commands

`make help` prints this list too.

| Command | What it does |
|---|---|
| `make setup` | Checks the tools, installs, creates the 5k, 50k and 500k datasets and runs the tests |
| `make check-tools` | Checks that uv and jq 1.7 or newer are installed |
| `make install` | Installs the Python dependencies |
| `make run` | Starts the API and dashboard on `http://127.0.0.1:8000` (trade tracker on `/tracker`) |
| `make test` | Runs the test suite |
| `make lint` | Runs ruff and mypy |
| `make datasets` | Creates every preset size, 5k to 5M (5M needs about 700 MB of disk) |
| `make benchmark` | Runs both engines on 5k to 1M rows, checks the outputs match, saves results in `data/reports/` |
| `make scenarios` | Stress tests: 200-level company trees and 100-step amendment chains, at 50k rows |
| `make reset` | Deletes run outputs, saved state (including rule versions), logs and caches. Keeps datasets and reports |
| `make clean` | Deletes everything generated: datasets, reports, outputs, virtualenv and caches |
| `make fresh` | `make clean`, then `make setup` |

`reset`, `clean` and `fresh` refuse to run while the API server is running.

The make targets call a small command-line tool you can also use directly:

```bash
uv run python -m app.cli generate --sizes 5k 1m --seed 7 --dirty-rate 0.05
uv run python -m app.cli benchmark --sizes 50k --engines duckdb --format csv --keep-outputs
```

Both take `--sizes`, `--seed`, `--dirty-rate`, `--depth` (company tree depth) and `--chain`
(amendment chain length). `benchmark` also takes `--engines`, `--format`, `--no-verify` and
`--keep-outputs`.

## What it does

The data generator builds five linked tables: trades, counterparties, instruments, books and FX
rates. It adds the kind of mess you see in real feeds: duplicates, missing values, odd formats,
links to rows that don't exist, amendments that point back to earlier trades, counterparty trees
seven levels deep, and a few loops. The same settings always give the same files, so every test can
be repeated. You can also upload your own CSV or Parquet files.

The pipeline has five stages: ingest, clean, validate, transform and aggregate. It removes
duplicates, fills gaps where it safely can, rejects bad trades with a reason, follows every link
and writes each trade as a nested JSON document. At the end it adds up the exposure per parent
company.

DuckDB runs the stages as SQL ([app/engines/duckdb_engine/sql](app/engines/duckdb_engine/sql)).
jq runs them as a series of jq programs over NDJSON files
([app/engines/jq_engine/programs](app/engines/jq_engine/programs)). jq can't read CSV or Parquet,
so DuckDB converts the input for it first, and that time counts as jq's.

After each comparison a checker matches both outputs document by document. A row tracker also
records where every input row ended up (delivered, rejected, duplicate or filtered out), and the
trade tracker page lets you search any trade and see its path through the stages. Rejected rows
show up there as soon as validation is done; the other rows follow when the run finishes.

The dashboard (`make run`) has an overview of the latest results and a tab each for datasets,
runs, benchmarks and live activity. If an upload's column names don't match the expected ones,
"Fix columns" suggests a match for each column and saves the result as a new version (v2, v3 and
so on); the original stays as it was. The Data tab shows every raw input table and every output
file. You can change raw values or remove rows there, which also saves a new version, and turn any
change into a rule. On the Rules tab, correction rules change values as data is read (for example
side `HOLD` to `BUY`), and validation switches turn reject rules off. Each change to the rules is
saved as a new version, new runs use it, and every run records the version it used. Rules that
would make the two engines disagree stay locked; `tests/test_parity.py` checks every other one.
Runs, benchmarks and datasets can be deleted, one at a time or with "Clear history".
Every run, benchmark and upload gets a number (Run 12, Benchmark 3, Upload 2) that is never
reused, so the Data tab, the activity log and the trade tracker always say which run the data
came from.

## How it's built

Requests return straight away and the work runs in the background. Services publish events
(`ComparisonRequested`, `StageCompleted` and so on) on an in-process event bus. Orchestrators pick
them up and send the heavy work to a pool of worker processes, two at a time by default, so
several datasets can be processed at once. Each job gets a fresh process, which keeps memory
measurements clean. The dashboard follows everything live over server-sent events.

```
app/
  domain/          models, events, interfaces, rule codes
  application/     services, orchestrators, pipeline runner, verification, reports
  engines/         duckdb_engine (SQL) and jq_engine (jq programs)
  infrastructure/  data generator, worker pool, storage, memory monitor, row tracker
  api/             FastAPI routes, dashboard and tracker pages
docs/              the report published on GitHub Pages
```

## API

| Endpoint | What it does |
|---|---|
| `POST /api/datasets/generate` | Generate a dataset (`rows`, `seed`, `dirty_rate`, `hierarchy_depth`, `max_chain_length`) |
| `POST /api/datasets/upload` | Upload CSV or Parquet files; only `trades` is required |
| `GET /api/datasets/{id}/columns` | Each file's columns next to the expected ones, with suggested matches |
| `POST /api/datasets/{id}/versions` | Save a copy with the columns renamed (`mapping`); the original is kept |
| `GET /api/datasets/{id}/tables/{table}` | A page of a raw input table, searchable |
| `POST /api/datasets/{id}/edits` | Save changed fields and removed rows as a new version |
| `GET /api/runs/{id}/outputs/{name}/page` | A page of trades, rejects or exposures, searchable |
| `GET` / `PUT /api/rules` | The current rules; saving creates a new version |
| `POST /api/comparisons` | Run the pipeline on one or both engines, with optional filters |
| `GET /api/comparisons/{id}` | Timings per stage, slowdown and the verification result |
| `GET /api/runs/{id}/rows` | Where each input row ended up |
| `GET /api/runs/{id}/outputs/{name}/download` | The full output file |
| `POST /api/benchmarks` | Run several sizes one after another |
| `GET /api/events` | Live event stream |
| `DELETE /api/datasets/{id}`, `/api/comparisons/{id}`, `/api/benchmarks/{id}` | Delete one item and its files |
| `DELETE /api/history` | Delete every finished run and benchmark; datasets are kept |

The full list is at `/docs` while the server runs.

## Settings

Set these as environment variables.

| Variable | Default | |
|---|---|---|
| `POC_DATA_DIR` | `data` | Where datasets, outputs and state are kept |
| `POC_MAX_WORKERS` | `2` | Jobs that can run at the same time |
| `POC_ENGINE__DUCKDB_THREADS` | all cores | Set to `1` to compare with jq on one core |
| `POC_ENGINE__DUCKDB_MEMORY_LIMIT` | `8GB` | DuckDB spills to disk above this |
| `POC_ENGINE__DUCKDB_LEAN` | `false` | Stream the output and drop tables early (less memory) |
| `POC_ENGINE__DUCKDB_PRESERVE_INSERTION_ORDER` | `true` | `false` is faster and leaner, but output row order changes |
| `POC_ENGINE__DUCKDB_STORAGE` | `memory` | `disk` keeps tables in a database file |
| `POC_ENGINE__JQ_MAX_RSS_MB` | `12000` | jq is stopped above this |
| `POC_ENGINE__TRACK_ROWS` | `true` | Build the per-row status table after each run |

## Limits

Each size ran once, so treat small differences as noise; a repeat run differed by up to 17%. Each
engine writes about 1.2 GB of JSON per million rows, which is why I didn't run 5M rows: it needs
around 20 GB of free disk. The 5M dataset is still there (`make datasets`) if you have the space.
The API has no login and binds to 127.0.0.1, so keep it on your own machine.
