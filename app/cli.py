"""Generate datasets and run benchmarks from the terminal.

uv run python -m app.cli generate --sizes 5k 50k 500k 1m 5m
uv run python -m app.cli benchmark --sizes 5k 50k 500k --format parquet
"""

import argparse
import asyncio
import logging

from app.api.schemas import BenchmarkRequest, DatasetProfile
from app.bootstrap import Container, build_container
from app.config import get_settings
from app.domain.models import DatasetStatus, Engine, SourceFormat


async def _generate(container: Container, profile: DatasetProfile) -> None:
    for spec in profile.specs():
        manifest = await container.dataset_orchestrator.ensure_ready(spec)
        status = "ready" if manifest.status is DatasetStatus.READY else f"FAILED: {manifest.error}"
        print(f"{manifest.dataset_id}: {status} ({manifest.generation_seconds or 0:.1f}s)")


async def _benchmark(container: Container, request: BenchmarkRequest) -> None:
    benchmark = await container.benchmarks.request(
        request.specs(),
        request.engines,
        request.source_format,
        request.params,
        request.verify,
        request.discard_outputs,
    )
    # The request already scheduled execution via its event; wait for that background task.
    while container.benchmarks.get(benchmark.benchmark_id).finished_at is None:
        await asyncio.sleep(1)
    report = container.reports.benchmark(container.benchmarks.get(benchmark.benchmark_id))
    reports_dir = container.settings.data_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"{benchmark.benchmark_id}.md").write_text(report.markdown + "\n")
    (reports_dir / f"{benchmark.benchmark_id}.json").write_text(report.model_dump_json(indent=2))
    print(report.markdown)
    print(f"\nsaved to {reports_dir / benchmark.benchmark_id}.{{md,json}}")
    if report.benchmark.error:
        print(f"benchmark error: {report.benchmark.error}")


async def _main(args: argparse.Namespace) -> None:
    container = build_container(get_settings())
    container.start()
    try:
        common = {
            "sizes": args.sizes,
            "seed": args.seed,
            "dirty_rate": args.dirty_rate,
            "hierarchy_depth": args.depth,
            "max_chain_length": args.chain,
        }
        if args.command == "generate":
            await _generate(container, DatasetProfile.model_validate(common))
        else:
            request = BenchmarkRequest.model_validate(
                common
                | {
                    "engines": args.engines,
                    "source_format": args.format,
                    "verify": not args.no_verify,
                    "discard_outputs": not args.keep_outputs,
                }
            )
            await _benchmark(container, request)
    finally:
        await container.stop()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.cli", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("generate", "benchmark"):
        command = sub.add_parser(name)
        command.add_argument("--sizes", nargs="+", default=["5k", "50k", "500k"])
        command.add_argument("--seed", type=int, default=42)
        command.add_argument("--dirty-rate", type=float, default=0.02)
        command.add_argument("--depth", type=int, default=7, help="counterparty hierarchy depth")
        command.add_argument("--chain", type=int, default=5, help="max amendment chain length")
        if name == "benchmark":
            command.add_argument(
                "--engines", nargs="+", default=[e.value for e in Engine], choices=list(Engine)
            )
            command.add_argument("--format", default=SourceFormat.PARQUET, choices=list(SourceFormat))
            command.add_argument("--no-verify", action="store_true")
            command.add_argument("--keep-outputs", action="store_true")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s: %(message)s")
    asyncio.run(_main(_parser().parse_args()))


if __name__ == "__main__":
    main()
