"""Read models that turn raw run metrics into comparisons: per-stage timings and ratios."""

from pydantic import BaseModel

from app.domain.models import (
    Benchmark,
    Comparison,
    Engine,
    PipelineRun,
    RunStatus,
    Stage,
)

BASELINE = Engine.DUCKDB


class StageTiming(BaseModel):
    wall_seconds: float
    cpu_seconds: float
    cpu_utilization: float
    peak_rss_mb: float
    rows_out: int


class RunSummary(BaseModel):
    run_id: str
    engine: Engine
    status: RunStatus
    current_stage: Stage | None
    total_seconds: float
    cpu_seconds: float
    cpu_utilization: float
    peak_rss_mb: float
    rows_per_second: float | None
    stages: dict[Stage, StageTiming]
    outputs: list[str]
    quality: dict[str, object] | None
    error: str | None


class ComparisonReport(BaseModel):
    comparison: Comparison
    runs: list[RunSummary]
    slowdown_vs_duckdb: dict[Engine, dict[str, float]]
    equivalent: bool | None


class BenchmarkRow(BaseModel):
    dataset_id: str
    rows: int
    engine: Engine
    status: RunStatus
    total_seconds: float
    stage_seconds: dict[Stage, float]
    peak_rss_mb: float
    cpu_utilization: float
    rows_per_second: float | None
    slowdown_vs_duckdb: float | None
    equivalent: bool | None
    error: str | None


class BenchmarkReport(BaseModel):
    benchmark: Benchmark
    rows: list[BenchmarkRow]
    markdown: str


def _ratio(numerator: float, denominator: float) -> float:
    return round(numerator / denominator, 2) if denominator else 0.0


def summarise_run(run: PipelineRun, raw_rows: int | None) -> RunSummary:
    total = run.total_seconds
    return RunSummary(
        run_id=run.run_id,
        engine=run.engine,
        status=run.status,
        current_stage=run.current_stage,
        total_seconds=round(total, 3),
        cpu_seconds=round(run.cpu_seconds, 3),
        cpu_utilization=_ratio(run.cpu_seconds, total),
        peak_rss_mb=run.peak_rss_mb,
        rows_per_second=round(raw_rows / total)
        if raw_rows and total and run.status is RunStatus.SUCCEEDED
        else None,
        stages={
            metrics.stage: StageTiming(
                wall_seconds=metrics.wall_seconds,
                cpu_seconds=metrics.cpu_seconds,
                cpu_utilization=round(metrics.cpu_utilization, 2),
                peak_rss_mb=metrics.peak_rss_mb,
                rows_out=metrics.rows_out,
            )
            for metrics in run.stages
        },
        outputs=list(run.outputs),
        quality=run.quality,
        error=run.error,
    )


def slowdown_vs_baseline(runs: list[PipelineRun]) -> dict[Engine, dict[str, float]]:
    """How many times slower each engine is than DuckDB, per stage and in total."""
    by_engine = {run.engine: run for run in runs if run.status is RunStatus.SUCCEEDED}
    baseline = by_engine.get(BASELINE)
    if baseline is None:
        return {}
    baseline_stages = {metrics.stage: metrics.wall_seconds for metrics in baseline.stages}
    result: dict[Engine, dict[str, float]] = {}
    for engine, run in by_engine.items():
        if engine is BASELINE:
            continue
        ratios = {
            str(metrics.stage): _ratio(metrics.wall_seconds, baseline_stages.get(metrics.stage, 0.0))
            for metrics in run.stages
        }
        ratios["total"] = _ratio(run.total_seconds, baseline.total_seconds)
        result[engine] = ratios
    return result


def build_comparison_report(
    comparison: Comparison, runs: list[PipelineRun], raw_rows: int | None
) -> ComparisonReport:
    return ComparisonReport(
        comparison=comparison,
        runs=[summarise_run(run, raw_rows) for run in runs],
        slowdown_vs_duckdb=slowdown_vs_baseline(runs),
        equivalent=comparison.verification.equivalent if comparison.verification else None,
    )


def _total_slowdown(run: PipelineRun, slowdowns: dict[Engine, dict[str, float]]) -> float | None:
    if run.engine is BASELINE:
        return 1.0 if run.status is RunStatus.SUCCEEDED else None
    return slowdowns.get(run.engine, {}).get("total")


def build_benchmark_report(
    benchmark: Benchmark,
    comparisons: list[Comparison],
    runs: dict[str, PipelineRun],
    rows_by_dataset: dict[str, int],
) -> BenchmarkReport:
    table: list[BenchmarkRow] = []
    for comparison in comparisons:
        comparison_runs = [runs[run_id] for run_id in comparison.run_ids.values() if run_id in runs]
        slowdowns = slowdown_vs_baseline(comparison_runs)
        raw_rows = rows_by_dataset.get(comparison.dataset_id, 0)
        for run in comparison_runs:
            summary = summarise_run(run, raw_rows)
            table.append(
                BenchmarkRow(
                    dataset_id=comparison.dataset_id,
                    rows=raw_rows,
                    engine=run.engine,
                    status=run.status,
                    total_seconds=summary.total_seconds,
                    stage_seconds={stage: timing.wall_seconds for stage, timing in summary.stages.items()},
                    peak_rss_mb=summary.peak_rss_mb,
                    cpu_utilization=summary.cpu_utilization,
                    rows_per_second=summary.rows_per_second,
                    slowdown_vs_duckdb=_total_slowdown(run, slowdowns),
                    equivalent=comparison.verification.equivalent if comparison.verification else None,
                    error=run.error,
                )
            )
    return BenchmarkReport(benchmark=benchmark, rows=table, markdown=render_markdown(table))


def render_markdown(rows: list[BenchmarkRow]) -> str:
    stages = list(Stage)
    header = [
        "rows",
        "engine",
        "status",
        *[str(stage) for stage in stages],
        "total s",
        "peak MB",
        "cores",
        "rows/s",
        "x DuckDB",
        "equivalent",
    ]
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for row in rows:
        cells = [
            f"{row.rows:,}",
            row.engine,
            row.status,
            *[f"{row.stage_seconds[stage]:.2f}" if stage in row.stage_seconds else "-" for stage in stages],
            f"{row.total_seconds:.2f}",
            f"{row.peak_rss_mb:,.0f}",
            f"{row.cpu_utilization:.1f}",
            f"{row.rows_per_second:,.0f}" if row.rows_per_second else "-",
            f"{row.slowdown_vs_duckdb:.1f}x" if row.slowdown_vs_duckdb else "-",
            {True: "yes", False: "NO", None: "-"}[row.equivalent],
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)
