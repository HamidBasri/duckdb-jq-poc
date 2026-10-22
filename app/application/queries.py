"""Query side: assemble report read-models from the repositories."""

from app.application.projections import Repositories
from app.application.reporting import (
    BenchmarkReport,
    ComparisonReport,
    build_benchmark_report,
    build_comparison_report,
)
from app.domain.models import Benchmark, Comparison, Entity, PipelineRun


class ReportQueries:
    def __init__(self, repositories: Repositories) -> None:
        self._repos = repositories

    def _trade_rows(self, dataset_id: str) -> int | None:
        dataset = self._repos.datasets.get(dataset_id)
        return dataset.row_counts.get(Entity.TRADES) if dataset else None

    def _runs(self, comparison: Comparison) -> list[PipelineRun]:
        return [
            run for run_id in comparison.run_ids.values() if (run := self._repos.runs.get(run_id)) is not None
        ]

    def comparison(self, comparison: Comparison) -> ComparisonReport:
        return build_comparison_report(
            comparison, self._runs(comparison), self._trade_rows(comparison.dataset_id)
        )

    def benchmark(self, benchmark: Benchmark) -> BenchmarkReport:
        comparisons = [
            comparison
            for comparison_id in benchmark.comparison_ids
            if (comparison := self._repos.comparisons.get(comparison_id)) is not None
        ]
        runs = {run.run_id: run for comparison in comparisons for run in self._runs(comparison)}
        rows_by_dataset = {c.dataset_id: self._trade_rows(c.dataset_id) or 0 for c in comparisons}
        return build_benchmark_report(benchmark, comparisons, runs, rows_by_dataset)
