"""Benchmark suites: generate sizes on demand and compare engines across them."""

from fastapi import APIRouter, status
from fastapi.responses import PlainTextResponse

from app.api.deps import ContainerDep
from app.api.schemas import BenchmarkRequest
from app.application.reporting import BenchmarkReport

router = APIRouter(prefix="/api/benchmarks", tags=["benchmarks"])


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def request_benchmark(request: BenchmarkRequest, container: ContainerDep) -> BenchmarkReport:
    """Sizes run one after another (never concurrently) so timings are not distorted."""
    benchmark = await container.benchmarks.request(
        request.specs(),
        request.engines,
        request.source_format,
        request.params,
        request.verify,
        request.discard_outputs,
    )
    return container.reports.benchmark(benchmark)


@router.get("")
async def list_benchmarks(container: ContainerDep) -> list[BenchmarkReport]:
    return [container.reports.benchmark(benchmark) for benchmark in container.benchmarks.list_all()]


@router.get("/{benchmark_id}")
async def get_benchmark(benchmark_id: str, container: ContainerDep) -> BenchmarkReport:
    return container.reports.benchmark(container.benchmarks.get(benchmark_id))


@router.get("/{benchmark_id}/report.md", response_class=PlainTextResponse)
async def benchmark_markdown(benchmark_id: str, container: ContainerDep) -> str:
    return container.reports.benchmark(container.benchmarks.get(benchmark_id)).markdown
