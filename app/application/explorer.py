"""Read side of the data explorer: pages of raw input tables and of a run's output documents."""

import asyncio
from pathlib import Path

from app.application.projections import Repositories
from app.application.services import NotFoundError
from app.domain.models import DocumentPage, Entity, OutputName, SourceFormat, TablePage
from app.domain.ports import DataBrowser

BROWSABLE_OUTPUTS = (OutputName.TRADES, OutputName.REJECTS, OutputName.EXPOSURES)


class ExplorerService:
    def __init__(self, repositories: Repositories, browser: DataBrowser, datasets_dir: Path) -> None:
        self._repos = repositories
        self._browser = browser
        self._datasets_dir = datasets_dir

    async def table(
        self, dataset_id: str, entity: Entity, offset: int, limit: int, search: str | None
    ) -> TablePage:
        """A page of one input table, exactly as the file holds it."""
        dataset = self._repos.datasets.get(dataset_id)
        if dataset is None:
            raise NotFoundError(f"dataset {dataset_id!r} not found")
        source = dataset.source_path(self._datasets_dir / dataset_id, entity, SourceFormat.PARQUET)
        if source is None or not source[0].exists():
            raise NotFoundError(f"dataset {dataset_id!r} has no {entity} table")
        return await asyncio.to_thread(self._browser.table_page, *source, offset, limit, search)

    async def output(
        self, run_id: str, name: OutputName, offset: int, limit: int, search: str | None
    ) -> DocumentPage:
        """A page of documents from one of a run's NDJSON outputs."""
        run = self._repos.runs.get(run_id)
        if run is None:
            raise NotFoundError(f"run {run_id!r} not found")
        if name not in BROWSABLE_OUTPUTS:
            raise ValueError(f"{name} can't be browsed page by page")
        path = run.outputs.get(name)
        if path is None or not Path(path).exists():
            raise NotFoundError(f"run {run_id!r} has no {name} file (yet, or it was deleted)")
        return await asyncio.to_thread(self._browser.document_page, Path(path), offset, limit, search)
