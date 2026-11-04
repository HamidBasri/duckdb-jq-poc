"""Service 1: dataset ingestion - synthetic generation and CSV/Parquet upload."""

from typing import Annotated, BinaryIO

from fastapi import APIRouter, File, Query, UploadFile, status

from app.api.deps import ContainerDep
from app.api.schemas import DatasetProfile, EditRequest, GenerateRequest, VersionRequest
from app.domain.models import DatasetManifest, Entity, EntityColumns, GenerationSpec, TablePage

MAX_PAGE_ROWS = 500

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


@router.get("")
async def list_datasets(container: ContainerDep) -> list[DatasetManifest]:
    return container.datasets.list_all()


@router.get("/{dataset_id}")
async def get_dataset(dataset_id: str, container: ContainerDep) -> DatasetManifest:
    return container.datasets.get(dataset_id)


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(dataset_id: str, container: ContainerDep) -> None:
    """Remove a dataset and its files (refused while a run is using it)."""
    await container.datasets.delete(dataset_id)


@router.get("/{dataset_id}/columns")
async def dataset_columns(dataset_id: str, container: ContainerDep) -> list[EntityColumns]:
    """Each file's columns next to the expected ones, with suggested matches and sample values."""
    return await container.datasets.columns(dataset_id)


@router.post("/{dataset_id}/versions", status_code=status.HTTP_202_ACCEPTED)
async def create_version(
    dataset_id: str, request: VersionRequest, container: ContainerDep
) -> DatasetManifest:
    """Copy the dataset with its columns renamed to the expected ones. The original is kept."""
    return await container.datasets.request_version(dataset_id, request.mapping)


@router.get("/{dataset_id}/tables/{entity}")
async def dataset_table(
    dataset_id: str,
    entity: Entity,
    container: ContainerDep,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=MAX_PAGE_ROWS),
    q: str | None = Query(None, description="Row number, or text found in any field"),
) -> TablePage:
    """A page of one input table, every value as text, exactly as the file holds it."""
    return await container.explorer.table(dataset_id, entity, offset, limit, q)


@router.post("/{dataset_id}/edits", status_code=status.HTTP_202_ACCEPTED)
async def edit_dataset(dataset_id: str, request: EditRequest, container: ContainerDep) -> DatasetManifest:
    """Save fields changed and rows removed by hand as a new version. The original is kept."""
    return await container.datasets.request_edits(dataset_id, request.edits, request.deleted_rows)


@router.post("/generate", status_code=status.HTTP_202_ACCEPTED)
async def generate_dataset(request: GenerateRequest, container: ContainerDep) -> DatasetManifest:
    """Queue generation of one synthetic dataset. Same parameters => same dataset id and bytes."""
    spec = request.model_dump(exclude={"overwrite"})
    return await container.datasets.request_generation(GenerationSpec.model_validate(spec), request.overwrite)


@router.post("/presets", status_code=status.HTTP_202_ACCEPTED)
async def generate_presets(profile: DatasetProfile, container: ContainerDep) -> list[DatasetManifest]:
    """Queue the standard sizes (5k, 50k, 500k, 1M, 5M by default)."""
    return [await container.datasets.request_generation(spec) for spec in profile.specs()]


@router.post("/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload_dataset(
    container: ContainerDep,
    trades: Annotated[UploadFile, File()],
    counterparties: Annotated[UploadFile | None, File()] = None,
    instruments: Annotated[UploadFile | None, File()] = None,
    books: Annotated[UploadFile | None, File()] = None,
    fx_rates: Annotated[UploadFile | None, File()] = None,
) -> DatasetManifest:
    """Upload CSV or Parquet files (one per entity). Only trades are required; missing
    reference entities simply leave the corresponding references unresolved."""
    files: dict[Entity, UploadFile | None] = {
        Entity.TRADES: trades,
        Entity.COUNTERPARTIES: counterparties,
        Entity.INSTRUMENTS: instruments,
        Entity.BOOKS: books,
        Entity.FX_RATES: fx_rates,
    }
    uploads: dict[Entity, tuple[str, BinaryIO]] = {
        entity: (upload.filename or f"{entity}.csv", upload.file)
        for entity, upload in files.items()
        if upload is not None
    }
    return await container.datasets.register_upload(uploads)
