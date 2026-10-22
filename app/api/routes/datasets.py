"""Service 1: dataset ingestion - synthetic generation and CSV/Parquet upload."""

from typing import Annotated, BinaryIO

from fastapi import APIRouter, File, UploadFile, status

from app.api.deps import ContainerDep
from app.api.schemas import DatasetProfile, GenerateRequest
from app.domain.models import DatasetManifest, Entity, GenerationSpec

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


@router.get("")
async def list_datasets(container: ContainerDep) -> list[DatasetManifest]:
    return container.datasets.list_all()


@router.get("/{dataset_id}")
async def get_dataset(dataset_id: str, container: ContainerDep) -> DatasetManifest:
    return container.datasets.get(dataset_id)


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
