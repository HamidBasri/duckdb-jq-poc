"""FastAPI application factory. Run with `uv run uvicorn app.main:app`."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.responses import FileResponse, JSONResponse

from app.api.routes import benchmarks, comparisons, datasets, events, tracking
from app.application.services import ConflictError, NotFoundError
from app.bootstrap import build_container
from app.config import get_settings

STATIC_DIR = Path(__file__).parent / "api" / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    container = build_container(get_settings())
    container.start()
    app.state.container = container
    try:
        yield
    finally:
        await container.stop()


def _error(status_code: int) -> object:
    async def handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status_code)

    return handler


def create_app() -> FastAPI:
    app = FastAPI(
        title="DuckDB vs jq - trade transformation POC",
        description="Event-driven pipeline that cleans, validates and maps tabular trade data to nested JSON "
        "with two interchangeable engines, and measures both.",
        version="0.1.0",
        lifespan=lifespan,
    )
    for router in (datasets.router, comparisons.router, benchmarks.router, events.router, tracking.router):
        app.include_router(router)
    app.add_exception_handler(NotFoundError, _error(status.HTTP_404_NOT_FOUND))  # type: ignore[arg-type]
    app.add_exception_handler(ConflictError, _error(status.HTTP_409_CONFLICT))  # type: ignore[arg-type]
    app.add_exception_handler(ValueError, _error(status.HTTP_422_UNPROCESSABLE_CONTENT))  # type: ignore[arg-type]

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
