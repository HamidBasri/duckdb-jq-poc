from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.api.deps import ContainerDep

router = APIRouter(prefix="/api", tags=["events"])


@router.get("/events")
async def stream_events(container: ContainerDep) -> StreamingResponse:
    """Server-Sent Events: every domain event (run progress, stage timings, completions) live."""
    return StreamingResponse(
        container.broadcaster.stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
