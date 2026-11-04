"""Correction rules and validation switches. Every change is saved as a new version."""

from fastapi import APIRouter

from app.api.deps import ContainerDep
from app.api.schemas import RulesUpdate
from app.application.services import RulesOverview
from app.domain.models import RuleSet

router = APIRouter(prefix="/api/rules", tags=["rules"])


@router.get("")
async def get_rules(container: ContainerDep) -> RulesOverview:
    """The current rules, every reject rule with whether it can be switched off, and the columns."""
    return container.rules.overview()


@router.get("/history")
async def rules_history(container: ContainerDep) -> list[RuleSet]:
    """Every saved version, newest first."""
    return container.rules.history()


@router.put("")
async def update_rules(request: RulesUpdate, container: ContainerDep) -> RuleSet:
    """Replace the rules. Runs started from now on use the new version; older runs keep theirs."""
    return await container.rules.update(request.corrections, request.disabled_rules, request.note)
