from __future__ import annotations

from fastapi import APIRouter, HTTPException

from schemas.events import AuditLogEntry
from services.workflow import WorkflowError, create_supabase_client, list_audit_entries


router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/{entity_id}", response_model=list[AuditLogEntry])
def get_audit(entity_id: int) -> list[dict]:
    try:
        client = create_supabase_client()
        return list_audit_entries(client, entity_id=entity_id)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
