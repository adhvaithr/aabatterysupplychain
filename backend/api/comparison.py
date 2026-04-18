from __future__ import annotations

from fastapi import APIRouter, HTTPException

from schemas.events import ComparisonResponse
from services.workflow import WorkflowError, create_supabase_client, get_manual_vs_system_comparison


router = APIRouter(prefix="/comparison", tags=["comparison"])


@router.get("", response_model=ComparisonResponse)
def get_comparison() -> dict:
    try:
        client = create_supabase_client()
        return get_manual_vs_system_comparison(client)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
