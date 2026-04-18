from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from schemas.events import InventoryHealthResponse
from services.workflow import WorkflowError, create_supabase_client, list_inventory_health


router = APIRouter(prefix="/inventory-health", tags=["inventory"])


@router.get("", response_model=InventoryHealthResponse)
def get_inventory_health(
    demand_window_days: int = Query(default=30, ge=1, le=180),
) -> dict:
    try:
        client = create_supabase_client()
        return list_inventory_health(client, demand_window_days=demand_window_days)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
