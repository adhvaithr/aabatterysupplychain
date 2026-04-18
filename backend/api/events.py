from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from schemas.events import EventDetailResponse, EventSummary
from services.workflow import WorkflowError, create_supabase_client, get_event_detail, list_events


router = APIRouter(prefix="/events", tags=["events"])


@router.get("", response_model=list[EventSummary])
def get_events(
    dc: str | None = Query(default=None),
    min_risk_level: str | None = Query(default=None),
    state: str | None = Query(default=None),
) -> list[dict]:
    try:
        client = create_supabase_client()
        return list_events(client, dc=dc, min_risk_level=min_risk_level, state=state)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/{event_id}", response_model=EventDetailResponse)
def get_event(event_id: int) -> dict:
    try:
        client = create_supabase_client()
        return get_event_detail(client, event_id)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
