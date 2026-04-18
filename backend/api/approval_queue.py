from __future__ import annotations

from fastapi import APIRouter, HTTPException

from schemas.events import ApprovalQueueItem
from services.workflow import WorkflowError, create_supabase_client, list_approval_queue


router = APIRouter(tags=["approval-queue"])


@router.get("/approval-queue", response_model=list[ApprovalQueueItem])
def get_approval_queue() -> list[dict]:
    try:
        client = create_supabase_client()
        return list_approval_queue(client)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
