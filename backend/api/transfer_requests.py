from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from schemas.events import (
    TransferRequestCreateRequest,
    TransferRequestRejectRequest,
    TransferRequestResponse,
)
from services.workflow import (
    WorkflowError,
    actor_from_headers,
    approve_transfer_request,
    create_supabase_client,
    create_transfer_request,
    reject_transfer_request,
)


router = APIRouter(prefix="/transfer-requests", tags=["transfer-requests"])


@router.post("", response_model=TransferRequestResponse)
def post_transfer_request(payload: TransferRequestCreateRequest, request: Request) -> dict:
    try:
        client = create_supabase_client()
        actor = actor_from_headers(request.headers)
        return create_transfer_request(client, actor=actor, **payload.model_dump())
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/{request_id}/approve", response_model=TransferRequestResponse)
def post_transfer_request_approve(request_id: int, request: Request) -> dict:
    try:
        client = create_supabase_client()
        actor = actor_from_headers(request.headers)
        return approve_transfer_request(client, request_id=request_id, actor=actor)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/{request_id}/reject", response_model=TransferRequestResponse)
def post_transfer_request_reject(
    request_id: int,
    payload: TransferRequestRejectRequest,
    request: Request,
) -> dict:
    try:
        client = create_supabase_client()
        actor = actor_from_headers(request.headers)
        return reject_transfer_request(client, request_id=request_id, actor=actor, reason=payload.reason)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
