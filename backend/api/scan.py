from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import JSONResponse

from schemas.events import ScanParams, ScanResult
from services.scanner import run_scan
from services.workflow import WorkflowError, actor_from_headers, require_supabase_config, create_supabase_client

router = APIRouter(prefix="/scan", tags=["scan"])


@router.post("", response_model=ScanResult)
def post_scan(
    request: Request,
    params: ScanParams = Body(default_factory=ScanParams),
) -> JSONResponse:
    require_supabase_config()
    client = create_supabase_client()
    actor = actor_from_headers(request.headers)
    try:
        result = run_scan(client, actor=actor, params=params)
        return JSONResponse(content=result)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
