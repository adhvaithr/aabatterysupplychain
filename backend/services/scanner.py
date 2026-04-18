"""
POST /scan orchestration: runs all four agents in sequence, upserts events,
then immediately runs Claude analysis per event via analyze_event().
"""

from __future__ import annotations

from typing import Any

from supabase import Client

from schemas.events import ScanParams
from services.agents.demand_agent import DemandAgent, DemandAgentConfig
from services.agents.imbalance_agent import ImbalanceAgent, ImbalanceAgentConfig
from services.agents.penalty_agent import PenaltyAgent, PenaltyAgentConfig
from services.orchestrator import analyze_event


def run_scan(
    client: Client,
    *,
    actor: str = "system",
    params: ScanParams | None = None,
) -> dict[str, Any]:
    p = params or ScanParams()
    print(
        "scanner: starting scan "
        f"actor={actor} max_dos={p.max_dos} demand_window_days={p.demand_window_days} horizon_days={p.horizon_days}"
    )

    # 1. Demand pass — explicit call so demand hits are visible as a named step.
    demand_agent = DemandAgent(
        client,
        config=DemandAgentConfig(
            demand_window_days=p.demand_window_days,
            max_days_of_supply=p.max_dos,
            horizon_days=p.horizon_days,
        ),
    )
    demand_hits_df = demand_agent.build_events()
    print(f"scanner: demand stage produced {len(demand_hits_df)} low-stock hits.")

    # 2. Imbalance + Supply pass — supply agent (evaluate_supply_for_event) is called
    #    per hit inside ImbalanceAgent.build_events() as a per-candidate gate.
    imbalance_agent = ImbalanceAgent(
        client,
        config=ImbalanceAgentConfig(
            demand_window_days=p.demand_window_days,
            max_days_of_supply=p.max_dos,
        ),
    )
    events_df = imbalance_agent.build_events(demand_hits_df=demand_hits_df)
    print(f"scanner: imbalance stage produced {len(events_df)} confirmed events.")

    if events_df.empty:
        print("scanner: no confirmed events to persist or analyze.")
        return {
            "events_scanned": 0,
            "events_analyzed": 0,
            "analysis_failures": 0,
            "event_ids": [],
            "failed_event_ids": [],
            "actor": actor,
        }

    # Upsert events with state DETECTED (on_conflict="event_key" refreshes fields).
    imbalance_agent.persist_events(events_df)

    # 3. Re-fetch the upserted event ids for downstream steps.
    event_keys: list[str] = events_df["event_key"].dropna().tolist()
    event_ids = _fetch_event_ids_by_keys(client, event_keys)
    print(f"scanner: fetched {len(event_ids)} event ids after upsert: {event_ids}")

    # 4. Penalty pass — scores expected_penalty_cost onto every event in the table.
    penalty_agent = PenaltyAgent(client, config=PenaltyAgentConfig())
    payload_df = penalty_agent.build_event_penalty_payloads(event_ids=event_ids)
    print(f"scanner: penalty stage produced {len(payload_df)} payload rows.")
    if not payload_df.empty:
        penalty_agent.persist_expected_penalty_costs(payload_df)

    # 5. Orchestrator pass — analyze each event; AI failures are soft.
    analyzed: list[int] = []
    failed: list[int] = []
    for event_id in event_ids:
        print(f"scanner: analyzing event_id={event_id}")
        result = analyze_event(event_id, client=client, actor=actor)
        if result.get("ok"):
            analyzed.append(event_id)
            print(
                "scanner: analyze_event succeeded "
                f"event_id={event_id} action={result.get('recommended_action')} "
                f"confidence={result.get('confidence')}"
            )
        else:
            failed.append(event_id)
            print(
                "scanner: analyze_event failed "
                f"event_id={event_id} error={result.get('error')} detail={result.get('detail')}"
            )

    print(
        "scanner: completed scan "
        f"events_scanned={len(event_ids)} events_analyzed={len(analyzed)} analysis_failures={len(failed)}"
    )

    return {
        "events_scanned": len(event_ids),
        "events_analyzed": len(analyzed),
        "analysis_failures": len(failed),
        "event_ids": event_ids,
        "failed_event_ids": failed,
        "actor": actor,
    }


def _fetch_event_ids_by_keys(client: Client, event_keys: list[str]) -> list[int]:
    if not event_keys:
        return []
    # Supabase PostgREST: use `.in_("event_key", [...])` for batch lookup.
    response = (
        client.table("events")
        .select("id")
        .in_("event_key", event_keys)
        .execute()
    )
    rows = response.data or []
    return [int(r["id"]) for r in rows if r.get("id") is not None]
