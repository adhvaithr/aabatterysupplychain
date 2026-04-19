# POP Sentinel

POP Sentinel is a supply-chain monitoring app for surfacing inventory imbalance risk across `SF`, `NJ`, and `LA`, recommending transfer decisions, and tracking agent-driven scan activity.

The repo has:

- a `FastAPI` backend for scanning, scoring, approvals, audit history, and agent runs
- a `Next.js` frontend for the dashboard, inventory health, comparison views, approvals, and event detail pages
- a Supabase/Postgres schema in `schema.sql`

## What It Does

- Scans inventory and sales data to detect low-stock and cross-DC imbalance events
- Scores risk and penalty exposure for detected events
- Shows inventory health by SKU and DC
- Compares manual vs. system-assisted outcomes
- Supports approval workflows and audit history
- Runs an autonomous scheduler-backed agent with a live activity feed

## Project Layout

```text
.
├── backend/      FastAPI API, scanners, agents, orchestration
├── frontend/     Next.js app UI
└── schema.sql    Supabase/Postgres schema
```

## Prerequisites

- Python 3.11+ recommended
- Node.js 18+ and Yarn 1.x
- A Supabase project

## Environment Variables

Backend requires:

```bash
SUPABASE_URL=...
SUPABASE_KEY=...
```

Optional backend variables:

```bash
AGENT_SCAN_INTERVAL_HOURS=6
OPENROUTER_API_KEY=...
OPENROUTER_MODEL=...
OPENROUTER_HTTP_REFERER=http://localhost
OPENROUTER_APP_TITLE=POP Supply Chain
```

Frontend optional variable:

```bash
POP_API_BASE_URL=http://127.0.0.1:8000
```

If `POP_API_BASE_URL` is unset, the frontend proxies `/api/*` requests to `http://127.0.0.1:8000`.

## Database Setup

Apply `schema.sql` to your Supabase Postgres database before running the app.

## Run The Backend

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

For local development with auto-reload:

```bash
cd backend
source .venv/bin/activate
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

Backend health check:

```bash
curl http://127.0.0.1:8000/health
```

## Run The Frontend

```bash
cd frontend
yarn install
yarn dev
```

The frontend runs on `http://127.0.0.1:3000` and talks to the backend through `frontend/app/api/[[...path]]/route.js`.

## Agent Scheduler

The scheduler starts automatically when the backend starts.

- It is configured in `backend/services/agent_scheduler.py`
- It runs immediately on startup
- It then repeats on the `AGENT_SCAN_INTERVAL_HOURS` cadence
- Overlap protection is handled in the backend agent runner

## Main Routes

Frontend pages:

- `/` dashboard
- `/inventory` inventory health
- `/comparison` manual vs. system-assisted comparison
- `/approvals` approval queue
- `/events/[id]` event detail

Backend API highlights:

- `GET /health`
- `POST /scan`
- `GET /inventory-health`
- `GET /comparison`
- `POST /agent/run`
- `GET /agent/runs/latest`
- `GET /agent/activity`

## Notes

- The backend allows CORS from `http://localhost:3000`
- AI event analysis depends on the OpenRouter env vars being set
- The scheduler and manual agent runs both write run/activity data for the dashboard feed
