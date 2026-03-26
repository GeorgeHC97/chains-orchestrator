from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from ..db import Database
from ..models import Run

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app = FastAPI(title="Orchestrator UI")


def _get_db() -> Database:
    return Database("orchestrator.db")


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _next_run_at(schedule: str | None) -> str | None:
    """Return a human-readable next fire time for a cron expression, or None."""
    if not schedule:
        return None
    try:
        from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
        trigger = CronTrigger.from_crontab(schedule)
        next_dt = trigger.get_next_fire_time(None, datetime.now(timezone.utc))
        if next_dt is None:
            return None
        return next_dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return None


def _build_mermaid(steps: list[dict]) -> str:
    """Build a Mermaid flowchart LR string from enriched step dicts."""
    lines = ["flowchart LR"]

    class_defs = [
        "  classDef success fill:#22c55e,stroke:#16a34a,color:#fff",
        "  classDef failed  fill:#ef4444,stroke:#dc2626,color:#fff",
        "  classDef skipped fill:#9ca3af,stroke:#6b7280,color:#fff",
        "  classDef pending fill:#fbbf24,stroke:#d97706,color:#000",
        "  classDef running fill:#3b82f6,stroke:#2563eb,color:#fff",
    ]

    # Pre-pass: collect all option names from branch tasks
    option_set: set[str] = set()
    for step in steps:
        if step.get("task_type") == "branch":
            option_set.update(step.get("branch_options", []))

    if not option_set:
        # Backward compatible: linear flow identical to original output
        node_ids = []
        for step in steps:
            safe_name = step["name"].replace("-", "_")
            node_ids.append(f"{safe_name}:::{step['status']}")
        if len(node_ids) == 1:
            lines.append(f"  {node_ids[0]}")
        else:
            lines.append("  " + " --> ".join(node_ids))
        lines.extend(class_defs)
        return "\n".join(lines)

    # Branching mode: declare all nodes first, then emit edges
    for step in steps:
        safe_name = step["name"].replace("-", "_")
        if step.get("task_type") == "branch":
            lines.append(f"  {safe_name}{{{step['name']}}}:::{step['status']}")
        else:
            lines.append(f"  {safe_name}[{step['name']}]:::{step['status']}")

    already_connected: set[str] = set()
    prev_safe: str | None = None
    i = 0
    while i < len(steps):
        step = steps[i]
        safe = step["name"].replace("-", "_")

        if step.get("task_type") == "branch":
            if prev_safe:
                lines.append(f"  {prev_safe} --> {safe}")
            branch_opts = step.get("branch_options", [])
            for opt in branch_opts:
                lines.append(f"  {safe} --> {opt.replace('-', '_')}")
            # Find first step after all options (convergence point)
            j = i + 1
            while j < len(steps) and steps[j]["name"] in option_set:
                j += 1
            if j < len(steps):
                conv_safe = steps[j]["name"].replace("-", "_")
                for opt in branch_opts:
                    lines.append(f"  {opt.replace('-', '_')} --> {conv_safe}")
                already_connected.add(conv_safe)
            prev_safe = safe
            i += 1
        elif step["name"] in option_set:
            i += 1  # already handled by the branch block above
        else:
            if prev_safe and safe not in already_connected:
                lines.append(f"  {prev_safe} --> {safe}")
            prev_safe = safe
            i += 1

    lines.extend(class_defs)
    return "\n".join(lines)


def _build_timeline(steps: list[dict], run: Run) -> list[dict]:
    """Build timeline bar data for each step."""
    now = datetime.now(timezone.utc)
    run_start = run.started_at
    run_end = run.completed_at or now

    if run_start is None:
        return []

    # Make run_start timezone-aware if it isn't
    if run_start.tzinfo is None:
        run_start = run_start.replace(tzinfo=timezone.utc)
    if run_end.tzinfo is None:
        run_end = run_end.replace(tzinfo=timezone.utc)

    total_s = (run_end - run_start).total_seconds()
    if total_s <= 0:
        total_s = 1.0

    timeline = []
    for step in steps:
        tr_start = step.get("started_at")
        tr_end = step.get("completed_at")
        duration_s = step.get("duration_s")

        if tr_start is not None:
            if tr_start.tzinfo is None:
                tr_start = tr_start.replace(tzinfo=timezone.utc)
            offset_pct = (tr_start - run_start).total_seconds() / total_s * 100
            if tr_end is not None:
                if tr_end.tzinfo is None:
                    tr_end = tr_end.replace(tzinfo=timezone.utc)
                width_pct = (tr_end - tr_start).total_seconds() / total_s * 100
            else:
                width_pct = (now - tr_start).total_seconds() / total_s * 100
        else:
            offset_pct = 0.0
            width_pct = 0.0

        width_pct = max(width_pct, 0.5) if tr_start is not None else 0.0

        timeline.append({
            "name": step["name"],
            "status": step["status"],
            "offset_pct": round(offset_pct, 2),
            "width_pct": round(width_pct, 2),
            "duration_s": duration_s,
            "has_bar": tr_start is not None,
        })

    return timeline


def enrich_run(db: Database, run: Run) -> dict:
    """Return everything templates need for a run detail page."""
    task_map = {t.id: t for t in db.get_tasks_for_chain(run.chain_id)}
    task_runs = db.get_task_runs_for_run(run.id)
    chain = db.get_chain_by_id(run.chain_id)

    duration_s = None
    if run.started_at and run.completed_at:
        start = run.started_at
        end = run.completed_at
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        duration_s = (end - start).total_seconds()

    steps = []
    for tr in task_runs:
        task = task_map.get(tr.task_id)
        task_name = task.name if task else tr.task_id

        step_duration = None
        if tr.started_at and tr.completed_at:
            s = tr.started_at
            e = tr.completed_at
            if s.tzinfo is None:
                s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None:
                e = e.replace(tzinfo=timezone.utc)
            step_duration = (e - s).total_seconds()

        steps.append({
            "name": task_name,
            "status": tr.status.value,
            "started_at": tr.started_at,
            "completed_at": tr.completed_at,
            "duration_s": step_duration,
            "error": tr.error,
            "input_keys": list(tr.input.keys()),
            "output_keys": list(tr.output.keys()),
            "task_type": task.task_type.value if task else "task",
            "branch_options": task.branch_options if task else [],
            "retries": task.retries if task else 0,
        })

    mermaid_src = _build_mermaid(steps)
    timeline = _build_timeline(steps, run)

    return {
        "run": run,
        "chain": chain,
        "duration_s": duration_s,
        "steps": steps,
        "mermaid": mermaid_src,
        "timeline": timeline,
    }


def _run_to_dict(run: Run, steps: list[dict], timeline: list[dict]) -> dict:
    """Serialise run data for the JSON polling endpoint."""

    def _fmt(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    return {
        "id": run.id,
        "chain_id": run.chain_id,
        "status": run.status.value,
        "started_at": _fmt(run.started_at),
        "completed_at": _fmt(run.completed_at),
        "error": run.error,
        "steps": [
            {
                "name": s["name"],
                "status": s["status"],
                "duration_s": s["duration_s"],
                "error": s["error"],
                "task_type": s["task_type"],
                "branch_options": s["branch_options"],
                "retries": s["retries"],
            }
            for s in steps
        ],
        "timeline": timeline,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    db = _get_db()
    chains = db.get_all_chains()
    chain_data = []
    for chain in chains:
        runs = db.get_runs_for_chain(chain.id)
        last_run = runs[0] if runs else None
        chain_data.append({
            "chain": chain,
            "run_count": len(runs),
            "last_run": last_run,
            "next_run_at": _next_run_at(chain.schedule),
        })
    return templates.TemplateResponse(request, "index.html", {"chain_data": chain_data})


@app.get("/chains/{chain_name}", response_class=HTMLResponse)
async def chain_detail(request: Request, chain_name: str):
    db = _get_db()
    chain = db.get_chain_by_name(chain_name)
    if chain is None:
        raise HTTPException(status_code=404, detail="Chain not found")
    runs = db.get_runs_for_chain(chain.id)

    run_rows = []
    for run in runs:
        duration_s = None
        if run.started_at and run.completed_at:
            s = run.started_at
            e = run.completed_at
            if s.tzinfo is None:
                s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None:
                e = e.replace(tzinfo=timezone.utc)
            duration_s = (e - s).total_seconds()
        run_rows.append({"run": run, "duration_s": duration_s})

    return templates.TemplateResponse(request, "chain.html", {
        "chain": chain,
        "run_rows": run_rows,
        "next_run_at": _next_run_at(chain.schedule),
    })


@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(request: Request, run_id: str):
    db = _get_db()
    run = db.get_run_by_id(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    data = enrich_run(db, run)
    return templates.TemplateResponse(request, "run.html", data)


@app.get("/api/runs/{run_id}")
async def api_run(run_id: str):
    db = _get_db()
    run = db.get_run_by_id(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    data = enrich_run(db, run)
    return _run_to_dict(run, data["steps"], data["timeline"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    uvicorn.run("orchestrator.web.app:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()
