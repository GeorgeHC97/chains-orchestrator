"""
Example pipelines for the local orchestrator.

Seven chains are demonstrated:
  1. full_pipeline      — fetch → validate → transform → save  (succeeds)
  2. quick_pipeline     — fetch → save                          (succeeds)
  3. bad_pipeline       — fetch → crash → save                  (fails mid-chain)
  4. tiered_pipeline    — fetch → validate → branch → report   (branch selects tier)
  5. timeout_pipeline   — fast → slow (times out) → after      (fails mid-chain due to timeout)
  6. retry_pipeline     — flaky (fails twice, succeeds third)  → save  (succeeds after retries)
  7. exhausted_pipeline — always_fails (retries=2) → save      (all retries exhausted)
"""

import json
import time

from orchestrator import Orchestrator

orch = Orchestrator("orchestrator.db")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

@orch.task("fetch_data", description="Simulate fetching records from an external source.")
def fetch_data(ctx: dict) -> dict:
    source = ctx.get("source", "default")
    print(f"  [fetch_data] fetching from '{source}'...")
    time.sleep(2)  # simulate latency
    return {
        "records": [
            {"id": 1, "name": "Alice", "score": 82},
            {"id": 2, "name": "Bob",   "score": 96},
            {"id": 3, "name": "Carol", "score": 91},
        ],
        "source": source,
    }


@orch.task("validate_data", description="Ensure all records have required fields and valid scores.")
def validate_data(ctx: dict) -> dict:
    records = ctx.get("records", [])
    invalid = [r for r in records if not (0 <= r.get("score", -1) <= 100)]
    if invalid:
        raise ValueError(f"Invalid records found: {invalid}")
    print(f"  [validate_data] {len(records)} records validated.")
    return {"validated": True, "record_count": len(records)}


@orch.task("transform_data", description="Enrich records with a pass/fail grade.")
def transform_data(ctx: dict) -> dict:
    records = ctx.get("records", [])
    enriched = [
        {**r, "grade": "pass" if r["score"] >= 60 else "fail"}
        for r in records
    ]
    print(f"  [transform_data] enriched {len(enriched)} records.")
    return {"enriched_records": enriched}


@orch.task("save_data", description="Persist records to a local JSON file.")
def save_data(ctx: dict) -> dict:
    records = ctx.get("enriched_records") or ctx.get("records", [])
    path = "output.json"
    with open(path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"  [save_data] wrote {len(records)} records to {path}.")
    return {"saved_to": path, "record_count": len(records)}


@orch.task("crash_task", description="Always raises an error — used to test failure handling.")
def crash_task(_ctx: dict) -> dict:
    raise RuntimeError("Simulated task failure!")


@orch.task("fast_step", description="Completes quickly — used in the timeout demo.", timeout=5.0)
def fast_step(ctx: dict) -> dict:
    print("  [fast_step] done.")
    return {"fast_done": True}


@orch.task("slow_step", description="Sleeps 3 s but has a 1 s timeout — will always time out.", timeout=1.0)
def slow_step(ctx: dict) -> dict:
    print("  [slow_step] starting (will time out)...")
    time.sleep(3)
    return {"slow_done": True}


@orch.task("after_step", description="Runs after the slow step — skipped when slow_step times out.")
def after_step(ctx: dict) -> dict:
    print("  [after_step] done.")
    return {"after_done": True}


_flaky_call_count = [0]

@orch.task("flaky_task", description="Fails on the first two attempts, succeeds on the third.", retries=2)
def flaky_task(ctx: dict) -> dict:
    _flaky_call_count[0] += 1
    attempt = _flaky_call_count[0]
    if attempt <= 2:
        print(f"  [flaky_task] attempt {attempt} failed.")
        raise RuntimeError(f"Transient error (attempt {attempt})")
    print(f"  [flaky_task] attempt {attempt} succeeded.")
    return {"recovered": True}


@orch.task("always_fails_task", description="Always fails — used to demonstrate exhausted retries.", retries=2)
def always_fails_task(ctx: dict) -> dict:
    raise RuntimeError("Persistent failure!")


@orch.branch("tier_router", options=["score_low", "score_mid", "score_high"],
             description="Route to a scoring tier based on average record score.")
def tier_router(ctx: dict) -> str:
    records = ctx.get("records", [])
    avg = sum(r["score"] for r in records) / len(records) if records else 0
    if avg < 60:
        return "score_low"
    if avg < 80:
        return "score_mid"
    return "score_high"


@orch.task("score_low", description="Handle low-scoring cohort (avg < 60).")
def score_low(ctx: dict) -> dict:
    records = ctx.get("records", [])
    print(f"  [score_low] {len(records)} records in low tier.")
    return {"tier": "low", "record_count": len(records)}


@orch.task("score_mid", description="Handle mid-scoring cohort (60 ≤ avg < 80).")
def score_mid(ctx: dict) -> dict:
    records = ctx.get("records", [])
    print(f"  [score_mid] {len(records)} records in mid tier.")
    return {"tier": "mid", "record_count": len(records)}


@orch.task("score_high", description="Handle high-scoring cohort (avg ≥ 80).")
def score_high(ctx: dict) -> dict:
    records = ctx.get("records", [])
    print(f"  [score_high] {len(records)} records in high tier.")
    return {"tier": "high", "record_count": len(records)}


@orch.task("report", description="Summarise the run and emit a final report.")
def report(ctx: dict) -> dict:
    tier = ctx.get("tier", "unknown")
    count = ctx.get("record_count", 0)
    print(f"  [report] tier={tier}, count={count}")
    return {"report": f"Tier {tier}: {count} records processed."}


# ---------------------------------------------------------------------------
# Chain definitions
# ---------------------------------------------------------------------------

orch.create_chain(
    "full_pipeline",
    ["fetch_data", "validate_data", "transform_data", "save_data"],
    description="Full ETL pipeline: fetch, validate, transform, save.",
)

orch.create_chain(
    "quick_pipeline",
    ["fetch_data", "save_data"],
    description="Lightweight pipeline: fetch then save directly.",
)

orch.create_chain(
    "bad_pipeline",
    ["fetch_data", "crash_task", "save_data"],
    description="Pipeline that fails mid-chain to demonstrate error handling.",
)

orch.create_chain(
    "tiered_pipeline",
    ["fetch_data", "validate_data", "tier_router", "score_low", "score_mid", "score_high", "report"],
    description="Branch pipeline: routes records to a scoring tier then produces a report.",
)

orch.create_chain(
    "timeout_pipeline",
    ["fast_step", "slow_step", "after_step"],
    description="Timeout demo: fast_step succeeds, slow_step times out, after_step is skipped.",
)

orch.create_chain(
    "retry_pipeline",
    ["flaky_task", "save_data"],
    description="Retry demo: flaky_task fails twice then succeeds on the third attempt.",
)

orch.create_chain(
    "exhausted_pipeline",
    ["always_fails_task", "save_data"],
    description="Retry demo: always_fails_task exhausts all retries, save_data is skipped.",
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def print_summary(run) -> None:
    s = orch.summary(run)
    status_icon = "✓" if s["status"] == "success" else "✗"
    print(f"\n  {status_icon} Run {s['run_id'][:8]}... | chain={s['chain_name']} | "
          f"status={s['status']} | duration={s['duration_s']:.3f}s")
    for step in s["steps"]:
        icon = {"success": "✓", "failed": "✗", "skipped": "—"}.get(step["status"], "?")
        extra = f" → {step['output_keys']}" if step["output_keys"] else ""
        err = f"\n      error: {step['error'].strip().splitlines()[-1]}" if step["error"] else ""
        print(f"    {icon} {step['task']:<20} [{step['status']}]{extra}{err}")


# ---------------------------------------------------------------------------
# Run all four chains
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("Running full_pipeline (expect: success)")
    print("=" * 60)
    run1 = orch.run_chain("full_pipeline", initial_input={"source": "api/v1/students"})
    print_summary(run1)

    print("\n" + "=" * 60)
    print("Running quick_pipeline (expect: success)")
    print("=" * 60)
    run2 = orch.run_chain("quick_pipeline", initial_input={"source": "cache"})
    print_summary(run2)

    print("\n" + "=" * 60)
    print("Running bad_pipeline (expect: fail at crash_task, save_data skipped)")
    print("=" * 60)
    run3 = orch.run_chain("bad_pipeline", initial_input={"source": "api/v2/students"})
    print_summary(run3)

    print("\n" + "=" * 60)
    print("Run history for bad_pipeline (from SQLite):")
    print("=" * 60)
    chain = orch.db.get_chain_by_name("bad_pipeline")
    runs = orch.db.get_runs_for_chain(chain.id)
    for r in runs:
        print(f"  run_id={r.id[:8]}... status={r.status.value} started={r.started_at}")

    print("\n" + "=" * 60)
    print("Running tiered_pipeline (expect: score_high green, others skipped, report green)")
    print("=" * 60)
    run4 = orch.run_chain("tiered_pipeline", initial_input={"source": "api/v1/students"})
    print_summary(run4)

    print("\n" + "=" * 60)
    print("Running timeout_pipeline (expect: fast_step ok, slow_step times out, after_step skipped)")
    print("=" * 60)
    run5 = orch.run_chain("timeout_pipeline")
    print_summary(run5)

    print("\n" + "=" * 60)
    print("Running retry_pipeline (expect: flaky_task fails twice then succeeds, save_data runs)")
    print("=" * 60)
    run6 = orch.run_chain("retry_pipeline")
    print_summary(run6)

    print("\n" + "=" * 60)
    print("Running exhausted_pipeline (expect: always_fails_task exhausts retries, save_data skipped)")
    print("=" * 60)
    run7 = orch.run_chain("exhausted_pipeline")
    print_summary(run7)
