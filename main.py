"""
Scheduler entry point.

Imports the orchestrator and all registered tasks from examples.py, attaches
a cron schedule to tiered_pipeline, then starts the background scheduler.

Usage:
    uv run python main.py
"""

import time

from examples import orch

orch.create_chain(
    "tiered_pipeline",
    ["fetch_data", "validate_data", "tier_router", "score_low", "score_mid", "score_high", "report"],
    description="Branch pipeline: routes records to a scoring tier then produces a report.",
    schedule="* * * * *",
)

if __name__ == "__main__":
    n = orch.start_scheduler()
    print(f"Scheduler started — {n} chain(s) registered. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        orch.stop_scheduler()
        print("Scheduler stopped.")
