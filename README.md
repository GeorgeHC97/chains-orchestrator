# Chains Orchestrator

A local task orchestrator backed by SQLite. Define tasks as plain Python functions, chain them together, run them, and query the full execution history from a local database.

## Features

- Register tasks as decorated Python functions
- Chain tasks in order, with each task receiving the merged output of all previous steps as context
- **Conditional branching** — route to one of several tasks based on runtime context; unchosen options are automatically skipped and execution reconverges after the branch
- **Per-task timeouts** — configurable per task (default 30 s); timed-out tasks are marked `FAILED` and stored like any other failure
- **Automatic retries** — configurable number of retry attempts per task (default 0); 1-second delay between attempts; tasks that exhaust all retries are marked `FAILED`
- **Concurrency control** — per-chain `max_concurrent` limit; excess calls raise `ConcurrencyError`; the scheduler drops overlapping runs silently
- **Cron scheduling** — attach a 5-field cron expression to any chain; a background scheduler fires `run_chain` automatically at each tick
- Full run history stored in SQLite — status, timing, inputs, outputs, and errors per task
- Failed tasks cause remaining tasks to be skipped (configurable)
- Simple summary view of any run
- Local web UI with pipeline diagrams, timeline visualisations, and schedule display

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv sync
```

## Usage

### 1. Define tasks

Tasks are plain functions that accept a `dict` (the current context) and return a `dict` (their output, which is merged into the context for the next task).

```python
from orchestrator import Orchestrator

orch = Orchestrator("orchestrator.db")

@orch.task("fetch_data", description="Fetch records from the API.")
def fetch_data(ctx: dict) -> dict:
    return {"records": [...]}

@orch.task("transform_data", description="Enrich records with a grade.")
def transform_data(ctx: dict) -> dict:
    records = ctx["records"]  # output of fetch_data is available here
    enriched = [{**r, "grade": "pass" if r["score"] >= 60 else "fail"} for r in records]
    return {"enriched_records": enriched}
```

### 2. Define a branch (optional)

A branch task inspects the current context and returns the **name of the next task to run**. All other options in the branch are automatically marked `SKIPPED`. Execution reconverges on the first task listed after the options.

```python
@orch.branch("tier_router", options=["score_low", "score_mid", "score_high"])
def tier_router(ctx: dict) -> str:
    avg = sum(r["score"] for r in ctx["records"]) / len(ctx["records"])
    if avg < 60:
        return "score_low"
    if avg < 80:
        return "score_mid"
    return "score_high"

@orch.task("score_low")
def score_low(ctx: dict) -> dict: ...

@orch.task("score_mid")
def score_mid(ctx: dict) -> dict: ...

@orch.task("score_high")
def score_high(ctx: dict) -> dict: ...

@orch.task("report")
def report(ctx: dict) -> dict: ...
```

Rules:
- The branch function must return a `str` that is one of the declared `options`; any other return value fails the run.
- The branch's own output (`{"selected": "<chosen>"}`) is **not** merged into the shared context — it is routing metadata only.
- The tasks listed after the options in the chain definition act as the convergence point and always run (unless a prior failure skips them).

### 3. Create a chain

```python
orch.create_chain(
    "my_pipeline",
    ["fetch_data", "transform_data"],
    description="Fetch and enrich records.",
)
```

For a branching chain, list the branch task followed by all its options, then the convergence task:

```python
orch.create_chain(
    "tiered_pipeline",
    ["fetch_data", "tier_router", "score_low", "score_mid", "score_high", "report"],
)
```

Chains are stored in SQLite and are idempotent — calling `create_chain` with the same name again is a no-op.

### 4. Run a chain

```python
run = orch.run_chain("my_pipeline", initial_input={"source": "api/v1"})
```

The `initial_input` dict is passed as the starting context for the first task.

### 5. Inspect results

```python
print(orch.summary(run))
```

```python
# Query run history directly from SQLite
chain = orch.db.get_chain_by_name("my_pipeline")
runs  = orch.db.get_runs_for_chain(chain.id)

for r in runs:
    print(r.status, r.started_at, r.completed_at)
```

## Timeouts

Every task has a timeout. If it does not complete within the allotted time it is marked `FAILED` with a clear error message and the rest of the chain is skipped as normal.

### Default timeout

The orchestrator-level default is **30 seconds**. Override it for the whole process at construction time:

```python
orch = Orchestrator("orchestrator.db", default_timeout=60.0)
```

### Per-task timeout

Pass `timeout=` to any decorator to override the default for that task:

```python
@orch.task("fetch_data", timeout=10.0)
def fetch_data(ctx: dict) -> dict: ...

@orch.branch("tier_router", options=["score_low", "score_mid", "score_high"], timeout=5.0)
def tier_router(ctx: dict) -> str: ...

# register() also accepts timeout
orch.register("quick_step", fn, timeout=2.0)
```

The timeout value is stored in the database alongside the task so it is visible in run history.

### How it works

Each task runs in a background thread via `concurrent.futures.ThreadPoolExecutor`. If `future.result(timeout=N)` raises `TimeoutError` the task run is recorded as `FAILED` with the message `Task '<name>' timed out after Ns.` The timed-out thread is abandoned and execution moves on (or stops, depending on `stop_on_failure`).

> **Note:** Python threads cannot be forcibly killed. A timed-out task thread will continue running in the background until it naturally returns or the process exits. For most I/O-bound work (HTTP requests, file operations) the OS will clean these up promptly.

## Retries

Tasks can be configured to retry automatically on failure, with a 1-second delay between attempts.

### Configure retries

Pass `retries=N` to any decorator or `register()` to allow up to N retries after the first failure:

```python
@orch.task("fetch_data", retries=3)
def fetch_data(ctx: dict) -> dict:
    # will be attempted up to 4 times total (1 + 3 retries)
    ...

@orch.branch("tier_router", options=["low", "high"], retries=1)
def tier_router(ctx: dict) -> str: ...

# register() also accepts retries
orch.register("quick_step", fn, retries=2)
```

The default is `retries=0` — tasks fail immediately with no retry.

### Behaviour

- Each retry waits 1 second before the next attempt.
- The per-task `timeout` applies independently to each attempt.
- If all attempts fail, the task run is marked `FAILED`. When `retries > 0`, the error message is prefixed with `Failed after N attempt(s).` so it is clear how many attempts were made.
- The retry count is stored in the database alongside the task and synced automatically if you change `retries=` and re-run `create_chain`.
- If any attempt succeeds, the run continues normally — the retry history is not recorded separately.

### Example

```
✗ always_fails_task    [failed]
  error: Failed after 3 attempt(s).
         RuntimeError: Persistent failure!
```

## Concurrency control

By default a chain can have any number of simultaneous runs. Set `max_concurrent` to limit this.

### Configure a limit

Pass `max_concurrent=N` to `create_chain`. `0` (the default) means unlimited.

```python
# Only one run of this chain may be active at a time
orch.create_chain(
    "tiered_pipeline",
    ["fetch_data", "tier_router", "score_low", "score_mid", "score_high", "report"],
    max_concurrent=1,
)

# Allow up to three parallel runs
orch.create_chain("quick_pipeline", ["fetch_data", "save_data"], max_concurrent=3)
```

The limit is stored in the database and synced automatically if you change it and call `create_chain` again.

### Behaviour when the limit is reached

`run_chain` raises `ConcurrencyError` immediately — no run record is created:

```python
from orchestrator import ConcurrencyError

try:
    run = orch.run_chain("tiered_pipeline")
except ConcurrencyError as e:
    print(e)  # Chain 'tiered_pipeline' already has 1 active run(s) (max_concurrent=1).
```

The background scheduler catches `ConcurrencyError` silently, so a slow scheduled run never causes a queue of blocked attempts to accumulate.

### How it works

The check-and-increment is protected by an in-memory `threading.Lock`, making it atomic with no TOCTOU race. Each thread that successfully acquires a slot decrements the counter in a `finally` block when the run completes (whether success, failure, or exception). Each thread also gets its own SQLite connection (WAL mode), so concurrent runs never block each other at the database level.

## Scheduling

Chains can be given a cron schedule at creation time. The scheduler runs in a background thread inside your Python process and fires `run_chain` automatically at each tick.

### Attach a schedule to a chain

Pass any standard 5-field cron expression to `create_chain`:

```python
orch.create_chain(
    "tiered_pipeline",
    ["fetch_data", "tier_router", "score_low", "score_mid", "score_high", "report"],
    schedule="0 6 * * *",   # daily at 06:00 UTC
)
```

The expression is validated immediately — an invalid string raises `ValueError` before anything is written to the database.

Chains without a `schedule` (the default) are unaffected.

### Start and stop the scheduler

```python
n = orch.start_scheduler()   # returns number of jobs registered
print(f"{n} chain(s) scheduled")

# keep the process alive, e.g. in a server or loop
try:
    import time
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    orch.stop_scheduler()
```

`start_scheduler` reads all chains from the database, registers a cron job for each one that has a `schedule`, and starts a background thread. `stop_scheduler` shuts the thread down cleanly.

### Cron expression reference

| Expression | Meaning |
|---|---|
| `* * * * *` | Every minute |
| `0 * * * *` | Every hour on the hour |
| `0 6 * * *` | Daily at 06:00 UTC |
| `0 6 * * 1` | Every Monday at 06:00 UTC |
| `0 6 1 * *` | First day of every month at 06:00 |
| `*/15 * * * *` | Every 15 minutes |

Fields are `minute hour day-of-month month day-of-week`, same as standard Unix cron.

## Web UI

A local dashboard is included for browsing run history visually.

### Launch

```bash
uv run uvicorn orchestrator.web.app:app --reload --port 8000
```

Then open `http://localhost:8000`.

### Pages

| Route | Description |
|---|---|
| `/` | Dashboard — all chains with last-run status |
| `/chains/{name}` | All runs for a chain, newest first |
| `/runs/{run_id}` | Run detail — pipeline diagram + timeline |
| `/api/runs/{run_id}` | JSON run data (used for live polling) |

### Run detail view

Each run page shows:

- **Pipeline diagram** — a Mermaid `flowchart LR` with each task node colour-coded by status (green = success, red = failed, grey = skipped, blue = running, yellow = pending). Branch tasks render as diamonds; unchosen options appear grey.
- **Timeline** — horizontal bars scaled proportionally to wall-clock duration, one row per task
- **Steps table** — per-task status, duration, output keys, and full error traceback if the task failed

If a run is still in progress, the page polls `/api/runs/{run_id}` every second and updates the diagram, timeline, and status badge live until the run completes.

### Schedule display

Chains that have a `schedule` show a cron pill on the dashboard card (hover for the next fire time) and a schedule box on the chain detail page showing the expression and the computed next run time.

### Tips

- Run `uv run python examples.py` first to populate the database with example chains and runs.
- The UI reads from `orchestrator.db` in the current working directory, the same file the engine writes to.

## Running the examples

### Run all seven pipelines

`examples.py` registers every task and chain, runs them all in sequence, and prints a summary of each run. Use this to populate the database before opening the web UI.

```bash
uv run python examples.py
```

```
============================================================
Running full_pipeline (expect: success)
============================================================
  [fetch_data] fetching from 'api/v1/students'...
  [validate_data] 3 records validated.
  [transform_data] enriched 3 records.
  [save_data] wrote 3 records to output.json.

  ✓ Run 0e3df5d5... | chain=full_pipeline | status=success | duration=2.015s
    ✓ fetch_data           [success] → ['records', 'source']
    ✓ validate_data        [success] → ['validated', 'record_count']
    ✓ transform_data       [success] → ['enriched_records']
    ✓ save_data            [success] → ['saved_to', 'record_count']

============================================================
Running bad_pipeline (expect: fail at crash_task, save_data skipped)
============================================================
  [fetch_data] fetching from 'api/v2/students'...

  ✗ Run 22c49a38... | chain=bad_pipeline | status=failed | duration=2.018s
    ✓ fetch_data           [success] → ['records', 'source']
    ✗ crash_task           [failed]
      error: RuntimeError: Simulated task failure!
    — save_data            [skipped]

============================================================
Running tiered_pipeline (expect: score_high green, others skipped, report green)
============================================================
  [fetch_data] fetching from 'api/v1/students'...
  [validate_data] 3 records validated.
  [score_high] 3 records in high tier.
  [report] tier=high, count=3

  ✓ Run 9f1a2b3c... | chain=tiered_pipeline | status=success | duration=2.043s
    ✓ fetch_data           [success] → ['records', 'source']
    ✓ validate_data        [success] → ['validated', 'record_count']
    ✓ tier_router          [success] → ['selected']
    — score_low            [skipped]
    — score_mid            [skipped]
    ✓ score_high           [success] → ['tier', 'record_count']
    ✓ report               [success] → ['report']

============================================================
Running timeout_pipeline (expect: fast_step ok, slow_step times out, after_step skipped)
============================================================
  [fast_step] done.
  [slow_step] starting (will time out)...

  ✗ Run e0b19710... | chain=timeout_pipeline | status=failed | duration=1.018s
    ✓ fast_step            [success] → ['fast_done']
    ✗ slow_step            [failed]
      error: Task 'slow_step' timed out after 1.0s.
    — after_step           [skipped]

============================================================
Running retry_pipeline (expect: flaky_task fails twice then succeeds, save_data runs)
============================================================
  [flaky_task] attempt 1 failed.
  [flaky_task] attempt 2 failed.
  [flaky_task] attempt 3 succeeded.
  [save_data] wrote 0 records to output.json.

  ✓ Run d8355992... | chain=retry_pipeline | status=success | duration=2.015s
    ✓ flaky_task           [success] → ['recovered']
    ✓ save_data            [success] → ['saved_to', 'record_count']

============================================================
Running exhausted_pipeline (expect: always_fails_task exhausts retries, save_data skipped)
============================================================

  ✗ Run e5865a6b... | chain=exhausted_pipeline | status=failed | duration=2.015s
    ✗ always_fails_task    [failed]
      error: RuntimeError: Persistent failure!
    — save_data            [skipped]
```

### Start the scheduler

`main.py` imports all tasks from `examples.py`, attaches a cron schedule to `tiered_pipeline`, and starts the background scheduler. It runs until interrupted.

```bash
uv run python main.py
```

```
Scheduler started — 1 chain(s) registered. Press Ctrl+C to stop.
```

## Database schema

All data is stored in a local SQLite file (`orchestrator.db` by default).

| Table | Description |
|---|---|
| `chains` | Named chain definitions — includes optional `schedule` (cron expression) |
| `tasks` | Ordered task definitions belonging to a chain — includes `task_type` (`task` or `branch`), `branch_options` (JSON array), `timeout`, and `retries` |
| `runs` | Each execution of a chain, with status and timing |
| `task_runs` | Per-task result within a run — input, output (JSON), error, and timing |

## Project structure

```
orchestrator/
├── orchestrator/
│   ├── __init__.py         — public API
│   ├── models.py           — dataclasses: Chain, Task, Run, TaskRun, Status, TaskType
│   ├── db.py               — SQLite CRUD layer
│   ├── engine.py           — Orchestrator class
│   └── web/
│       ├── __init__.py     — exposes FastAPI app
│       ├── app.py          — routes, data helpers, Mermaid/timeline generation
│       └── templates/
│           ├── base.html   — layout, nav, Mermaid CDN
│           ├── index.html  — chain cards dashboard
│           ├── chain.html  — run list for a chain
│           └── run.html    — pipeline diagram + timeline + steps table
├── examples.py             — all task/chain definitions; runs all four pipelines when executed directly
├── main.py                 — scheduler entry point; imports examples.py and starts the background scheduler
└── pyproject.toml
```

## API reference

### `Orchestrator(db_path, default_timeout)`

| Method | Description |
|---|---|
| `@orch.task(name, description, timeout, retries)` | Decorator to register a task function `(dict) -> dict`; `timeout` defaults to `default_timeout`; `retries` defaults to `0` |
| `@orch.branch(name, options, description, timeout, retries)` | Decorator to register a branch function `(dict) -> str`; `options` is the list of valid return values |
| `orch.register(name, fn, description, timeout, retries)` | Register a task without the decorator |
| `orch.create_chain(name, task_names, description, schedule, max_concurrent)` | Define an ordered chain; optional `schedule` is a 5-field cron expression; `max_concurrent=0` means unlimited |
| `orch.run_chain(name, initial_input, stop_on_failure)` | Execute a chain and return the `Run` record; raises `ConcurrencyError` if `max_concurrent` is reached |
| `orch.summary(run)` | Return a structured dict summarising a run |
| `orch.start_scheduler()` | Start the background cron scheduler; returns number of jobs registered |
| `orch.stop_scheduler()` | Shut down the scheduler cleanly |

### `Database`

Accessible via `orch.db` for direct queries.

| Method | Description |
|---|---|
| `get_chain_by_name(name)` | Look up a chain by name |
| `get_chain_by_id(id)` | Look up a chain by ID |
| `get_tasks_for_chain(chain_id)` | Get ordered tasks for a chain |
| `get_all_chains()` | Get all chains, newest first |
| `get_run_by_id(run_id)` | Look up a single run by ID |
| `get_runs_for_chain(chain_id)` | Get all runs for a chain, newest first |
| `get_task_runs_for_run(run_id)` | Get all task-level results for a run |

### `Status`

```python
class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"
    SKIPPED = "skipped"
```

### `TaskType`

```python
class TaskType(str, Enum):
    TASK   = "task"    # regular task — returns dict
    BRANCH = "branch"  # branch task — returns str (option name)
```

Accessible on `Task.task_type`; stored as a text column in SQLite.
