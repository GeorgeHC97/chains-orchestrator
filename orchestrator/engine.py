import threading
import time
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone

from .db import Database
from .models import Chain, Run, Status, Task, TaskRun, TaskType

TaskFn = Callable[[dict], dict]
BranchFn = Callable[[dict], str]


class ConcurrencyError(Exception):
    """Raised when run_chain is called but the chain is already at its max_concurrent limit."""


class Orchestrator:
    """
    Local task orchestrator backed by SQLite.

    Usage
    -----
    orch = Orchestrator()

    @orch.task("fetch_data")
    def fetch_data(ctx: dict) -> dict:
        return {"records": [...]}

    orch.create_chain("my_pipeline", ["fetch_data", "process_data"])
    run = orch.run_chain("my_pipeline", initial_input={"source": "api"})
    """

    def __init__(self, db_path: str = "orchestrator.db", default_timeout: float = 30.0) -> None:
        self.db = Database(db_path)
        self.default_timeout = default_timeout
        self._task_fns: dict[str, TaskFn] = {}
        self._branch_fns: dict[str, BranchFn] = {}
        self._branch_options: dict[str, list[str]] = {}
        self._active_run_counts: dict[str, int] = {}
        self._active_runs_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def task(self, name: str, description: str = "", timeout: float | None = None, retries: int = 0) -> Callable[[TaskFn], TaskFn]:
        """Decorator that registers a function as a named task."""

        def decorator(fn: TaskFn) -> TaskFn:
            self._task_fns[name] = fn
            fn._task_name = name  # type: ignore[attr-defined]
            fn._task_description = description  # type: ignore[attr-defined]
            fn._task_timeout = timeout if timeout is not None else self.default_timeout  # type: ignore[attr-defined]
            fn._task_retries = retries  # type: ignore[attr-defined]
            return fn

        return decorator

    def register(self, name: str, fn: TaskFn, description: str = "", timeout: float | None = None, retries: int = 0) -> None:
        """Register a task function without using the decorator."""
        self._task_fns[name] = fn
        fn._task_timeout = timeout if timeout is not None else self.default_timeout  # type: ignore[attr-defined]
        fn._task_retries = retries  # type: ignore[attr-defined]

    def branch(self, name: str, options: list[str], description: str = "", timeout: float | None = None, retries: int = 0) -> Callable[[BranchFn], BranchFn]:
        """Decorator that registers a function as a branch task."""

        def decorator(fn: BranchFn) -> BranchFn:
            self._task_fns[name] = fn  # type: ignore[assignment]
            self._branch_fns[name] = fn
            self._branch_options[name] = options
            fn._task_name = name  # type: ignore[attr-defined]
            fn._task_description = description  # type: ignore[attr-defined]
            fn._task_timeout = timeout if timeout is not None else self.default_timeout  # type: ignore[attr-defined]
            fn._task_retries = retries  # type: ignore[attr-defined]
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Chain management
    # ------------------------------------------------------------------

    def create_chain(
        self,
        name: str,
        task_names: list[str],
        description: str = "",
        schedule: str | None = None,
        max_concurrent: int = 0,
    ) -> Chain:
        """
        Define a chain as an ordered list of registered task names.
        Re-creating a chain with the same name is a no-op (idempotent).
        """
        for task_name in task_names:
            if task_name not in self._task_fns:
                raise ValueError(f"Task '{task_name}' is not registered.")

        if schedule is not None:
            try:
                from apscheduler.triggers.cron import CronTrigger
                CronTrigger.from_crontab(schedule)
            except (ValueError, KeyError) as exc:
                raise ValueError(f"Invalid cron schedule '{schedule}': {exc}") from exc

        existing = self.db.get_chain_by_name(name)
        if existing:
            if existing.schedule != schedule:
                self.db.update_chain_schedule(existing.id, schedule)
                existing.schedule = schedule
            if existing.max_concurrent != max_concurrent:
                self.db.update_chain_max_concurrent(existing.id, max_concurrent)
                existing.max_concurrent = max_concurrent
            existing_tasks = {t.name: t for t in self.db.get_tasks_for_chain(existing.id)}
            for task_name in task_names:
                fn = self._task_fns[task_name]
                new_timeout = getattr(fn, "_task_timeout", self.default_timeout)
                new_retries = getattr(fn, "_task_retries", 0)
                stored = existing_tasks.get(task_name)
                if stored is not None:
                    if stored.timeout != new_timeout:
                        self.db.update_task_timeout(stored.id, new_timeout)
                    if stored.retries != new_retries:
                        self.db.update_task_retries(stored.id, new_retries)
            return existing

        chain = Chain(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            created_at=datetime.now(timezone.utc),
            schedule=schedule,
            max_concurrent=max_concurrent,
        )
        self.db.save_chain(chain)

        for order, task_name in enumerate(task_names):
            fn = self._task_fns[task_name]
            task_desc = getattr(fn, "_task_description", "")
            task_timeout = getattr(fn, "_task_timeout", self.default_timeout)
            task_retries = getattr(fn, "_task_retries", 0)
            is_branch = task_name in self._branch_fns
            task = Task(
                id=str(uuid.uuid4()),
                chain_id=chain.id,
                name=task_name,
                description=task_desc,
                step_order=order,
                created_at=datetime.now(timezone.utc),
                task_type=TaskType.BRANCH if is_branch else TaskType.TASK,
                branch_options=self._branch_options.get(task_name, []),
                timeout=task_timeout,
                retries=task_retries,
            )
            self.db.save_task(task)

        return chain

    # ------------------------------------------------------------------
    # Execution helpers
    # ------------------------------------------------------------------

    def _run_with_timeout(self, fn: Callable, arg: dict, timeout: float):
        """Run fn(arg) in a thread; raise TimeoutError if it exceeds timeout seconds."""
        pool = ThreadPoolExecutor(max_workers=1)
        future = pool.submit(fn, arg)
        pool.shutdown(wait=False)
        return future.result(timeout=timeout)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run_chain(
        self,
        chain_name: str,
        initial_input: dict | None = None,
        stop_on_failure: bool = True,
    ) -> Run:
        """
        Execute all tasks in the chain sequentially.

        The output of each task is merged into the context and passed as
        the input to the next task.  If a task raises an exception the run
        is marked FAILED; remaining tasks are SKIPPED (unless
        stop_on_failure=False, in which case they still run).

        Returns the completed Run record.
        """
        chain = self.db.get_chain_by_name(chain_name)
        if chain is None:
            raise ValueError(f"Chain '{chain_name}' does not exist.")

        if chain.max_concurrent > 0:
            with self._active_runs_lock:
                active = self._active_run_counts.get(chain.id, 0)
                if active >= chain.max_concurrent:
                    raise ConcurrencyError(
                        f"Chain '{chain_name}' already has {active} active run(s) "
                        f"(max_concurrent={chain.max_concurrent})."
                    )
                self._active_run_counts[chain.id] = active + 1

        try:
            return self._execute_chain(chain, chain_name, initial_input, stop_on_failure)
        finally:
            if chain.max_concurrent > 0:
                with self._active_runs_lock:
                    self._active_run_counts[chain.id] -= 1

    def _execute_chain(
        self,
        chain: "Chain",
        chain_name: str,
        initial_input: dict | None,
        stop_on_failure: bool,
    ) -> "Run":
        tasks = self.db.get_tasks_for_chain(chain.id)
        if not tasks:
            raise ValueError(f"Chain '{chain_name}' has no tasks.")

        run = Run(
            id=str(uuid.uuid4()),
            chain_id=chain.id,
            status=Status.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        self.db.save_run(run)

        ctx: dict = initial_input.copy() if initial_input else {}
        chain_failed = False
        skip_names: set[str] = set()

        for task in tasks:
            task_run = TaskRun(
                id=str(uuid.uuid4()),
                run_id=run.id,
                task_id=task.id,
                status=Status.PENDING,
                input=ctx.copy(),
            )
            self.db.save_task_run(task_run)

            if task.name in skip_names or (chain_failed and stop_on_failure):
                task_run.status = Status.SKIPPED
                self.db.update_task_run(task_run)
                continue

            fn = self._task_fns.get(task.name)
            if fn is None:
                task_run.status = Status.FAILED
                task_run.error = f"No registered function for task '{task.name}'."
                task_run.started_at = datetime.now(timezone.utc)
                task_run.completed_at = datetime.now(timezone.utc)
                self.db.update_task_run(task_run)
                chain_failed = True
                continue

            task_run.status = Status.RUNNING
            task_run.started_at = datetime.now(timezone.utc)
            self.db.update_task_run(task_run)

            if task.task_type == TaskType.BRANCH:
                last_error: str | None = None
                for attempt in range(task.retries + 1):
                    try:
                        chosen = self._run_with_timeout(fn, ctx.copy(), task.timeout)
                        if not isinstance(chosen, str):
                            raise TypeError(
                                f"Branch '{task.name}' must return a str, got {type(chosen).__name__}."
                            )
                        if chosen not in task.branch_options:
                            raise ValueError(
                                f"Branch '{task.name}' returned '{chosen}' which is not in "
                                f"options {task.branch_options}."
                            )
                        for opt in task.branch_options:
                            if opt != chosen:
                                skip_names.add(opt)
                        task_run.output = {"selected": chosen}
                        task_run.status = Status.SUCCESS
                        last_error = None
                        break
                    except FutureTimeoutError:
                        last_error = f"Branch '{task.name}' timed out after {task.timeout}s."
                    except Exception:
                        last_error = traceback.format_exc()
                    if attempt < task.retries:
                        time.sleep(1)
                if last_error is not None:
                    task_run.status = Status.FAILED
                    prefix = f"Failed after {task.retries + 1} attempt(s).\n" if task.retries > 0 else ""
                    task_run.error = prefix + last_error
                    chain_failed = True
            else:
                last_error = None
                for attempt in range(task.retries + 1):
                    try:
                        output = self._run_with_timeout(fn, ctx.copy(), task.timeout)
                        if not isinstance(output, dict):
                            raise TypeError(f"Task '{task.name}' must return a dict, got {type(output).__name__}.")
                        task_run.output = output
                        task_run.status = Status.SUCCESS
                        ctx.update(output)
                        last_error = None
                        break
                    except FutureTimeoutError:
                        last_error = f"Task '{task.name}' timed out after {task.timeout}s."
                    except Exception:
                        last_error = traceback.format_exc()
                    if attempt < task.retries:
                        time.sleep(1)
                if last_error is not None:
                    task_run.status = Status.FAILED
                    prefix = f"Failed after {task.retries + 1} attempt(s).\n" if task.retries > 0 else ""
                    task_run.error = prefix + last_error
                    chain_failed = True

            task_run.completed_at = datetime.now(timezone.utc)
            self.db.update_task_run(task_run)

        run.status = Status.FAILED if chain_failed else Status.SUCCESS
        run.completed_at = datetime.now(timezone.utc)
        self.db.update_run(run)
        return run

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def _run_chain_safe(self, chain_name: str) -> None:
        """Run a chain, silently dropping the attempt if the concurrency limit is reached."""
        try:
            self.run_chain(chain_name)
        except ConcurrencyError:
            pass

    def start_scheduler(self) -> int:
        """
        Start the APScheduler background scheduler.

        Reads all chains that have a ``schedule`` cron expression from the
        database and registers a job for each one.  Returns the number of
        jobs registered.  Call ``stop_scheduler()`` to shut down cleanly.
        """
        from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
        from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]

        self._scheduler = BackgroundScheduler()
        chains = self.db.get_all_chains()
        count = 0
        for chain in chains:
            if chain.schedule:
                self._scheduler.add_job(
                    self._run_chain_safe,
                    CronTrigger.from_crontab(chain.schedule),
                    args=[chain.name],
                    id=chain.name,
                    replace_existing=True,
                    name=f"orch:{chain.name}",
                )
                count += 1
        self._scheduler.start()
        return count

    def stop_scheduler(self) -> None:
        """Stop the background scheduler if it is running."""
        if hasattr(self, "_scheduler") and self._scheduler.running:
            self._scheduler.shutdown()

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def summary(self, run: Run) -> dict:
        """Return a structured summary of a completed run."""
        chain = self.db.get_chain_by_id(run.chain_id)
        task_runs = self.db.get_task_runs_for_run(run.id)
        task_map = {t.id: t.name for t in self.db.get_tasks_for_chain(run.chain_id)}

        duration = None
        if run.started_at and run.completed_at:
            duration = (run.completed_at - run.started_at).total_seconds()

        steps = []
        for tr in task_runs:
            step_duration = None
            if tr.started_at and tr.completed_at:
                step_duration = (tr.completed_at - tr.started_at).total_seconds()
            steps.append(
                {
                    "task": task_map.get(tr.task_id, tr.task_id),
                    "status": tr.status.value,
                    "duration_s": step_duration,
                    "error": tr.error,
                    "output_keys": list(tr.output.keys()),
                }
            )

        return {
            "run_id": run.id,
            "chain_name": chain.name if chain else run.chain_id,
            "status": run.status.value,
            "duration_s": duration,
            "steps": steps,
        }
