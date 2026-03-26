import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

from .models import Chain, Run, Status, Task, TaskRun, TaskType

_DT_FMT = "%Y-%m-%dT%H:%M:%S.%f"


def _dt(value: str | None) -> datetime | None:
    return datetime.strptime(value, _DT_FMT) if value else None


def _dt_req(value: str) -> datetime:
    """Parse a required (non-null) datetime string."""
    return datetime.strptime(value, _DT_FMT)


def _ts(value: datetime | None) -> str | None:
    return value.strftime(_DT_FMT) if value else None


class Database:
    def __init__(self, path: str = "orchestrator.db") -> None:
        self._path = Path(path)
        self._local = threading.local()
        # Initialise the main-thread connection and create the schema.
        self._local.conn = self._make_conn()
        self._setup_schema()

    def _make_conn(self) -> sqlite3.Connection:
        """Open a new SQLite connection for the calling thread."""
        conn = sqlite3.connect(str(self._path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @property
    def _conn(self) -> sqlite3.Connection:
        """Return the calling thread's connection, creating one if needed."""
        if not hasattr(self._local, "conn"):
            self._local.conn = self._make_conn()
        return self._local.conn

    def _setup_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS chains (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id          TEXT PRIMARY KEY,
                chain_id    TEXT NOT NULL REFERENCES chains(id),
                name        TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                step_order  INTEGER NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id           TEXT PRIMARY KEY,
                chain_id     TEXT NOT NULL REFERENCES chains(id),
                status       TEXT NOT NULL,
                started_at   TEXT,
                completed_at TEXT,
                error        TEXT
            );

            CREATE TABLE IF NOT EXISTS task_runs (
                id           TEXT PRIMARY KEY,
                run_id       TEXT NOT NULL REFERENCES runs(id),
                task_id      TEXT NOT NULL REFERENCES tasks(id),
                status       TEXT NOT NULL,
                started_at   TEXT,
                completed_at TEXT,
                input        TEXT NOT NULL DEFAULT '{}',
                output       TEXT NOT NULL DEFAULT '{}',
                error        TEXT
            );
        """)
        self._conn.commit()

        for ddl in [
            "ALTER TABLE chains ADD COLUMN schedule TEXT",
            "ALTER TABLE chains ADD COLUMN max_concurrent INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN task_type TEXT NOT NULL DEFAULT 'task'",
            "ALTER TABLE tasks ADD COLUMN branch_options TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE tasks ADD COLUMN timeout REAL NOT NULL DEFAULT 30.0",
            "ALTER TABLE tasks ADD COLUMN retries INTEGER NOT NULL DEFAULT 0",
        ]:
            try:
                self._conn.execute(ddl)
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

    # --- chains ---

    def save_chain(self, chain: Chain) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO chains (id, name, description, created_at, schedule, max_concurrent) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chain.id, chain.name, chain.description, _ts(chain.created_at), chain.schedule, chain.max_concurrent),
        )
        self._conn.commit()

    def update_chain_schedule(self, chain_id: str, schedule: str | None) -> None:
        self._conn.execute(
            "UPDATE chains SET schedule = ? WHERE id = ?", (schedule, chain_id)
        )
        self._conn.commit()

    def update_chain_max_concurrent(self, chain_id: str, max_concurrent: int) -> None:
        self._conn.execute(
            "UPDATE chains SET max_concurrent = ? WHERE id = ?", (max_concurrent, chain_id)
        )
        self._conn.commit()

    def get_chain_by_name(self, name: str) -> Chain | None:
        row = self._conn.execute(
            "SELECT * FROM chains WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            return None
        return Chain(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            created_at=_dt_req(row["created_at"]),
            schedule=row["schedule"],
            max_concurrent=row["max_concurrent"],
        )

    def get_chain_by_id(self, chain_id: str) -> Chain | None:
        row = self._conn.execute(
            "SELECT * FROM chains WHERE id = ?", (chain_id,)
        ).fetchone()
        if row is None:
            return None
        return Chain(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            created_at=_dt_req(row["created_at"]),
            schedule=row["schedule"],
            max_concurrent=row["max_concurrent"],
        )

    def get_all_chains(self) -> list[Chain]:
        rows = self._conn.execute(
            "SELECT * FROM chains ORDER BY created_at DESC"
        ).fetchall()
        return [
            Chain(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                created_at=_dt_req(row["created_at"]),
                schedule=row["schedule"],
                max_concurrent=row["max_concurrent"],
            )
            for row in rows
        ]

    def count_active_runs_for_chain(self, chain_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM runs WHERE chain_id = ? AND status = 'running'",
            (chain_id,),
        ).fetchone()
        return row[0] if row else 0

    # --- tasks ---

    def save_task(self, task: Task) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO tasks "
            "(id, chain_id, name, description, step_order, created_at, task_type, branch_options, timeout, retries) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task.id, task.chain_id, task.name, task.description, task.step_order,
                _ts(task.created_at), task.task_type.value, json.dumps(task.branch_options),
                task.timeout, task.retries,
            ),
        )
        self._conn.commit()

    def update_task_timeout(self, task_id: str, timeout: float) -> None:
        self._conn.execute(
            "UPDATE tasks SET timeout = ? WHERE id = ?", (timeout, task_id)
        )
        self._conn.commit()

    def update_task_retries(self, task_id: str, retries: int) -> None:
        self._conn.execute(
            "UPDATE tasks SET retries = ? WHERE id = ?", (retries, task_id)
        )
        self._conn.commit()

    def get_tasks_for_chain(self, chain_id: str) -> list[Task]:
        rows = self._conn.execute(
            "SELECT * FROM tasks WHERE chain_id = ? ORDER BY step_order", (chain_id,)
        ).fetchall()
        return [
            Task(
                id=row["id"],
                chain_id=row["chain_id"],
                name=row["name"],
                description=row["description"],
                step_order=row["step_order"],
                created_at=_dt_req(row["created_at"]),
                task_type=TaskType(row["task_type"]),
                branch_options=json.loads(row["branch_options"]),
                timeout=row["timeout"],
                retries=row["retries"],
            )
            for row in rows
        ]

    # --- runs ---

    def save_run(self, run: Run) -> None:
        self._conn.execute(
            "INSERT INTO runs (id, chain_id, status, started_at, completed_at, error) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (run.id, run.chain_id, run.status, _ts(run.started_at), _ts(run.completed_at), run.error),
        )
        self._conn.commit()

    def update_run(self, run: Run) -> None:
        self._conn.execute(
            "UPDATE runs SET status = ?, started_at = ?, completed_at = ?, error = ? WHERE id = ?",
            (run.status, _ts(run.started_at), _ts(run.completed_at), run.error, run.id),
        )
        self._conn.commit()

    def get_run_by_id(self, run_id: str) -> Run | None:
        row = self._conn.execute(
            "SELECT * FROM runs WHERE id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        return Run(
            id=row["id"],
            chain_id=row["chain_id"],
            status=Status(row["status"]),
            started_at=_dt(row["started_at"]),
            completed_at=_dt(row["completed_at"]),
            error=row["error"],
        )

    def get_runs_for_chain(self, chain_id: str) -> list[Run]:
        rows = self._conn.execute(
            "SELECT * FROM runs WHERE chain_id = ? ORDER BY started_at DESC", (chain_id,)
        ).fetchall()
        return [
            Run(
                id=row["id"],
                chain_id=row["chain_id"],
                status=Status(row["status"]),
                started_at=_dt(row["started_at"]),
                completed_at=_dt(row["completed_at"]),
                error=row["error"],
            )
            for row in rows
        ]

    # --- task runs ---

    def save_task_run(self, task_run: TaskRun) -> None:
        self._conn.execute(
            "INSERT INTO task_runs (id, run_id, task_id, status, started_at, completed_at, input, output, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_run.id,
                task_run.run_id,
                task_run.task_id,
                task_run.status,
                _ts(task_run.started_at),
                _ts(task_run.completed_at),
                json.dumps(task_run.input),
                json.dumps(task_run.output),
                task_run.error,
            ),
        )
        self._conn.commit()

    def update_task_run(self, task_run: TaskRun) -> None:
        self._conn.execute(
            "UPDATE task_runs SET status = ?, started_at = ?, completed_at = ?, "
            "input = ?, output = ?, error = ? WHERE id = ?",
            (
                task_run.status,
                _ts(task_run.started_at),
                _ts(task_run.completed_at),
                json.dumps(task_run.input),
                json.dumps(task_run.output),
                task_run.error,
                task_run.id,
            ),
        )
        self._conn.commit()

    def get_task_runs_for_run(self, run_id: str) -> list[TaskRun]:
        rows = self._conn.execute(
            """
            SELECT tr.*, t.step_order
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE tr.run_id = ?
            ORDER BY t.step_order
            """,
            (run_id,),
        ).fetchall()
        return [
            TaskRun(
                id=row["id"],
                run_id=row["run_id"],
                task_id=row["task_id"],
                status=Status(row["status"]),
                started_at=_dt(row["started_at"]),
                completed_at=_dt(row["completed_at"]),
                input=json.loads(row["input"]),
                output=json.loads(row["output"]),
                error=row["error"],
            )
            for row in rows
        ]

    def close(self) -> None:
        if hasattr(self._local, "conn"):
            self._local.conn.close()
            del self._local.conn
