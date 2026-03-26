from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class TaskType(str, Enum):
    TASK = "task"
    BRANCH = "branch"


@dataclass
class Chain:
    id: str
    name: str
    created_at: datetime
    description: str = ""
    schedule: str | None = None
    max_concurrent: int = 0


@dataclass
class Task:
    id: str
    chain_id: str
    name: str
    step_order: int
    created_at: datetime
    description: str = ""
    task_type: TaskType = TaskType.TASK
    branch_options: list[str] = field(default_factory=list)
    timeout: float = 30.0
    retries: int = 0


@dataclass
class Run:
    id: str
    chain_id: str
    status: Status
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


@dataclass
class TaskRun:
    id: str
    run_id: str
    task_id: str
    status: Status
    started_at: datetime | None = None
    completed_at: datetime | None = None
    input: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)
    error: str | None = None
