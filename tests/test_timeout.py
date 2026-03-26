import time
import pytest
from orchestrator import Orchestrator, Status


@pytest.fixture
def orch(tmp_path):
    return Orchestrator(str(tmp_path / "t.db"))


# ---------------------------------------------------------------------------
# Basic timeout behaviour
# ---------------------------------------------------------------------------

def test_task_that_exceeds_timeout_is_failed(orch):
    orch.register("slow", lambda ctx: (time.sleep(5), {})[1], timeout=0.1)
    orch.create_chain("pipe", ["slow"])
    run = orch.run_chain("pipe")

    assert run.status == Status.FAILED
    tr = orch.db.get_task_runs_for_run(run.id)[0]
    assert tr.status == Status.FAILED
    assert "timed out" in tr.error


def test_timeout_error_message_includes_duration(orch):
    orch.register("slow", lambda ctx: (time.sleep(5), {})[1], timeout=0.1)
    orch.create_chain("pipe", ["slow"])
    run = orch.run_chain("pipe")

    tr = orch.db.get_task_runs_for_run(run.id)[0]
    assert "0.1s" in tr.error


def test_task_within_timeout_succeeds(orch):
    orch.register("fast", lambda ctx: {"ok": True}, timeout=5.0)
    orch.create_chain("pipe", ["fast"])
    run = orch.run_chain("pipe")

    assert run.status == Status.SUCCESS


def test_remaining_tasks_skipped_after_timeout(orch):
    orch.register("slow",  lambda ctx: (time.sleep(5), {})[1], timeout=0.1)
    orch.register("after", lambda ctx: {"done": True})
    orch.create_chain("pipe", ["slow", "after"])
    run = orch.run_chain("pipe")

    trs = orch.db.get_task_runs_for_run(run.id)
    assert trs[0].status == Status.FAILED
    assert trs[1].status == Status.SKIPPED


# ---------------------------------------------------------------------------
# Default timeout
# ---------------------------------------------------------------------------

def test_default_timeout_applied_when_none_specified(tmp_path):
    orch = Orchestrator(str(tmp_path / "t.db"), default_timeout=0.1)
    orch.register("slow", lambda ctx: (time.sleep(5), {})[1])
    orch.create_chain("pipe", ["slow"])
    run = orch.run_chain("pipe")

    assert run.status == Status.FAILED
    tr = orch.db.get_task_runs_for_run(run.id)[0]
    assert "timed out" in tr.error


def test_per_task_timeout_overrides_default(tmp_path):
    orch = Orchestrator(str(tmp_path / "t.db"), default_timeout=0.1)
    orch.register("task", lambda ctx: {"ok": True}, timeout=10.0)
    orch.create_chain("pipe", ["task"])
    run = orch.run_chain("pipe")

    assert run.status == Status.SUCCESS


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_timeout_persisted_to_db(orch):
    orch.register("t", lambda ctx: {}, timeout=42.0)
    chain = orch.create_chain("pipe", ["t"])
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert tasks[0].timeout == 42.0


def test_default_timeout_persisted_when_unspecified(tmp_path):
    orch = Orchestrator(str(tmp_path / "t.db"), default_timeout=15.0)
    orch.register("t", lambda ctx: {})
    chain = orch.create_chain("pipe", ["t"])
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert tasks[0].timeout == 15.0


def test_timeout_updated_when_chain_recreated(orch):
    """Re-registering a task with a new timeout and calling create_chain again updates the DB."""
    orch.register("t", lambda ctx: {}, timeout=10.0)
    chain = orch.create_chain("pipe", ["t"])
    assert orch.db.get_tasks_for_chain(chain.id)[0].timeout == 10.0

    # Re-register with a new timeout and re-create the chain (idempotent path)
    orch.register("t", lambda ctx: {}, timeout=1.0)
    orch.create_chain("pipe", ["t"])
    assert orch.db.get_tasks_for_chain(chain.id)[0].timeout == 1.0


def test_updated_timeout_enforced_at_runtime(orch):
    """After updating the timeout via create_chain, the new value is used when running."""
    orch.register("slow", lambda ctx: (time.sleep(5), {})[1], timeout=10.0)
    chain = orch.create_chain("pipe", ["slow"])
    assert orch.db.get_tasks_for_chain(chain.id)[0].timeout == 10.0

    # Tighten the timeout and recreate — should now fail
    orch.register("slow", lambda ctx: (time.sleep(5), {})[1], timeout=0.1)
    orch.create_chain("pipe", ["slow"])
    assert orch.db.get_tasks_for_chain(chain.id)[0].timeout == 0.1

    run = orch.run_chain("pipe")
    assert run.status == Status.FAILED
    tr = orch.db.get_task_runs_for_run(run.id)[0]
    assert "timed out" in tr.error


# ---------------------------------------------------------------------------
# Branch timeout
# ---------------------------------------------------------------------------

def test_branch_timeout_fails_run(orch):
    orch.register("noop", lambda ctx: {})

    @orch.branch("pick", options=["a", "b"], timeout=0.1)
    def pick(ctx):
        time.sleep(5)
        return "a"

    orch.register("a", lambda ctx: {})
    orch.register("b", lambda ctx: {})
    orch.create_chain("pipe", ["noop", "pick", "a", "b"])
    run = orch.run_chain("pipe")

    assert run.status == Status.FAILED
    trs = {t.name: tr for t, tr in zip(
        orch.db.get_tasks_for_chain(run.chain_id),
        orch.db.get_task_runs_for_run(run.id),
    )}
    assert trs["pick"].status == Status.FAILED
    assert "timed out" in trs["pick"].error


# ---------------------------------------------------------------------------
# Decorator syntax
# ---------------------------------------------------------------------------

def test_task_decorator_with_timeout(tmp_path):
    orch = Orchestrator(str(tmp_path / "t.db"))

    @orch.task("t", timeout=99.0)
    def t(ctx):
        return {}

    chain = orch.create_chain("pipe", ["t"])
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert tasks[0].timeout == 99.0
