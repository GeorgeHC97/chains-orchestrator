import pytest
from orchestrator import Orchestrator, Status


@pytest.fixture
def orch(tmp_path):
    return Orchestrator(str(tmp_path / "t.db"))


# ---------------------------------------------------------------------------
# Basic retry behaviour
# ---------------------------------------------------------------------------

def test_task_succeeds_on_retry(orch):
    attempts = [0]

    def flaky(ctx):
        attempts[0] += 1
        if attempts[0] < 3:
            raise RuntimeError("transient")
        return {"ok": True}

    orch.register("flaky", flaky, retries=2)
    orch.create_chain("pipe", ["flaky"])
    run = orch.run_chain("pipe")

    assert run.status == Status.SUCCESS
    assert attempts[0] == 3


def test_task_fails_after_exhausting_retries(orch):
    orch.register("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("oops")), retries=2)
    orch.create_chain("pipe", ["bad"])
    run = orch.run_chain("pipe")

    assert run.status == Status.FAILED


def test_error_message_includes_attempt_count(orch):
    orch.register("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("oops")), retries=2)
    orch.create_chain("pipe", ["bad"])
    run = orch.run_chain("pipe")

    tr = orch.db.get_task_runs_for_run(run.id)[0]
    assert "3 attempt(s)" in tr.error


def test_no_attempt_prefix_when_retries_zero(orch):
    orch.register("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("oops")))
    orch.create_chain("pipe", ["bad"])
    run = orch.run_chain("pipe")

    tr = orch.db.get_task_runs_for_run(run.id)[0]
    assert "attempt" not in tr.error


def test_remaining_tasks_skipped_after_exhausted_retries(orch):
    orch.register("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("oops")), retries=1)
    orch.register("after", lambda ctx: {"done": True})
    orch.create_chain("pipe", ["bad", "after"])
    run = orch.run_chain("pipe")

    trs = orch.db.get_task_runs_for_run(run.id)
    assert trs[0].status == Status.FAILED
    assert trs[1].status == Status.SKIPPED


def test_subsequent_task_runs_after_successful_retry(orch):
    attempts = [0]

    def flaky(ctx):
        attempts[0] += 1
        if attempts[0] < 2:
            raise RuntimeError("transient")
        return {"ok": True}

    orch.register("flaky", flaky, retries=1)
    orch.register("after", lambda ctx: {"done": True})
    orch.create_chain("pipe", ["flaky", "after"])
    run = orch.run_chain("pipe")

    assert run.status == Status.SUCCESS
    trs = orch.db.get_task_runs_for_run(run.id)
    assert trs[0].status == Status.SUCCESS
    assert trs[1].status == Status.SUCCESS


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_retries_persisted_to_db(orch):
    orch.register("t", lambda ctx: {}, retries=3)
    chain = orch.create_chain("pipe", ["t"])
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert tasks[0].retries == 3


def test_default_retries_is_zero(orch):
    orch.register("t", lambda ctx: {})
    chain = orch.create_chain("pipe", ["t"])
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert tasks[0].retries == 0


def test_retries_updated_when_chain_recreated(orch):
    orch.register("t", lambda ctx: {}, retries=1)
    chain = orch.create_chain("pipe", ["t"])
    assert orch.db.get_tasks_for_chain(chain.id)[0].retries == 1

    orch.register("t", lambda ctx: {}, retries=5)
    orch.create_chain("pipe", ["t"])
    assert orch.db.get_tasks_for_chain(chain.id)[0].retries == 5


# ---------------------------------------------------------------------------
# Decorator syntax
# ---------------------------------------------------------------------------

def test_task_decorator_with_retries(tmp_path):
    orch = Orchestrator(str(tmp_path / "t.db"))

    @orch.task("t", retries=4)
    def t(ctx):
        return {}

    chain = orch.create_chain("pipe", ["t"])
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert tasks[0].retries == 4


# ---------------------------------------------------------------------------
# Branch retries
# ---------------------------------------------------------------------------

def test_branch_retries_on_failure(orch):
    attempts = [0]

    orch.register("noop", lambda ctx: {})

    @orch.branch("pick", options=["a", "b"], retries=2)
    def pick(ctx):
        attempts[0] += 1
        if attempts[0] < 3:
            raise RuntimeError("transient branch failure")
        return "a"

    orch.register("a", lambda ctx: {})
    orch.register("b", lambda ctx: {})
    orch.create_chain("pipe", ["noop", "pick", "a", "b"])
    run = orch.run_chain("pipe")

    assert run.status == Status.SUCCESS
    assert attempts[0] == 3
