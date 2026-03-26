import pytest
from orchestrator import Orchestrator, Status, TaskType


# ---------------------------------------------------------------------------
# Core behaviour
# ---------------------------------------------------------------------------

def test_selected_option_runs_others_skipped(tmp_path):
    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {"ran": "a"})
    o.register("opt_b", lambda ctx: {"ran": "b"})
    o.register("opt_c", lambda ctx: {"ran": "c"})
    o.register("after", lambda ctx: {"done": True})

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return "opt_a"

    o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    run = o.run_chain("pipe")

    task_runs = o.db.get_task_runs_for_run(run.id)
    statuses = {tr.task_id: tr.status for tr in task_runs}
    tasks = {t.name: t.id for t in o.db.get_tasks_for_chain(run.chain_id)}

    assert statuses[tasks["opt_a"]] == Status.SUCCESS
    assert statuses[tasks["opt_b"]] == Status.SKIPPED
    assert statuses[tasks["opt_c"]] == Status.SKIPPED


def test_branch_output_is_selected_key(tmp_path):
    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {})
    o.register("opt_b", lambda ctx: {})
    o.register("opt_c", lambda ctx: {})
    o.register("after", lambda ctx: {})

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return "opt_a"

    o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    run = o.run_chain("pipe")

    task_runs = o.db.get_task_runs_for_run(run.id)
    tasks = {t.name: t.id for t in o.db.get_tasks_for_chain(run.chain_id)}
    branch_run = next(tr for tr in task_runs if tr.task_id == tasks["pick"])

    assert branch_run.output == {"selected": "opt_a"}


def test_selected_key_not_in_next_task_ctx(tmp_path):
    """Branch output must NOT be merged into ctx."""
    received_ctx: dict = {}

    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {"ran": "a"})
    o.register("opt_b", lambda ctx: {})
    o.register("opt_c", lambda ctx: {})

    def capture_after(ctx: dict) -> dict:
        received_ctx.update(ctx)
        return {"done": True}

    o.register("after", capture_after)

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return "opt_a"

    o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    o.run_chain("pipe")

    assert "selected" not in received_ctx


def test_after_task_runs_unconditionally(tmp_path):
    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {})
    o.register("opt_b", lambda ctx: {})
    o.register("opt_c", lambda ctx: {})
    o.register("after", lambda ctx: {"done": True})

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return "opt_c"

    o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    run = o.run_chain("pipe")

    task_runs = o.db.get_task_runs_for_run(run.id)
    tasks = {t.name: t.id for t in o.db.get_tasks_for_chain(run.chain_id)}
    after_run = next(tr for tr in task_runs if tr.task_id == tasks["after"])

    assert after_run.status == Status.SUCCESS
    assert run.status == Status.SUCCESS


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_branch_returning_invalid_name_fails(tmp_path):
    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {})
    o.register("opt_b", lambda ctx: {})
    o.register("opt_c", lambda ctx: {})
    o.register("after", lambda ctx: {})

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return "not_an_option"

    o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    run = o.run_chain("pipe")

    task_runs = o.db.get_task_runs_for_run(run.id)
    tasks = {t.name: t.id for t in o.db.get_tasks_for_chain(run.chain_id)}
    branch_run = next(tr for tr in task_runs if tr.task_id == tasks["pick"])

    assert branch_run.status == Status.FAILED
    assert run.status == Status.FAILED


def test_branch_returning_non_string_fails(tmp_path):
    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {})
    o.register("opt_b", lambda ctx: {})
    o.register("opt_c", lambda ctx: {})
    o.register("after", lambda ctx: {})

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return 42  # type: ignore[return-value]

    o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    run = o.run_chain("pipe")

    task_runs = o.db.get_task_runs_for_run(run.id)
    tasks = {t.name: t.id for t in o.db.get_tasks_for_chain(run.chain_id)}
    branch_run = next(tr for tr in task_runs if tr.task_id == tasks["pick"])

    assert branch_run.status == Status.FAILED
    assert "TypeError" in branch_run.error


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_task_type_and_branch_options_persisted(tmp_path):
    o = Orchestrator(str(tmp_path / "b.db"))
    o.register("noop", lambda ctx: {})
    o.register("opt_a", lambda ctx: {})
    o.register("opt_b", lambda ctx: {})
    o.register("opt_c", lambda ctx: {})
    o.register("after", lambda ctx: {})

    @o.branch("pick", options=["opt_a", "opt_b", "opt_c"])
    def pick(ctx: dict) -> str:
        return "opt_b"

    chain = o.create_chain("pipe", ["noop", "pick", "opt_a", "opt_b", "opt_c", "after"])
    tasks = {t.name: t for t in o.db.get_tasks_for_chain(chain.id)}

    assert tasks["pick"].task_type == TaskType.BRANCH
    assert tasks["pick"].branch_options == ["opt_a", "opt_b", "opt_c"]
    assert tasks["noop"].task_type == TaskType.TASK
    assert tasks["after"].task_type == TaskType.TASK
