import pytest
from orchestrator import Status


def test_successful_chain_returns_success_status(orch):
    orch.create_chain("pipe", ["add_one", "double"])
    run = orch.run_chain("pipe", initial_input={"value": 3})
    assert run.status == Status.SUCCESS


def test_context_flows_between_tasks(orch):
    orch.create_chain("pipe", ["add_one", "double"])
    run = orch.run_chain("pipe", initial_input={"value": 3})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    # add_one: 3 + 1 = 4
    assert task_runs[0].output == {"value": 4}
    # double: 4 * 2 = 8
    assert task_runs[1].output == {"value": 8}


def test_initial_input_is_passed_to_first_task(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 10})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    assert task_runs[0].input == {"value": 10}


def test_run_with_no_initial_input(orch):
    orch.register("no_input", lambda ctx: {"done": True})
    orch.create_chain("pipe", ["no_input"])
    run = orch.run_chain("pipe")
    assert run.status == Status.SUCCESS


def test_failing_task_marks_run_as_failed(orch):
    orch.create_chain("pipe", ["add_one", "failing"])
    run = orch.run_chain("pipe", initial_input={"value": 1})
    assert run.status == Status.FAILED


def test_failing_task_skips_remaining_tasks_by_default(orch):
    orch.create_chain("pipe", ["add_one", "failing", "double"])
    run = orch.run_chain("pipe", initial_input={"value": 1})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    assert task_runs[0].status == Status.SUCCESS   # add_one
    assert task_runs[1].status == Status.FAILED    # failing
    assert task_runs[2].status == Status.SKIPPED   # double


def test_stop_on_failure_false_continues_after_error(orch):
    orch.create_chain("pipe", ["add_one", "failing", "double"])
    run = orch.run_chain("pipe", initial_input={"value": 1}, stop_on_failure=False)

    task_runs = orch.db.get_task_runs_for_run(run.id)
    assert task_runs[0].status == Status.SUCCESS
    assert task_runs[1].status == Status.FAILED
    assert task_runs[2].status == Status.SUCCESS  # ran despite earlier failure
    assert run.status == Status.FAILED            # overall run still failed


def test_task_returning_non_dict_marks_task_failed(orch):
    orch.create_chain("pipe", ["bad_return"])
    run = orch.run_chain("pipe", initial_input={"value": 0})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    assert task_runs[0].status == Status.FAILED
    assert "TypeError" in task_runs[0].error
    assert run.status == Status.FAILED


def test_failed_task_stores_traceback(orch):
    orch.create_chain("pipe", ["failing"])
    run = orch.run_chain("pipe", initial_input={"value": 0})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    assert task_runs[0].error is not None
    assert "boom" in task_runs[0].error


def test_run_nonexistent_chain_raises(orch):
    with pytest.raises(ValueError, match="does not exist"):
        orch.run_chain("ghost")


def test_run_timestamps_are_set(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})

    assert run.started_at is not None
    assert run.completed_at is not None
    assert run.completed_at >= run.started_at


def test_task_run_timestamps_are_set(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    tr = task_runs[0]
    assert tr.started_at is not None
    assert tr.completed_at is not None
    assert tr.completed_at >= tr.started_at


def test_skipped_tasks_have_no_timestamps(orch):
    orch.create_chain("pipe", ["failing", "add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    skipped = task_runs[1]
    assert skipped.status == Status.SKIPPED
    assert skipped.started_at is None
    assert skipped.completed_at is None
