"""Tests that data survives being written to and read back from SQLite."""

from orchestrator import Orchestrator, Status


def test_run_history_persists_across_orchestrator_instances(tmp_path):
    db_path = str(tmp_path / "test.db")

    orch1 = Orchestrator(db_path)
    orch1.register("add_one", lambda ctx: {"value": ctx["value"] + 1})
    orch1.create_chain("pipe", ["add_one"])
    run = orch1.run_chain("pipe", initial_input={"value": 5})
    run_id = run.id

    # Create a second instance pointing at the same DB
    orch2 = Orchestrator(db_path)
    chain = orch2.db.get_chain_by_name("pipe")
    assert chain is not None

    runs = orch2.db.get_runs_for_chain(chain.id)
    assert len(runs) == 1
    assert runs[0].id == run_id
    assert runs[0].status == Status.SUCCESS


def test_multiple_runs_are_all_recorded(orch):
    orch.create_chain("pipe", ["add_one"])
    orch.run_chain("pipe", initial_input={"value": 1})
    orch.run_chain("pipe", initial_input={"value": 2})
    orch.run_chain("pipe", initial_input={"value": 3})

    chain = orch.db.get_chain_by_name("pipe")
    runs = orch.db.get_runs_for_chain(chain.id)
    assert len(runs) == 3


def test_get_chain_by_id_returns_correct_chain(orch):
    chain = orch.create_chain("pipe", ["add_one"])
    fetched = orch.db.get_chain_by_id(chain.id)
    assert fetched is not None
    assert fetched.id == chain.id
    assert fetched.name == "pipe"


def test_get_chain_by_id_returns_none_for_missing(orch):
    assert orch.db.get_chain_by_id("nonexistent-id") is None


def test_get_chain_by_name_returns_none_for_missing(orch):
    assert orch.db.get_chain_by_name("ghost") is None


def test_task_run_input_and_output_round_trip(orch):
    orch.create_chain("pipe", ["add_one", "double"])
    run = orch.run_chain("pipe", initial_input={"value": 7})

    task_runs = orch.db.get_task_runs_for_run(run.id)
    # double receives the output of add_one merged into original ctx
    assert task_runs[1].input == {"value": 8}
    assert task_runs[1].output == {"value": 16}


def test_runs_returned_newest_first(orch):
    orch.create_chain("pipe", ["add_one"])
    run_a = orch.run_chain("pipe", initial_input={"value": 1})
    run_b = orch.run_chain("pipe", initial_input={"value": 2})

    chain = orch.db.get_chain_by_name("pipe")
    runs = orch.db.get_runs_for_chain(chain.id)
    # most recent first
    assert runs[0].id == run_b.id
    assert runs[1].id == run_a.id
