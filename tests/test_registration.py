import pytest
from orchestrator import Orchestrator


def test_decorator_registers_task(tmp_path):
    orch = Orchestrator(str(tmp_path / "test.db"))

    @orch.task("my_task", description="Does something.")
    def my_task(ctx: dict) -> dict:
        return {}

    assert "my_task" in orch._task_fns
    assert orch._task_fns["my_task"] is my_task


def test_decorator_sets_metadata(tmp_path):
    orch = Orchestrator(str(tmp_path / "test.db"))

    @orch.task("meta_task", description="Useful description.")
    def meta_task(ctx: dict) -> dict:
        return {}

    assert meta_task._task_name == "meta_task"
    assert meta_task._task_description == "Useful description."


def test_register_without_decorator(tmp_path):
    orch = Orchestrator(str(tmp_path / "test.db"))
    fn = lambda ctx: {}
    orch.register("plain", fn)
    assert orch._task_fns["plain"] is fn


def test_create_chain_with_unregistered_task_raises(orch):
    with pytest.raises(ValueError, match="not registered"):
        orch.create_chain("bad", ["nonexistent"])


def test_create_chain_is_idempotent(orch):
    chain1 = orch.create_chain("pipe", ["add_one"])
    chain2 = orch.create_chain("pipe", ["add_one"])
    assert chain1.id == chain2.id


def test_create_chain_persists_to_db(orch):
    orch.create_chain("pipe", ["add_one", "double"])
    chain = orch.db.get_chain_by_name("pipe")
    assert chain is not None
    assert chain.name == "pipe"


def test_create_chain_persists_tasks_in_order(orch):
    orch.create_chain("pipe", ["add_one", "double", "to_str"])
    chain = orch.db.get_chain_by_name("pipe")
    tasks = orch.db.get_tasks_for_chain(chain.id)
    assert [t.name for t in tasks] == ["add_one", "double", "to_str"]
    assert [t.step_order for t in tasks] == [0, 1, 2]
