import pytest
from orchestrator import Orchestrator


@pytest.fixture
def orch(tmp_path):
    """A fresh in-memory orchestrator for each test."""
    o = Orchestrator(str(tmp_path / "test.db"))

    o.register("add_one", lambda ctx: {"value": ctx["value"] + 1})
    o.register("double",  lambda ctx: {"value": ctx["value"] * 2})
    o.register("to_str",  lambda ctx: {"result": str(ctx["value"])})
    o.register("failing", lambda ctx: (_ for _ in ()).throw(RuntimeError("boom")))
    o.register("bad_return", lambda ctx: "not a dict")  # type: ignore[return-value]

    return o
