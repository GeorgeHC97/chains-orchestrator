import threading
import pytest
from orchestrator import ConcurrencyError, Orchestrator, Status


@pytest.fixture
def orch(tmp_path):
    return Orchestrator(str(tmp_path / "t.db"))


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------

def test_no_limit_by_default(orch):
    """max_concurrent=0 means unlimited — no error is raised."""
    orch.register("t", lambda ctx: {})
    orch.create_chain("pipe", ["t"])
    run = orch.run_chain("pipe")
    assert run.status == Status.SUCCESS


def _make_blocking_task(done: threading.Event, started: threading.Event | None = None):
    """Return a task function that sets started, then blocks until done is set."""
    def task(ctx):
        if started is not None:
            started.set()
        done.wait()
        return {}
    return task


def test_raises_when_at_limit(tmp_path):
    """A second call raises ConcurrencyError while the first run is still active."""
    orch = Orchestrator(str(tmp_path / "t.db"))
    started = threading.Event()
    done = threading.Event()

    orch.register("slow", _make_blocking_task(done, started))
    orch.create_chain("pipe", ["slow"], max_concurrent=1)

    t = threading.Thread(target=orch.run_chain, args=("pipe",))
    t.start()
    assert started.wait(timeout=10.0), "run never started"

    try:
        with pytest.raises(ConcurrencyError):
            orch.run_chain("pipe")
    finally:
        done.set()
        t.join()


def test_allows_run_after_previous_completes(orch):
    orch.register("t", lambda ctx: {})
    orch.create_chain("pipe", ["t"], max_concurrent=1)

    run1 = orch.run_chain("pipe")
    assert run1.status == Status.SUCCESS

    run2 = orch.run_chain("pipe")
    assert run2.status == Status.SUCCESS


def test_limit_greater_than_one(tmp_path):
    """max_concurrent=2 allows two concurrent runs but blocks a third."""
    orch = Orchestrator(str(tmp_path / "t.db"))

    started = [threading.Event(), threading.Event()]
    done = threading.Event()
    call_counter = [0]
    counter_lock = threading.Lock()

    def slow(ctx):
        with counter_lock:
            idx = call_counter[0]
            call_counter[0] += 1
        if idx < 2:
            started[idx].set()
        done.wait()
        return {}

    orch.register("slow", slow)
    orch.create_chain("pipe", ["slow"], max_concurrent=2)

    t1 = threading.Thread(target=orch.run_chain, args=("pipe",))
    t2 = threading.Thread(target=orch.run_chain, args=("pipe",))
    t1.start()
    t2.start()
    assert started[0].wait(timeout=10.0), "first run never started"
    assert started[1].wait(timeout=10.0), "second run never started"

    try:
        with pytest.raises(ConcurrencyError):
            orch.run_chain("pipe")
    finally:
        done.set()
        t1.join()
        t2.join()


def test_error_message_includes_chain_name_and_limit(tmp_path):
    orch = Orchestrator(str(tmp_path / "t.db"))
    started = threading.Event()
    done = threading.Event()

    orch.register("slow", _make_blocking_task(done, started))
    orch.create_chain("my_pipe", ["slow"], max_concurrent=1)

    t = threading.Thread(target=orch.run_chain, args=("my_pipe",))
    t.start()
    assert started.wait(timeout=10.0), "run never started"

    try:
        with pytest.raises(ConcurrencyError, match="my_pipe"):
            orch.run_chain("my_pipe")
        with pytest.raises(ConcurrencyError, match="max_concurrent=1"):
            orch.run_chain("my_pipe")
    finally:
        done.set()
        t.join()


# ---------------------------------------------------------------------------
# Persistence and sync
# ---------------------------------------------------------------------------

def test_max_concurrent_persisted(orch):
    orch.register("t", lambda ctx: {})
    orch.create_chain("pipe", ["t"], max_concurrent=3)
    stored = orch.db.get_chain_by_name("pipe")
    assert stored.max_concurrent == 3


def test_default_max_concurrent_is_zero(orch):
    orch.register("t", lambda ctx: {})
    orch.create_chain("pipe", ["t"])
    stored = orch.db.get_chain_by_name("pipe")
    assert stored.max_concurrent == 0


def test_max_concurrent_updated_when_chain_recreated(orch):
    orch.register("t", lambda ctx: {})
    orch.create_chain("pipe", ["t"], max_concurrent=1)
    assert orch.db.get_chain_by_name("pipe").max_concurrent == 1

    orch.create_chain("pipe", ["t"], max_concurrent=5)
    assert orch.db.get_chain_by_name("pipe").max_concurrent == 5
