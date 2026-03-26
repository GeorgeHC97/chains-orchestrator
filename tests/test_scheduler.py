import pytest
from orchestrator import Orchestrator


@pytest.fixture
def sched_orch(tmp_path):
    o = Orchestrator(str(tmp_path / "sched.db"))
    o.register("noop", lambda ctx: {})
    return o


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_schedule_persisted_to_db(sched_orch):
    chain = sched_orch.create_chain("pipe", ["noop"], schedule="0 6 * * *")
    assert chain.schedule == "0 6 * * *"

    # Read back from DB via a fresh query
    stored = sched_orch.db.get_chain_by_name("pipe")
    assert stored.schedule == "0 6 * * *"


def test_no_schedule_stored_as_none(sched_orch):
    chain = sched_orch.create_chain("pipe", ["noop"])
    assert chain.schedule is None

    stored = sched_orch.db.get_chain_by_name("pipe")
    assert stored.schedule is None


def test_invalid_cron_raises_value_error(sched_orch):
    with pytest.raises(ValueError, match="Invalid cron schedule"):
        sched_orch.create_chain("pipe", ["noop"], schedule="not a cron")


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def test_start_scheduler_registers_scheduled_chains(sched_orch):
    sched_orch.create_chain("daily",  ["noop"], schedule="0 6 * * *")
    sched_orch.create_chain("hourly", ["noop"], schedule="0 * * * *")
    sched_orch.create_chain("no_sched", ["noop"])

    count = sched_orch.start_scheduler()
    try:
        assert count == 2
        jobs = sched_orch._scheduler.get_jobs()
        job_ids = {j.id for j in jobs}
        assert "daily"  in job_ids
        assert "hourly" in job_ids
        assert "no_sched" not in job_ids
    finally:
        sched_orch.stop_scheduler()


def test_stop_scheduler_shuts_down_cleanly(sched_orch):
    sched_orch.create_chain("pipe", ["noop"], schedule="0 6 * * *")
    sched_orch.start_scheduler()
    assert sched_orch._scheduler.running
    sched_orch.stop_scheduler()
    assert not sched_orch._scheduler.running


def test_stop_scheduler_without_start_is_safe(sched_orch):
    """stop_scheduler() must not raise if the scheduler was never started."""
    sched_orch.stop_scheduler()  # should be a no-op


def test_start_scheduler_with_no_schedules_returns_zero(sched_orch):
    sched_orch.create_chain("pipe", ["noop"])
    count = sched_orch.start_scheduler()
    try:
        assert count == 0
    finally:
        sched_orch.stop_scheduler()


# ---------------------------------------------------------------------------
# Next-run-at helper (via app layer)
# ---------------------------------------------------------------------------

def test_next_run_at_returns_string_for_valid_cron():
    from orchestrator.web.app import _next_run_at
    result = _next_run_at("0 6 * * *")
    assert result is not None
    assert "UTC" in result


def test_next_run_at_returns_none_for_none():
    from orchestrator.web.app import _next_run_at
    assert _next_run_at(None) is None


def test_next_run_at_returns_none_for_invalid():
    from orchestrator.web.app import _next_run_at
    assert _next_run_at("not valid") is None
