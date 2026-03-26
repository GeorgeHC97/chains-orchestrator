from orchestrator import Status


def test_summary_contains_expected_keys(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    s = orch.summary(run)

    assert set(s.keys()) == {"run_id", "chain_name", "status", "duration_s", "steps"}


def test_summary_chain_name(orch):
    orch.create_chain("my_chain", ["add_one"])
    run = orch.run_chain("my_chain", initial_input={"value": 0})
    assert orch.summary(run)["chain_name"] == "my_chain"


def test_summary_status_success(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    assert orch.summary(run)["status"] == "success"


def test_summary_status_failed(orch):
    orch.create_chain("pipe", ["failing"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    assert orch.summary(run)["status"] == "failed"


def test_summary_duration_is_non_negative(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    assert orch.summary(run)["duration_s"] >= 0


def test_summary_steps_count_matches_chain_length(orch):
    orch.create_chain("pipe", ["add_one", "double", "to_str"])
    run = orch.run_chain("pipe", initial_input={"value": 1})
    assert len(orch.summary(run)["steps"]) == 3


def test_summary_steps_have_correct_task_names(orch):
    orch.create_chain("pipe", ["add_one", "double"])
    run = orch.run_chain("pipe", initial_input={"value": 1})
    names = [s["task"] for s in orch.summary(run)["steps"]]
    assert names == ["add_one", "double"]


def test_summary_step_output_keys(orch):
    orch.create_chain("pipe", ["add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    step = orch.summary(run)["steps"][0]
    assert step["output_keys"] == ["value"]


def test_summary_failed_step_includes_error(orch):
    orch.create_chain("pipe", ["failing"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    step = orch.summary(run)["steps"][0]
    assert step["status"] == "failed"
    assert step["error"] is not None
    assert "boom" in step["error"]


def test_summary_skipped_step_has_no_error(orch):
    orch.create_chain("pipe", ["failing", "add_one"])
    run = orch.run_chain("pipe", initial_input={"value": 0})
    steps = orch.summary(run)["steps"]
    skipped = steps[1]
    assert skipped["status"] == Status.SKIPPED.value
    assert skipped["error"] is None
