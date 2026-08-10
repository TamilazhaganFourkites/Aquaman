"""`agents.evaluate_node` is EXECUTED here — every other test monkeypatches it away.

The audit's finding was exact: this function has never been run by the suite. Every test that
reaches D5's node evaluation replaces it, so the whole `agent_md` / `model` / `verdict_model` /
`cwd` wiring rested on one source-grep. A grep cannot tell you that `verdict_model` is passed by
the right keyword, that `node` carries the `eval_` prefix the monitor keys its skip on, or that the
judge is pointed at the node's own tree rather than the control plane's checkout — and each of
those, wrong, produces a run that looks completely normal:

* wrong `model` — the generator grades its own work, which is the bias `JUDGE_MODEL` exists for;
* missing `eval_` prefix — `monitor/app.py` stops skipping the header, and every scored node leaves
  a phantom step pulsing "running" forever, because its completion line matches neither parser;
* wrong `cwd` — the judge cannot read the diff it is scoring and grades the prose it was handed,
  which the worker prompt itself calls not judging.

`run_agent` is the seam: it is the one call `evaluate_node` makes, and stubbing it captures the
arguments without spending an SDK session. Nothing here reaches the network, Jira, QAT or Jenkins.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from ocean_pipeline import agents, config, schemas


@pytest.fixture
def captured(monkeypatch):
    seen = {}

    async def _fake_run_agent(**kwargs):
        seen.update(kwargs)
        return schemas.NodeEvaluation(node="coder", verdict="PASS")

    monkeypatch.setattr(agents, "run_agent", _fake_run_agent)
    return seen


def _call(**over):
    kwargs = dict(node="coder", ticket_id="MM-14816", execution_id="EXE-eval-wiring",
                  node_job="write the fix", node_output="a diff", cwd=Path("/tmp/some-worktree"))
    kwargs.update(over)
    return asyncio.run(agents.evaluate_node(**kwargs))


def test_it_runs_at_all_and_returns_the_typed_verdict(captured):
    """The baseline the suite never had: the function is entered, and what comes back is a
    NodeEvaluation rather than whatever the SDK happened to emit."""
    result = _call()
    assert isinstance(result, schemas.NodeEvaluation)
    assert captured, "run_agent was never called — evaluate_node did nothing"


def test_the_label_carries_the_eval_prefix_the_monitor_skips_on(captured):
    """`monitor/app.py` skips headers starting `eval_`. Drop the prefix and every scored node
    leaves a phantom step stuck 'running' in the UI, one per evaluation, forever."""
    _call(node="harsh_reviewer")
    assert captured["node"] == "eval_harsh_reviewer"


def test_the_judge_model_is_used_not_the_station_model(captured):
    """A generator grading its own work has self-preference bias — the reason `harsh_reviewer`
    already uses JUDGE_MODEL, and an accuracy judge is that situation in a purer form."""
    _call()
    assert captured["model"] == config.JUDGE_MODEL
    if config.JUDGE_MODEL != config.STATION_MODEL:
        assert captured["model"] != config.STATION_MODEL


def test_the_verdict_is_schema_constrained(captured):
    """Without `verdict_model`, the judge returns free text and `_eval_node`'s accuracy read
    silently becomes 'whatever parsed'."""
    _call()
    assert captured["verdict_model"] is schemas.NodeEvaluation


def test_the_judge_reads_the_nodes_own_tree(captured):
    """Its prompt DEMANDS reading the real artifacts and grepping the repo. Pointed at the control
    plane's own checkout it would grade the prose it was handed instead."""
    wt = Path("/tmp/some-worktree")
    _call(cwd=wt)
    assert captured["cwd"] == wt
    assert captured["cwd"] != config.FK_AIDEVELOPER_DIR


def test_it_loads_the_worker_that_actually_exists(captured):
    """`agent_md` names a file in fk-aideveloper's ocean-coding-agent. A typo here is a run-time
    resolution failure on a station nothing else exercises. Skips when the sibling is absent."""
    _call()
    assert captured["agent_md"] == "node-evaluator.md"
    workers = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-coding-agent" / "workers"
    if not workers.is_dir():
        pytest.skip("fk-aideveloper checkout not present")
    assert (workers / captured["agent_md"]).exists(), (
        f"{captured['agent_md']} does not exist in {workers}")


def test_the_prompt_carries_both_the_job_and_the_output_being_judged(captured):
    """A judge handed only the output cannot say whether it was the RIGHT output — 'accurate'
    is meaningless without the node's job beside it."""
    _call(node_job="UNIQUE-JOB-TEXT", node_output="UNIQUE-OUTPUT-TEXT")
    prompt = captured["task_prompt"]
    assert "UNIQUE-JOB-TEXT" in prompt and "UNIQUE-OUTPUT-TEXT" in prompt
    assert "do not edit code" in prompt, "nothing stops the judge from 'fixing' what it is grading"
