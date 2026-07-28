"""Tests for the QA-only overnight batch sweep (ocean_pipeline.qa_batch).

Reuses test_graph.py's Script/_install harness — qa_batch drives the EXACT SAME node functions
(sit_resolve/sit_author/qa_review_gate/sit_run/sit_testrail/sit_triage/learn_repo) the main graph's
own tests already cover, so this file only needs to test what's DIFFERENT about the batch subgraph:
sequential multi-ticket looping, the no-coder code_fault terminus, and that it never flips a PR.
"""
from __future__ import annotations

import asyncio

from ocean_pipeline import agents, config, gitops, nodes, qa_batch, telemetry
from test_graph import Script, _install


def test_build_qa_subgraph_compiles():
    assert qa_batch.build_qa_subgraph().compile() is not None


def test_single_ticket_pass(tmp_path, monkeypatch):
    s = Script(sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    result = asyncio.run(qa_batch.run_one("MM-1"))
    assert result["final_status"] == "completed"
    assert "sit_passed" in result["final_outcome"]
    assert s.calls["sit_run"] == 1


def test_code_fault_terminates_without_a_coder(tmp_path, monkeypatch):
    """qa-batch has no coder node — a code_fault must terminate immediately with a clear 'needs a
    coder re-run' outcome, never attempt to loop back and fix the code itself."""
    s = Script(sit_seq=["code_fault"])
    _install(s, tmp_path, monkeypatch)
    result = asyncio.run(qa_batch.run_one("MM-2"))
    assert result["final_status"] == "failed"
    assert "needs a coder re-run" in result["final_outcome"]
    assert s.calls["sit_triage"] == 1   # exactly once — no rework loop exists in this subgraph


def test_could_not_verify_terminates(tmp_path, monkeypatch):
    s = Script(sit_seq=["could_not_verify"])
    _install(s, tmp_path, monkeypatch)
    result = asyncio.run(qa_batch.run_one("MM-3"))
    assert result["final_status"] == "failed"


def test_batch_is_sequential_and_never_flips_a_pr(tmp_path, monkeypatch):
    """Two tickets, one pass one fail — both must run (independent Script instances since sit_seq is
    per-ticket), and gitops.cross_link_and_ready (the flip primitive) must NEVER be called: qa-batch
    mode has no flip_ready node at all, so this also guards against a future accidental import of it."""
    flip_calls = []
    monkeypatch.setattr(gitops, "cross_link_and_ready",
                        lambda *a, **kw: flip_calls.append((a, kw)))
    monkeypatch.setattr(gitops, "open_draft_pr", lambda *a, **kw: 123)
    monkeypatch.setattr(config, "QA_REVIEW_AUTO", True)
    monkeypatch.setattr(config, "QA_TESTRAIL", False)
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    # This test doesn't go through test_graph.py's _install, so it needs its own Docker stub —
    # see _install's comment: without it, sit_run's real `docker info` call depends on Docker
    # Desktop actually being up on whatever machine runs the suite.
    monkeypatch.setattr(nodes, "_docker_preflight_reason", lambda: "")
    vdir = tmp_path / "verdicts"
    vdir.mkdir()
    monkeypatch.setattr(config, "automation_verdict_path", lambda tid: vdir / f"{tid}.json")

    import json as _json

    async def fake_run_skill(**kw):
        node, tid = kw["node"], kw["ticket_id"]
        path = config.automation_verdict_path(tid)
        if node in ("sit_run", "learn_repo"):
            return
        if node == "sit_author":
            path.write_text(_json.dumps({"ticket_id": tid, "test_path": "test_x.py"}))
            return
        if node == "sit_resolve":
            path.write_text(_json.dumps({"ticket_id": tid, "pr_number": 1, "needs_onboarding": False}))
            return
        # sit_triage: MM-A passes, MM-B fails could_not_verify
        outcome = "passed" if tid == "MM-A" else "could_not_verify"
        path.write_text(_json.dumps({
            "ticket_id": tid, "automation_result": "passed" if outcome == "passed" else "failed",
            "failure_class": "" if outcome == "passed" else outcome, "execution_mode": "local-mock-first",
            "tests": [], "needs_onboarding": False, "onboard_repo": "",
        }))

    monkeypatch.setattr(agents, "run_skill", fake_run_skill)
    monkeypatch.setattr(telemetry, "station_event", lambda *a, **kw: None)
    monkeypatch.setattr(telemetry, "new_execution_id", lambda: "EXE-batch-test-" + str(id(object())))

    results = asyncio.run(qa_batch.run_batch(["MM-A", "MM-B"]))
    assert [r["ticket_id"] for r in results] == ["MM-A", "MM-B"]
    assert results[0]["final_status"] == "completed"
    assert results[1]["final_status"] == "failed"
    assert flip_calls == []   # the whole point: qa-batch never flips a service PR


def test_load_tickets_from_file(tmp_path):
    f = tmp_path / "tickets.txt"
    f.write_text("MM-101\n# a comment\n\nMM-102  # inline comment\n   \nMM-103\n")

    class Args:
        file = str(f)
        tickets = []

    assert qa_batch._load_tickets(Args()) == ["MM-101", "MM-102", "MM-103"]


def test_run_one_forces_qa_review_auto_even_when_caller_left_it_off(tmp_path, monkeypatch):
    """Regression: QA_REVIEW_AUTO used to be set only in main(), so a caller that imports run_one/
    run_batch directly (this module's own docstring documents `python -m ocean_pipeline.qa_batch`
    as supported usage) would hit qa_review_gate's interrupt() with nobody watching -- the run
    never resumes (no checkpointer here), silently defeating the unattended-sweep purpose. Force
    QA_REVIEW_AUTO back to its real default (False) AFTER _install's own override, proving run_one
    itself restores the invariant rather than relying on the caller to have set it."""
    s = Script(sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "QA_REVIEW_AUTO", False)   # override _install's own default back off
    result = asyncio.run(qa_batch.run_one("MM-9"))
    assert result["final_status"] == "completed"
    assert config.QA_REVIEW_AUTO is True   # run_one must have set it back


def test_run_one_catches_a_crash_and_returns_a_failed_result(tmp_path, monkeypatch):
    """The module docstring's own stated guarantee -- 'a crash just fails that ticket and the
    batch moves on' -- was never directly tested; only the clean-completion paths were. Force
    the subgraph itself to raise (not just a node returning a failed verdict) and confirm run_one
    catches it, still returns ticket_id/execution_id, and doesn't propagate."""
    s = Script(sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)

    async def fake_run_skill_that_crashes(**kw):
        raise RuntimeError("simulated ocean-automation-testing crash")

    monkeypatch.setattr(agents, "run_skill", fake_run_skill_that_crashes)
    result = asyncio.run(qa_batch.run_one("MM-10"))
    assert result["ticket_id"] == "MM-10"
    assert "execution_id" in result
    assert result["final_status"] == "failed"
    assert "RuntimeError" in result["final_outcome"]
    assert "simulated ocean-automation-testing crash" in result["final_outcome"]


def test_run_batch_continues_after_one_ticket_crashes(tmp_path, monkeypatch):
    """The batch-level guarantee: one ticket's crash must not stop the rest, and the summary must
    still include every ticket's result (crashed or not)."""
    s = Script(sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    real_fake_run_skill = agents.run_skill

    async def fake_run_skill_crashes_for_one_ticket(**kw):
        if kw["ticket_id"] == "MM-CRASH":
            raise RuntimeError("boom")
        return await real_fake_run_skill(**kw)

    monkeypatch.setattr(agents, "run_skill", fake_run_skill_crashes_for_one_ticket)
    results = asyncio.run(qa_batch.run_batch(["MM-CRASH", "MM-OK"]))
    assert [r["ticket_id"] for r in results] == ["MM-CRASH", "MM-OK"]
    assert results[0]["final_status"] == "failed"
    assert "boom" in results[0]["final_outcome"]
    assert results[1]["final_status"] == "completed"
