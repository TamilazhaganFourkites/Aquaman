"""Deterministic control-flow tests for the Ocean pipeline graph.

The whole point of Aquaman is that the *control flow* is code, not prompt-following.
These tests verify that flow with the agents mocked out — no API key, no `claude`
CLI, no real work — so every path is asserted in milliseconds:

  - happy coding path -> ready flip
  - RCA no-fix (terminal) and RCA fix -> gates -> coder -> ... -> flip
  - review loop then approve; review-iteration cap
  - Station-6 code_fault loop then pass; code_fault budget exhausted -> stop
  - could_not_verify -> stop
  - unsupported route -> stop

Run: `pip install -e '.[test]' && pytest`
"""
from __future__ import annotations

import asyncio
import collections
import json

import pytest

from ocean_pipeline import agents, config, gitops, graph, schemas


class Script:
    """Per-test control over what the mocked agents return, plus call counts."""

    def __init__(self, *, route="coding", rca_fix=False, review_seq=("APPROVE",),
                 sit_seq=("passed",)):
        self.route = route
        self.rca_fix = rca_fix
        self.review_seq = list(review_seq)
        self.sit_seq = list(sit_seq)
        self._review_i = 0
        self._sit_i = 0
        self.calls: collections.Counter[str] = collections.Counter()

    def _next(self, seq, i_attr):
        i = getattr(self, i_attr)
        val = seq[min(i, len(seq) - 1)]
        setattr(self, i_attr, i + 1)
        return val

    def next_review(self):
        return self._next(self.review_seq, "_review_i")

    def next_sit(self):
        return self._next(self.sit_seq, "_sit_i")


def _install(script: Script, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    vdir = tmp_path / "verdicts"
    vdir.mkdir()
    monkeypatch.setattr(config, "automation_verdict_path", lambda tid: vdir / f"{tid}.json")
    nowhere = str(tmp_path / "nonexistent.json")  # -> _load_json returns {}

    async def fake_run_agent(**kw):
        node = kw["node"]
        script.calls[node] += 1
        if node.startswith("researcher"):
            return schemas.ResearchVerdict(route=script.route, packet_path=nowhere, target_repos=[])
        if node.startswith("rca_agent"):
            return schemas.RcaVerdict(report_path=nowhere, fix_needed=script.rca_fix,
                                      findings_for_coder=(["fix X in ocean-worker"] if script.rca_fix else []))
        if node.startswith("dep_resolver") or node.startswith("reachability_gate"):
            return schemas.ReachabilityVerdict(report_path=nowhere)
        if node.startswith("coder"):
            return schemas.CoderVerdict(branch=f"{kw['ticket_id']}/b", pushed_sha="deadbeef",
                                        repo="cloudqwest/ocean-worker", pr_title="t", pr_body="b")
        if node.startswith("harsh_reviewer"):
            v = script.next_review()
            findings = [{"severity": "MAJOR", "file": "f.rb", "summary": "x"}] if v == "CHANGES_REQUIRED" else []
            return schemas.ReviewVerdict(verdict=v, findings=findings)
        # graph_augment / release_intel
        return schemas.CoderVerdict(branch="b", pushed_sha="sha")

    # Git/PR ops are now plain code (gitops), not agent calls — stub them and count the calls
    # under the same keys the tests already assert on (open_pr / flip_ready).
    def fake_open_draft_pr(slug, branch, title, body):
        script.calls["open_pr"] += 1
        return 123

    def fake_cross_link_and_ready(slug, pr_number, test_pr_url=""):
        script.calls["flip_ready"] += 1

    async def fake_run_skill(**kw):
        script.calls["automation_testing"] += 1
        outcome = script.next_sit()  # passed | code_fault | could_not_verify
        verdict = {
            "ticket_id": kw["ticket_id"],
            "pr_number": 123,
            "automation_result": "passed" if outcome == "passed" else "failed",
            "failure_class": "" if outcome == "passed" else outcome,
            "execution_mode": "local-mock-first",
            "tests": [{"name": "test_x", "result": "passed" if outcome == "passed" else "failed"}],
            "test_automation_pr_url": "https://github.com/cloudqwest/test-automation/pull/9" if outcome == "passed" else "",
            "findings_for_coder": [{"test": "test_x", "cause": "bug"}] if outcome == "code_fault" else [],
        }
        config.automation_verdict_path(kw["ticket_id"]).write_text(json.dumps(verdict))

    monkeypatch.setattr(agents, "run_agent", fake_run_agent)
    monkeypatch.setattr(agents, "run_skill", fake_run_skill)
    monkeypatch.setattr(gitops, "open_draft_pr", fake_open_draft_pr)
    monkeypatch.setattr(gitops, "cross_link_and_ready", fake_cross_link_and_ready)


def _run(ticket="MM-1"):
    app = graph.build_graph().compile()  # no checkpointer needed for a single invoke
    initial = {
        "ticket_id": ticket, "execution_id": "EXE-test", "profile": "isbu", "context": "",
        "review_iteration": 0, "review_findings": [], "coding_attempts": 0, "sit_findings": [],
    }
    return asyncio.run(app.ainvoke(initial, config={"recursion_limit": 100}))


# ----------------------------------------------------------------- pure routing
def test_route_after_research():
    assert graph.route_after_research({"route": "rca"}) == "rca"
    assert graph.route_after_research({"route": "coding"}) == "coding"
    assert graph.route_after_research({"route": "sop"}) == "unsupported"
    assert graph.route_after_research({}) == "unsupported"


def test_after_review():
    assert graph.after_review({"review_verdict": "APPROVE", "review_iteration": 1}) == "approve"
    assert graph.after_review({"review_verdict": "CHANGES_REQUIRED", "review_iteration": 1}) == "rework"
    assert graph.after_review({"review_verdict": "CHANGES_REQUIRED", "review_iteration": 2}) == "approve"


def test_after_automation():
    assert graph.after_automation({"automation_result": "passed"}) == "pass"
    assert graph.after_automation({"automation_result": "failed", "failure_class": "code_fault",
                                   "coding_attempts": 0}) == "code_fault"
    assert graph.after_automation({"automation_result": "failed", "failure_class": "code_fault",
                                   "coding_attempts": 2}) == "stop"
    assert graph.after_automation({"automation_result": "failed", "failure_class": "could_not_verify",
                                   "coding_attempts": 0}) == "stop"


def test_after_rca():
    assert graph.after_rca({"rca_fix_needed": True}) == "fix_needed"
    assert graph.after_rca({"rca_fix_needed": False}) == "done"


# ----------------------------------------------------------------- full paths
def test_happy_path(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert final["ready_flipped"] is True
    assert s.calls["coder"] == 1
    assert s.calls["harsh_reviewer"] == 1
    assert s.calls["automation_testing"] == 1
    assert s.calls["flip_ready"] == 1


def test_review_loop_then_approve(tmp_path, monkeypatch):
    s = Script(review_seq=["CHANGES_REQUIRED", "APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["coder"] == 2      # initial + one rework
    assert s.calls["harsh_reviewer"] == 2


def test_review_iteration_cap(tmp_path, monkeypatch):
    s = Script(review_seq=["CHANGES_REQUIRED"], sit_seq=["passed"])  # never approves
    _install(s, tmp_path, monkeypatch)
    final = _run()
    # capped at MAX_REVIEW_ITERATIONS then proceeds to open_pr and on to SIT
    assert s.calls["harsh_reviewer"] == config.MAX_REVIEW_ITERATIONS
    assert final["final_status"] == "completed"


def test_code_fault_loop_then_pass(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["code_fault", "passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["automation_testing"] == 2
    assert s.calls["coder"] == 2      # initial + one code_fault rework
    assert s.calls["open_pr"] == 1  # opened once; re-entry is a no-op


def test_code_fault_budget_exhausted(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["code_fault"])  # always code_fault
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "coding_attempts_exhausted" in final["final_outcome"]
    assert s.calls["automation_testing"] == config.MAX_CODING_ATTEMPTS + 1  # initial + N reworks


def test_could_not_verify_stops(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["could_not_verify"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "could_not_verify" in final["final_outcome"]
    assert s.calls["automation_testing"] == 1        # no rework loop on could_not_verify
    assert s.calls["flip_ready"] == 0


def test_rca_no_fix_terminal(tmp_path, monkeypatch):
    s = Script(route="rca", rca_fix=False)
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "rca_report"
    assert s.calls["coder"] == 0
    assert s.calls["dep_resolver"] == 0


def test_rca_fix_runs_gates_then_codes(tmp_path, monkeypatch):
    s = Script(route="rca", rca_fix=True, review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["rca_agent"] == 1
    assert s.calls["dep_resolver"] == 1          # RCA fix went THROUGH the gates
    assert s.calls["reachability_gate"] == 1
    assert s.calls["coder"] == 1


def test_unsupported_route_stops(tmp_path, monkeypatch):
    s = Script(route="sop")
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "unsupported route" in final["final_outcome"]
    assert s.calls["coder"] == 0


# ----------------------------------------------------------------- vendored workers (Phase B)
def test_vendored_workers_resolve():
    """The migrated workers are vendored into this repo and resolve BEFORE any fk-aideveloper
    fallback; un-migrated agents still fall back. Guards the Phase B decoupling."""
    for name in ("research.md", "code.md", "review.md"):
        p = agents._agent_path(name)
        assert p == config.VENDORED_AGENTS_DIR / name and p.exists(), f"{name} not vendored"
        assert "Not your job" in p.read_text(), f"{name} missing the process-ownership boundary"
    # un-migrated agents still resolve to the fk-aideveloper station dir
    assert agents._agent_path("fk-coder.md") == config.AGENTS_DIR / "fk-coder.md"
