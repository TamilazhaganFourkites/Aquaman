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
import os
import re
import shutil
import subprocess
import urllib.request
from pathlib import Path

import pytest

from ocean_pipeline import agents, config, gitops, graph, jira, metrics, nodes, report, schemas, telemetry, tracing, ui
from ocean_pipeline import cli


class Script:
    """Per-test control over what the mocked agents return, plus call counts."""

    def __init__(self, *, route="coding", rca_fix=False, review_seq=("APPROVE",),
                 sit_seq=("passed",), sme_bucket=""):
        self.route = route
        self.rca_fix = rca_fix
        self.sme_bucket = sme_bucket
        self.review_seq = list(review_seq)
        self.sit_seq = list(sit_seq)
        self._review_i = 0
        self._sit_i = 0
        self._cur = "passed"   # this attempt's SIT outcome, decided at sit_resolve, used at sit_triage
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
            return schemas.ResearchVerdict(route=script.route, packet_path=nowhere,
                                           target_repos=[], domain_bucket=script.sme_bucket)
        if node.startswith("sme_consult"):
            return schemas.SmeVerdict(summary="owner: ocean-worker", findings=["reuse helper X"])
        if node.startswith("rca_agent"):
            return schemas.RcaVerdict(report_path=nowhere, fix_needed=script.rca_fix,
                                      findings_for_coder=(["fix X in ocean-worker"] if script.rca_fix else []))
        if node.startswith("dep_resolver"):
            return schemas.DependencyVerdict(report_path=nowhere)
        if node.startswith("reachability_gate"):
            return schemas.ReachabilityVerdict(report_path=nowhere)
        if node.startswith("coder"):
            return schemas.CoderVerdict(branch=f"{kw['ticket_id']}/b",
                                        repo="cloudqwest/ocean-worker", repo_dir="/tmp/ws/ocean-worker",
                                        pr_title="t", pr_body="b")
        if node.startswith("harsh_reviewer"):
            v = script.next_review()
            findings = [{"severity": "MAJOR", "file": "f.rb", "summary": "x"}] if v == "CHANGES_REQUIRED" else []
            return schemas.ReviewVerdict(verdict=v, findings=findings)
        raise AssertionError(f"fake_run_agent has no mock branch for node {node!r}")

    # Git/PR ops are now plain code (gitops), not agent calls — stub them and count the calls
    # under the same keys the tests already assert on (open_pr / flip_ready).
    def fake_open_draft_pr(slug, branch, title, body):
        script.calls["open_pr"] += 1
        return 123

    def fake_cross_link_and_ready(slug, pr_number, test_pr_url=""):
        script.calls["flip_ready"] += 1

    async def fake_run_skill(**kw):
        # Station 6 is decomposed into sit_resolve -> sit_run -> sit_triage. sit_resolve decides this
        # attempt's outcome (onboard vs proceed); sit_triage finalizes the verdict for a proceed.
        node = kw["node"]
        script.calls[node] += 1
        path = config.automation_verdict_path(kw["ticket_id"])
        if node == "learn_repo" or node == "sit_run":
            return  # onboarding pass / execute phase write no final verdict
        if node == "sit_author":
            path.write_text(json.dumps({"ticket_id": kw["ticket_id"], "test_path": "test_MM_1_ocean.py"}))
            return
        if node == "sit_testrail":
            # run_skill has no execution_id parameter -- the real skill only ever sees the target
            # path embedded literally in the prompt text (nodes.py's sit_testrail interpolates
            # `tr_path` there), so the mock reads it out the same way a real skill would have to.
            m = re.search(r"TestRail run id to (\S+)\.", kw["task_prompt"])
            Path(m.group(1)).write_text("555")
            return
        if node == "sit_resolve":
            script._cur = script.next_sit()  # passed | code_fault | could_not_verify | needs_onboarding
            needs = script._cur == "needs_onboarding"
            path.write_text(json.dumps({
                "ticket_id": kw["ticket_id"], "pr_number": 123,
                "automation_result": "failed" if needs else "passed",   # placeholder; triage finalizes
                "failure_class": "could_not_verify" if needs else "",
                "needs_onboarding": needs,
                "onboard_repo": "ocean-newrepo" if needs else "",
            }))
            return
        # node == "sit_triage": write the canonical verdict for this attempt's outcome
        outcome = script._cur
        result = "passed" if outcome == "passed" else "failed"
        path.write_text(json.dumps({
            "ticket_id": kw["ticket_id"], "pr_number": 123,
            "automation_result": result,
            "failure_class": "" if outcome == "passed" else outcome,
            "execution_mode": "local-mock-first",
            "tests": [{"name": "test_x", "result": result}],
            "test_automation_pr_url": "https://github.com/cloudqwest/test-automation/pull/9" if outcome == "passed" else "",
            "findings_for_coder": [{"test": "test_x", "cause": "bug"}] if outcome == "code_fault" else [],
            "needs_onboarding": False, "onboard_repo": "",
        }))

    # Default the QA review gate and RCA review gate to AUTO (no human pause) + no TestRail, so
    # the full-path tests run without interrupting. Gate-specific tests override these.
    monkeypatch.setattr(config, "QA_REVIEW_AUTO", True)
    monkeypatch.setattr(config, "QA_TESTRAIL", False)
    monkeypatch.setattr(config, "RCA_REVIEW_AUTO", True)
    # sit_run's own Docker resource preflight (_docker_preflight_reason) shells out to the REAL
    # `docker info` unless stubbed — without this, every full-path test that reaches sit_run
    # silently depends on Docker Desktop actually being up on whatever machine runs the suite,
    # rather than the mocked control-flow this file's whole docstring promises ("no real work").
    # Docker-specific tests override this back (test_sit_run_preflight_short_circuit_*).
    monkeypatch.setattr(nodes, "_docker_preflight_reason", lambda *a, **kw: "")

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
    # route_after_research now returns next-node NAME(s), not routing keys.
    assert graph.route_after_research({"route": "rca"}) == "rca_agent"
    assert graph.route_after_research({"route": "sop"}) == "unsupported_route"
    assert graph.route_after_research({}) == "unsupported_route"


def test_route_after_research_coding_fans_out_when_parallel(monkeypatch):
    # Parallel on: coding route fans out to sme ∥ dep ∥ prep_image (a list => LangGraph fan-out).
    monkeypatch.setattr(config, "PARALLEL_ANALYSIS", True)
    dests = graph.route_after_research({"route": "coding"})
    assert isinstance(dests, list)
    assert set(dests) == {"sme_consult", "dep_resolver", "prep_image"}
    # Parallel off: the original sequential chain via sme_consult.
    monkeypatch.setattr(config, "PARALLEL_ANALYSIS", False)
    assert graph.route_after_research({"route": "coding"}) == "sme_consult"


def test_graph_builds_both_topologies(monkeypatch):
    # Both wirings must COMPILE (compile is where unreachable-node / missing-edge guards fire).
    for parallel in (True, False):
        monkeypatch.setattr(config, "PARALLEL_ANALYSIS", parallel)
        app = graph.build_graph().compile()
        nodes_present = set(app.get_graph().nodes)
        assert "prep_image" in nodes_present if parallel else "prep_image" not in nodes_present


def test_parallel_fanout_joins_reachability_once(tmp_path, monkeypatch):
    # The core correctness of #5: research fans out to sme ∥ dep (∥ prep_image), and reachability_gate
    # must run EXACTLY ONCE (a broken join would run it per-predecessor). sme_bucket set so sme_consult
    # actually runs rather than no-opping.
    monkeypatch.setattr(config, "PARALLEL_ANALYSIS", True)
    s = Script(route="coding", sme_bucket="callback_notification")
    _install(s, tmp_path, monkeypatch)
    _run()
    assert s.calls["sme_consult"] == 1        # fanned out
    assert s.calls["dep_resolver"] == 1       # fanned out (in parallel, not after sme)
    assert s.calls["reachability_gate"] == 1  # JOINED once despite 2-3 predecessors
    assert s.calls["coder"] == 1


def test_sequential_baseline_still_works(tmp_path, monkeypatch):
    # Parallel off: the original research -> sme -> dep -> reachability chain, each once.
    monkeypatch.setattr(config, "PARALLEL_ANALYSIS", False)
    s = Script(route="coding", sme_bucket="callback_notification")
    _install(s, tmp_path, monkeypatch)
    _run()
    assert s.calls["sme_consult"] == 1
    assert s.calls["dep_resolver"] == 1
    assert s.calls["reachability_gate"] == 1
    assert s.calls["coder"] == 1


# ----------------------------------------------------------- #1 persistent container
def test_container_directive_toggle(monkeypatch):
    # Off / not-ready -> empty (stations use their own recipe; prompts unchanged, fallback-safe).
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", False)
    assert nodes._container_directive({"container_name": "c", "container_ready": True}, "/w") == ""
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    assert nodes._container_directive({"container_ready": False}, "/w") == ""
    # On + ready -> a directive naming the container and using docker cp/exec (never docker run).
    d = nodes._container_directive({"container_name": "ocean-x-EXE1", "container_ready": True}, "/ws")
    assert "ocean-x-EXE1" in d and "docker cp" in d and "docker exec" in d
    assert "/ws" in d


def test_prep_container_fallback(monkeypatch):
    # No ruby target -> clean no-op (container_ready False), never raises.
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    out = asyncio.run(nodes.prep_container({"execution_id": "EXE1", "target_repos": []}))
    assert out == {"container_ready": False}
    # Ruby target but Docker absent -> still a clean fallback (no container started).
    monkeypatch.setattr(nodes.shutil, "which", lambda _: None)
    out = asyncio.run(nodes.prep_container(
        {"execution_id": "EXE1", "target_repos": [{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]}))
    assert out == {"container_ready": False}


def test_teardown_container_is_safe_noop(monkeypatch):
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    # No container name -> no docker call, returns cleanly.
    assert asyncio.run(nodes.teardown_container({"execution_id": "EXE1", "container_name": ""})) == {}


def test_graph_persistent_container_wiring(monkeypatch):
    # On: reachability -> prep_container -> coder, and both terminals -> teardown_container -> END.
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    edges = {(e.source, e.target) for e in graph.build_graph().compile().get_graph().edges}
    assert ("reachability_gate", "prep_container") in edges
    assert ("prep_container", "coder") in edges
    assert ("flip_ready", "teardown_container") in edges
    assert ("stop_run", "teardown_container") in edges
    # Off: coder is fed directly, no container nodes.
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", False)
    edges = {(e.source, e.target) for e in graph.build_graph().compile().get_graph().edges}
    assert ("reachability_gate", "coder") in edges
    assert not any("container" in a or "container" in b for a, b in edges)


def test_full_coding_flow_with_persistent_container_on(tmp_path, monkeypatch):
    # PC on must not break the flow even when no container starts (empty target_repos -> fallback).
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    s = Script(route="coding")
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert s.calls["coder"] == 1
    assert final["final_status"] in ("completed", "rca_report") or final.get("pr_number")


# ----------------------------------------------------------- #6 warm SIT infra
def test_sit_infra_directive_toggle(monkeypatch):
    monkeypatch.setattr(config, "WARM_SIT_INFRA", False)
    assert nodes._sit_infra_directive({"execution_id": "EXE1"}) == ""
    monkeypatch.setattr(config, "WARM_SIT_INFRA", True)
    d = nodes._sit_infra_directive({"execution_id": "EXE1"})
    assert "ocean-sit-EXE1" in d and "compose -p" in d and "REUSE" in d


def test_teardown_removes_sit_infra(monkeypatch):
    calls = []

    async def rec(args, timeout=120):
        calls.append(args)
        return 0
    monkeypatch.setattr(nodes, "_docker", rec)
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    monkeypatch.setattr(config, "WARM_SIT_INFRA", True)
    asyncio.run(nodes.teardown_container({"execution_id": "EXE1", "container_name": "ocean-x-EXE1"}))
    # removed both the container and the SIT compose project
    assert ["rm", "-f", "ocean-x-EXE1"] in calls
    assert any(a[:2] == ["compose", "-p"] and "ocean-sit-EXE1" in a for a in calls)


def test_graph_warm_sit_infra_wiring_without_container(monkeypatch):
    # #6 alone (no persistent container): teardown node present + terminals routed, but NO prep_container.
    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", False)
    monkeypatch.setattr(config, "WARM_SIT_INFRA", True)
    app = graph.build_graph().compile()
    ns = set(app.get_graph().nodes)
    assert "teardown_container" in ns and "prep_container" not in ns
    edges = {(e.source, e.target) for e in app.get_graph().edges}
    assert ("flip_ready", "teardown_container") in edges
    assert ("stop_run", "teardown_container") in edges
    assert ("reachability_gate", "coder") in edges   # no prep_container inserted


def test_after_review():
    assert graph.after_review({"review_verdict": "APPROVE", "review_iteration": 1}) == "approve"
    assert graph.after_review({"review_verdict": "CHANGES_REQUIRED", "review_iteration": 1}) == "rework"
    assert graph.after_review({"review_verdict": "CHANGES_REQUIRED", "review_iteration": 2}) == "approve"


def test_after_sit_resolve():
    # onboarding is decided at resolve, before authoring/running
    assert graph.after_sit_resolve({}) == "run"
    assert graph.after_sit_resolve({"needs_onboarding": True, "onboard_attempts": 0}) == "onboard"
    assert graph.after_sit_resolve({"needs_onboarding": True,
                                    "onboard_attempts": config.MAX_ONBOARD_ATTEMPTS}) == "stop"


def test_after_sit_triage():
    assert graph.after_sit_triage({"automation_result": "passed"}) == "pass"
    assert graph.after_sit_triage({"automation_result": "failed", "failure_class": "code_fault",
                                   "coding_attempts": 0}) == "code_fault"
    assert graph.after_sit_triage({"automation_result": "failed", "failure_class": "code_fault",
                                   "coding_attempts": 2}) == "stop"
    assert graph.after_sit_triage({"automation_result": "failed", "failure_class": "could_not_verify",
                                   "coding_attempts": 0}) == "stop"
    # late-surfaced unsupported repo -> onboard (until the attempt budget is spent, then stop)
    assert graph.after_sit_triage({"automation_result": "failed", "failure_class": "could_not_verify",
                                   "needs_onboarding": True, "onboard_attempts": 0}) == "onboard"
    assert graph.after_sit_triage({"automation_result": "failed", "failure_class": "could_not_verify",
                                   "needs_onboarding": True,
                                   "onboard_attempts": config.MAX_ONBOARD_ATTEMPTS}) == "stop"


def test_after_rca_review():
    assert graph.after_rca_review({"rca_fix_needed": True}) == "fix_needed"
    assert graph.after_rca_review({"rca_fix_needed": False}) == "done"
    # Human rejected at the gate -> stop, regardless of what the RCA itself concluded.
    assert graph.after_rca_review({"rca_fix_needed": True, "rca_approval_decision": "reject"}) == "reject"
    assert graph.after_rca_review({"rca_fix_needed": False, "rca_approval_decision": "reject"}) == "reject"


# ----------------------------------------------------------------- full paths
def test_happy_path(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert final["ready_flipped"] is True
    assert final["worktree_dir"] == "/tmp/ws/ocean-worker"  # coder's clone threaded into state
    assert s.calls["sme_consult"] == 0   # no domain_bucket -> SME node is a no-op
    assert s.calls["coder"] == 1
    assert s.calls["harsh_reviewer"] == 1
    assert s.calls["sit_triage"] == 1
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
    assert s.calls["sit_triage"] == 2
    assert s.calls["coder"] == 2      # initial + one code_fault rework
    assert s.calls["open_pr"] == 1  # opened once; re-entry is a no-op


def test_code_fault_budget_exhausted(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["code_fault"])  # always code_fault
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "coding_attempts_exhausted" in final["final_outcome"]
    assert s.calls["sit_triage"] == config.MAX_CODING_ATTEMPTS + 1  # initial + N reworks


def test_environment_failure_retries_sit_run_only_then_passes(tmp_path, monkeypatch):
    """environment_failure re-enters at sit_run ONLY (prep_env_retry -> sit_run), unlike code_fault's
    full coder loop -- sit_resolve/sit_author/coder must NOT be re-called. Because the retry skips
    sit_resolve, script._cur (which sit_resolve's own mock call advances via next_sit()) never moves on
    its own for this loop, so this test drives the outcome directly off the sit_triage call count
    instead of reusing the Script/_cur convention the other loop tests rely on."""
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])   # sit_resolve's own outcome; irrelevant here
    _install(s, tmp_path, monkeypatch)
    real_fake_run_skill = agents.run_skill

    async def fake_run_skill_env_retry(**kw):
        if kw["node"] == "sit_triage":
            first_attempt = s.calls["sit_triage"] == 0
            s.calls["sit_triage"] += 1
            outcome = "environment_failure" if first_attempt else "passed"
            path = config.automation_verdict_path(kw["ticket_id"])
            path.write_text(json.dumps({
                "ticket_id": kw["ticket_id"], "pr_number": 123,
                "automation_result": "failed" if first_attempt else "passed",
                "failure_class": outcome if first_attempt else "",
                "execution_mode": "local-mock-first",
                "tests": [{"name": "test_x", "result": "failed" if first_attempt else "passed"}],
                "test_automation_pr_url": "" if first_attempt else "https://github.com/cloudqwest/test-automation/pull/9",
                "findings_for_coder": [], "needs_onboarding": False, "onboard_repo": "",
            }))
            return
        await real_fake_run_skill(**kw)

    monkeypatch.setattr(agents, "run_skill", fake_run_skill_env_retry)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["sit_triage"] == 2
    assert s.calls["sit_run"] == 2          # retried once
    assert s.calls["sit_resolve"] == 1      # NOT re-resolved -- retry skips straight to sit_run
    assert s.calls["sit_author"] == 1       # NOT re-authored -- the draft/review already happened
    assert s.calls["coder"] == 1            # NOT the code_fault loop -- this isn't a code problem


def test_environment_failure_budget_exhausted(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["environment_failure"])  # always environment_failure
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "environment_failure_retries_exhausted" in final["final_outcome"]
    assert s.calls["sit_triage"] == config.MAX_ENV_RETRY_ATTEMPTS + 1  # initial + N retries
    assert s.calls["sit_resolve"] == 1   # never re-resolved -- only sit_run repeats
    assert s.calls["coder"] == 1         # never the code_fault loop


def test_onboard_then_pass(tmp_path, monkeypatch):
    """Station 6 reports an unsupported repo -> graph onboards it (learn_repo) -> re-runs
    Station 6, which now passes -> ready flip. The onboarding decision + persistence is the
    graph's, not a hidden skill side-effect."""
    s = Script(review_seq=["APPROVE"], sit_seq=["needs_onboarding", "passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["learn_repo"] == 1                 # graph onboarded the repo exactly once
    assert s.calls["sit_resolve"] == 2         # gap attempt + re-run; triage only on the pass
    assert s.calls["sit_triage"] == 1
    assert final["repo_onboarded"] == "ocean-newrepo"
    assert s.calls["flip_ready"] == 1


def test_onboard_budget_exhausted(tmp_path, monkeypatch):
    """A repo that still reports unsupported after being onboarded ends as a clean stop
    (service PR left draft), capped by MAX_ONBOARD_ATTEMPTS — no infinite learn loop."""
    s = Script(review_seq=["APPROVE"], sit_seq=["needs_onboarding"])  # never becomes supported
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "repo_onboarding_exhausted" in final["final_outcome"]
    assert s.calls["learn_repo"] == config.MAX_ONBOARD_ATTEMPTS
    assert s.calls["sit_resolve"] == config.MAX_ONBOARD_ATTEMPTS + 1
    assert s.calls["sit_triage"] == 0
    assert s.calls["flip_ready"] == 0


def test_could_not_verify_stops(tmp_path, monkeypatch):
    s = Script(review_seq=["APPROVE"], sit_seq=["could_not_verify"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "could_not_verify" in final["final_outcome"]
    assert s.calls["sit_triage"] == 1        # no rework loop on could_not_verify
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


def test_rca_review_gate_interrupts_then_resume_approves(tmp_path, monkeypatch):
    """Default (RCA_REVIEW_AUTO off): the run pauses before acting on the RCA's own conclusion,
    then --approve resumes to whatever routing the RCA already decided (here: no fix needed)."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    s = Script(route="rca", rca_fix=False)
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "RCA_REVIEW_AUTO", False)   # override _install's default True
    app = graph.build_graph().compile(checkpointer=MemorySaver())
    thread = {"configurable": {"thread_id": "t-rca-approve"}, "recursion_limit": 100}
    asyncio.run(app.ainvoke(_initial(), config=thread))
    snap = asyncio.run(app.aget_state(thread))
    assert snap.next                      # paused at the interrupt
    assert s.calls["rca_agent"] == 1
    asyncio.run(app.ainvoke(Command(resume="approve"), config=thread))
    final = asyncio.run(app.aget_state(thread)).values
    assert final["final_status"] == "rca_report"


def test_rca_review_gate_reject_stops_before_coding(tmp_path, monkeypatch):
    """Reject at the RCA review gate stops the run even when the RCA itself found fix_needed --
    no code should get written off an RCA conclusion nobody has reviewed yet."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    s = Script(route="rca", rca_fix=True)
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "RCA_REVIEW_AUTO", False)   # override _install's default True
    app = graph.build_graph().compile(checkpointer=MemorySaver())
    thread = {"configurable": {"thread_id": "t-rca-reject"}, "recursion_limit": 100}
    asyncio.run(app.ainvoke(_initial(), config=thread))
    asyncio.run(app.ainvoke(Command(resume="reject"), config=thread))
    final = asyncio.run(app.aget_state(thread)).values
    assert final["final_status"] == "failed"
    assert "rejected" in final["final_outcome"]
    assert s.calls["dep_resolver"] == 0
    assert s.calls["coder"] == 0


def test_unsupported_route_stops(tmp_path, monkeypatch):
    s = Script(route="sop")
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "failed"
    assert "unsupported route" in final["final_outcome"]
    assert s.calls["coder"] == 0


def test_telemetry_noop_without_token(monkeypatch):
    """Telemetry is best-effort: with no RCA_TOKEN every call is a silent no-op and flush() is
    safe — a run must never block on or crash from aidev-db telemetry."""
    monkeypatch.setattr(telemetry, "RCA_TOKEN", "")
    telemetry._futures.clear()
    telemetry.execution_start("EXE-x", "MM-1", "coding")
    telemetry.station_event("EXE-x", 0, "start", route="coding")
    telemetry.execution_end("EXE-x", "MM-1", "completed", "coding", final_outcome="done")
    assert telemetry._futures == []   # nothing dispatched without a token
    telemetry.flush()                 # no-op, must not raise


def test_sme_consult_runs_for_known_bucket(tmp_path, monkeypatch):
    s = Script(sme_bucket="load_creation", review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["sme_consult"] == 1          # graph consulted the SME for a known bucket
    assert s.calls["dep_resolver"] == 1          # SME sits before the gates
    assert final["sme_findings"]["summary"] == "owner: ocean-worker"


# ----------------------------------------------------------------- ocean workers (MM-14620: single source in fk-aideveloper)
def test_ocean_workers_resolve():
    """The ocean coding workers live in fk-aideveloper's ocean-coding-agent (single source — the
    control plane holds NO worker content, MM-14620 Q1). They resolve from OCEAN_WORKERS_DIR; the
    ocean SMEs still resolve from the fk-aideveloper station dir. Skips if fk-aideveloper isn't
    checked out (the workers no longer live in this repo)."""
    if not config.OCEAN_WORKERS_DIR.exists():
        pytest.skip("fk-aideveloper ocean-coding-agent/workers not checked out")
    for name in ("research.md", "code.md", "review.md",
                 "dep-resolve.md", "reachability.md", "rca-research.md"):
        p = agents._agent_path(name)
        assert p == config.OCEAN_WORKERS_DIR / name and p.exists(), f"{name} not in ocean-coding-agent/workers"
        assert "Not your job" in p.read_text(), f"{name} missing the process-ownership boundary"
    # ocean SME agents resolve to the ocean-coding-agent agents dir (MM-14620: ocean-introduced ->
    # ocean home, no longer in the generic agents/pipeline)
    assert agents._agent_path("sme-load-creation.md") == config.OCEAN_AGENTS_DIR / "sme-load-creation.md"


def test_run_agent_loads_ocean_worker(tmp_path, monkeypatch):
    """run_agent must actually LOAD the ocean worker prompt from fk-aideveloper's ocean-coding-agent
    (regression guard for the resolver). Skips if fk-aideveloper isn't checked out."""
    if not config.OCEAN_WORKERS_DIR.exists():
        pytest.skip("fk-aideveloper ocean-coding-agent/workers not checked out")
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    captured: dict = {}

    async def fake_drive(**kw):
        captured["system_prompt"] = kw["system_prompt"]
        (config.artifacts_dir("EXE-x") / "researcher.verdict.json").write_text(
            json.dumps({"route": "coding", "packet_path": "/tmp/p.json", "target_repos": []}))

    monkeypatch.setattr(agents, "_drive_with_retry", fake_drive)
    v = asyncio.run(agents.run_agent(
        agent_md="research.md", node="researcher", ticket_id="MM-1", execution_id="EXE-x",
        task_prompt="go", verdict_model=schemas.ResearchVerdict))
    assert v.route == "coding"
    assert "Not your job" in captured["system_prompt"]  # proves it loaded workers/research.md


def test_ensure_verdict_tool_allowed():
    """Pure unit test of the Write-tool union: run_agent's VERDICT_INSTRUCTION contract always
    requires Write, regardless of what a worker's own tools: frontmatter declares."""
    assert agents._ensure_verdict_tool_allowed(None) is None
    assert agents._ensure_verdict_tool_allowed(["Read", "Bash"]) == ["Read", "Bash", "Write"]
    assert agents._ensure_verdict_tool_allowed(["Read", "Write"]) == ["Read", "Write"]


def test_run_agent_allows_write_for_a_write_less_sme_file(tmp_path, monkeypatch):
    """Regression for the PreToolUse allowlist bug: a worker file (like every real
    agents/pipeline/sme-*.md) that declares tools: without Write must still be able to satisfy
    run_agent's own mandatory verdict-write contract. Captures the allowed_tools run_agent
    actually passes down to _drive_with_retry (the layer the fix operates at) rather than
    mocking it away, so a regression that re-narrows the allowlist would be caught here."""
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    sme_dir = tmp_path / "agents_pipeline"
    sme_dir.mkdir()
    sme_file = sme_dir / "sme-no-write.md"
    sme_file.write_text(
        '---\nname: sme-no-write\ntools: ["Read", "Grep", "Glob", "Bash"]\n---\n\n# SME\n'
    )
    monkeypatch.setattr(agents, "_agent_path", lambda agent_md: sme_file)
    captured: dict = {}

    async def fake_drive(**kw):
        captured["allowed_tools"] = kw["allowed_tools"]
        (config.artifacts_dir("EXE-y") / "sme_consult.verdict.json").write_text(
            json.dumps({"findings": []}))

    monkeypatch.setattr(agents, "_drive_with_retry", fake_drive)
    asyncio.run(agents.run_agent(
        agent_md="sme-no-write.md", node="sme_consult", ticket_id="MM-1", execution_id="EXE-y",
        task_prompt="go", verdict_model=schemas.SmeVerdict))
    assert "Write" in captured["allowed_tools"], (
        "run_agent must guarantee Write is allowed even when the worker's own frontmatter omits "
        "it, since VERDICT_INSTRUCTION always requires writing the verdict file"
    )
    assert set(captured["allowed_tools"]) == {"Read", "Grep", "Glob", "Bash", "Write"}, (
        "the file's OTHER declared restrictions must still be preserved — only Write is added"
    )


# ----------------------------------------------------------------- language-scoped Docker rule drift guard
def test_language_scoped_docker_rule_present():
    """Generic drift guard — NOT a hardcoded repo list. Every worker prompt that builds/tests an ocean
    repo, plus AGENT_GUARDRAILS, must carry the LANGUAGE-scoped 'Ruby -> Docker only' rule in-context, so
    ANY Ruby repo — including a NEW one named nowhere here — is run in Docker, never native (native fails
    on old gems). Keyed on the rule (by language), not on which repos exist, so a new Ruby repo needs no
    change here; the test fails loudly only if a copy silently LOSES the rule (the native-run regression).
    The copies are kept in-context deliberately (a reference the worker might not read would reintroduce
    the bug) — this guard is what keeps them from drifting apart. AGENT_GUARDRAILS (this repo) is always
    checked; the ocean workers now live in fk-aideveloper's ocean-coding-agent, so they're checked only
    when that checkout is present."""
    low = agents.AGENT_GUARDRAILS.lower()
    assert "ruby" in low and "docker" in low, (
        "AGENT_GUARDRAILS lost the language-scoped Ruby->Docker rule — a Ruby repo could be run native.")
    if not config.OCEAN_WORKERS_DIR.exists():
        pytest.skip("fk-aideveloper ocean-coding-agent/workers not checked out")
    for name in ("research.md", "code.md", "review.md", "reachability.md"):
        low = (config.OCEAN_WORKERS_DIR / name).read_text().lower()
        assert "ruby" in low and "docker" in low, (
            f"{name} lost the language-scoped Ruby->Docker build/test rule — a Ruby repo (incl. a new "
            f"one) could be run natively and fail on old gems. Re-add it BY LANGUAGE (Ruby -> Docker), "
            f"never as an enumerated repo list.")


# ----------------------------------------------------------------- G2: fail-loud worker resolution
def test_agent_path_raises_clear_error_when_missing(tmp_path, monkeypatch):
    """G2: a worker prompt that exists in NEITHER the ocean-coding-agent workers dir nor the
    fk-aideveloper station dir must raise a clear StationError naming both locations — not silently
    return a nonexistent path that later dies as a bare FileNotFoundError inside _read."""
    (tmp_path / "workers").mkdir()
    (tmp_path / "agents").mkdir()
    monkeypatch.setattr(config, "OCEAN_WORKERS_DIR", tmp_path / "workers")
    monkeypatch.setattr(config, "AGENTS_DIR", tmp_path / "agents")
    with pytest.raises(agents.StationError) as ei:
        agents._agent_path("does-not-exist.md")
    msg = str(ei.value)
    assert "does-not-exist.md" in msg
    assert "workers" in msg and "agents" in msg


# ----------------------------------------------------------------- G7: node-level retry recovery
def test_drive_with_retry_recovers_from_transient_failure(monkeypatch):
    """G7: a single transient _drive failure must be retried and RECOVERED (not propagated) —
    the node-level resilience config.MAX_AGENT_RETRIES promises. Previously no test proved recovery,
    only that the retry code existed."""
    monkeypatch.setattr(config, "MAX_AGENT_RETRIES", 2)
    monkeypatch.setattr(config, "AGENT_RETRY_BACKOFF_SECONDS", 0)  # no real sleep
    calls = {"n": 0}

    async def flaky_drive(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient claude-CLI error")
        return  # second attempt succeeds

    monkeypatch.setattr(agents, "_drive", flaky_drive)
    asyncio.run(agents._drive_with_retry(
        system_prompt="sp", prompt="p", cwd=Path("/tmp"),
        permission_mode="bypassPermissions", label="test"))
    assert calls["n"] == 2   # failed once, recovered on the retry — did NOT propagate


def test_drive_with_retry_propagates_after_budget(monkeypatch):
    """The flip side of G7: once MAX_AGENT_RETRIES is exhausted the last error propagates, so the
    caller (run_agent/run_skill) can normalize it to a StationError rather than it being swallowed."""
    monkeypatch.setattr(config, "MAX_AGENT_RETRIES", 1)
    monkeypatch.setattr(config, "AGENT_RETRY_BACKOFF_SECONDS", 0)

    async def always_fails(*a, **kw):
        raise RuntimeError("persistent failure")

    monkeypatch.setattr(agents, "_drive", always_fails)
    with pytest.raises(RuntimeError, match="persistent failure"):
        asyncio.run(agents._drive_with_retry(
            system_prompt="sp", prompt="p", cwd=Path("/tmp"),
            permission_mode="bypassPermissions", label="test"))


# ----------------------------------------------------------------- A3: RCA report posted by plain code
def test_rca_report_posts_worker_report_via_jira(tmp_path, monkeypatch):
    """A3: the RCA report is posted by the plain-code rca_report node via jira.py — NOT by the worker
    via the Atlassian MCP. rca_report reads the worker-written report file and posts exactly one
    comment; a missing file or no token is a silent no-op."""
    posted = []
    monkeypatch.setattr(jira, "comment", lambda tid, body: posted.append((tid, body)))
    monkeypatch.setattr(telemetry, "station_event", lambda *a, **kw: None)

    report_file = tmp_path / "rca.md"
    report_file.write_text("## Root cause\nX broke Y.")
    out = asyncio.run(nodes.rca_report(
        {"ticket_id": "MM-1", "execution_id": "EXE-x", "rca_report_path": str(report_file)}))
    assert out == {}
    assert len(posted) == 1                      # exactly one comment
    assert posted[0][0] == "MM-1"
    assert "Root cause" in posted[0][1] and posted[0][1].startswith("🤖 Aquaman Ocean RCA")

    # missing report file -> no post (best-effort)
    posted.clear()
    asyncio.run(nodes.rca_report(
        {"ticket_id": "MM-1", "execution_id": "EXE-x", "rca_report_path": str(tmp_path / "nope.md")}))
    assert posted == []


# ----------------------------------------------------------------- human-approval gate (Phase C)
def _initial(ticket="MM-1", exe="EXE-test"):
    return {"ticket_id": ticket, "execution_id": exe, "profile": "isbu", "context": "",
            "review_iteration": 0, "review_findings": [], "coding_attempts": 0, "sit_findings": []}


def test_human_gate_passthrough_when_off(tmp_path, monkeypatch):
    """Default (approval off): the gate is a no-op and the run auto-flips on green."""
    monkeypatch.setattr(config, "REQUIRE_APPROVAL", False)
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["flip_ready"] == 1


def test_human_gate_interrupts_then_resume_approves(tmp_path, monkeypatch):
    """Approval on: the run pauses at the gate (no flip), then --approve resumes to the flip."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    monkeypatch.setattr(config, "REQUIRE_APPROVAL", True)
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    app = graph.build_graph().compile(checkpointer=MemorySaver())
    thread = {"configurable": {"thread_id": "t-approve"}, "recursion_limit": 100}
    asyncio.run(app.ainvoke(_initial(), config=thread))
    snap = asyncio.run(app.aget_state(thread))
    assert snap.next                      # paused at the interrupt
    assert s.calls["flip_ready"] == 0     # not flipped yet
    asyncio.run(app.ainvoke(Command(resume="approve"), config=thread))
    final = asyncio.run(app.aget_state(thread)).values
    assert final["final_status"] == "completed"
    assert s.calls["flip_ready"] == 1


def test_human_gate_reject_stops(tmp_path, monkeypatch):
    """Approval on + --reject: the PR is left draft, never flipped."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    monkeypatch.setattr(config, "REQUIRE_APPROVAL", True)
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    app = graph.build_graph().compile(checkpointer=MemorySaver())
    thread = {"configurable": {"thread_id": "t-reject"}, "recursion_limit": 100}
    asyncio.run(app.ainvoke(_initial(), config=thread))
    asyncio.run(app.ainvoke(Command(resume="reject"), config=thread))
    final = asyncio.run(app.aget_state(thread)).values
    assert final["final_status"] == "failed"
    assert "rejected" in final["final_outcome"]
    assert s.calls["flip_ready"] == 0


def test_qa_gate_auto_with_testrail(tmp_path, monkeypatch):
    """Auto mode + TestRail on: the gate auto-approves with TestRail; sit_testrail runs (in parallel
    with sit_run) and its run id lands in the report."""
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "QA_TESTRAIL", True)   # override _install default
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["sit_author"] == 1
    assert s.calls["sit_run"] == 1
    assert s.calls["sit_testrail"] == 1
    assert final["sit_report"]["testrail_run_id"] == 555


def test_qa_gate_auto_no_testrail(tmp_path, monkeypatch):
    """Auto mode, TestRail off (the demo default): draft → auto-approve → run, no TestRail branch."""
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)   # QA_REVIEW_AUTO=True, QA_TESTRAIL=False
    final = _run()
    assert final["final_status"] == "completed"
    assert s.calls["sit_author"] == 1
    assert s.calls["sit_testrail"] == 0


def test_qa_gate_human_approve(tmp_path, monkeypatch):
    """Gate ON: the run pauses at the drafted SIT for a human; --qa approve-no-testrail proceeds to run."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "QA_REVIEW_AUTO", False)   # gate ON (default in prod)
    app = graph.build_graph().compile(checkpointer=MemorySaver())
    thread = {"configurable": {"thread_id": "t-qa"}, "recursion_limit": 100}
    asyncio.run(app.ainvoke(_initial(), config=thread))
    assert asyncio.run(app.aget_state(thread)).next   # paused at qa_review_gate
    assert s.calls["sit_run"] == 0                     # nothing ran before approval
    asyncio.run(app.ainvoke(Command(resume={"decision": "approve_no_testrail"}), config=thread))
    final = asyncio.run(app.aget_state(thread)).values
    assert final["final_status"] == "completed"
    assert s.calls["sit_testrail"] == 0
    assert s.calls["flip_ready"] == 1


def test_qa_gate_changes_then_approve(tmp_path, monkeypatch):
    """Gate ON: 'changes' loops back to redraft (sit_author runs again), then approve proceeds."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(config, "QA_REVIEW_AUTO", False)
    app = graph.build_graph().compile(checkpointer=MemorySaver())
    thread = {"configurable": {"thread_id": "t-qa-changes"}, "recursion_limit": 100}
    asyncio.run(app.ainvoke(_initial(), config=thread))
    asyncio.run(app.ainvoke(Command(resume={"decision": "changes", "note": "cover null-SCAC"}), config=thread))
    assert asyncio.run(app.aget_state(thread)).next   # redrafted, paused again
    assert s.calls["sit_author"] == 2                  # initial draft + redraft
    asyncio.run(app.ainvoke(Command(resume={"decision": "approve_no_testrail"}), config=thread))
    final = asyncio.run(app.aget_state(thread)).values
    assert final["final_status"] == "completed"


def test_jira_noop_without_token(monkeypatch):
    """Jira lifecycle updates are best-effort: no JIRA_API_TOKEN -> silent no-op, never raises."""
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "")
    jira.transition("MM-1", "In Progress")   # must not raise or make a request
    jira.comment("MM-1", "hi")


# ----------------------------------------------------------------- resource pre-flight (Workstream 3.4)
def test_docker_preflight_reason_pure(monkeypatch):
    """_docker_preflight_reason is a pure wrapper over _docker_resources — test it directly, no Docker
    needed. Empty string = OK; non-empty = a ready-to-use could_not_verify reason."""
    monkeypatch.setattr(nodes, "_docker_resources", lambda: None)
    assert "not running" in nodes._docker_preflight_reason()

    monkeypatch.setattr(config, "MIN_DOCKER_MEMORY_GB", 8.0)
    monkeypatch.setattr(config, "MIN_DOCKER_CPUS", 4)
    monkeypatch.setattr(nodes, "_docker_resources", lambda: (2.0, 2))
    reason = nodes._docker_preflight_reason()
    assert "insufficient_docker_resources" in reason and "2.0 GB" in reason

    monkeypatch.setattr(nodes, "_docker_resources", lambda: (16.0, 8))
    assert nodes._docker_preflight_reason() == ""


def test_sit_run_preflight_short_circuit_skips_agent_calls(tmp_path, monkeypatch):
    """When Docker resources are insufficient, sit_run must fail BEFORE calling the (expensive) agent —
    MM-13437's real 40-minute OOM attempt is exactly the cost this gate exists to avoid — and sit_triage
    must recognize the marker and skip its own redundant agent call too. Zero calls to either node's
    agents.run_skill proves both money-saving properties, not just the eventual failed verdict."""
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])   # would pass if it ever ran
    _install(s, tmp_path, monkeypatch)
    monkeypatch.setattr(nodes, "_docker_preflight_reason",
                        lambda *a, **kw: "environment_failure: insufficient_docker_resources — have 2.0 GB / 2 CPU")
    final = _run()
    assert final["final_status"] == "failed"
    # environment_failure (not could_not_verify) -- this IS a harness/infra limit, but the
    # deterministic resource-insufficient case specifically never auto-retries (more Docker
    # memory doesn't appear between attempts), so stop_run reports it non-retriable.
    assert "environment_failure_non_retriable" in final["final_outcome"]
    assert s.calls["sit_run"] == 0        # short-circuited before the agent call
    assert s.calls["sit_triage"] == 0     # recognized preflight_failed in typed state, skipped its own agent call
    assert s.calls["flip_ready"] == 0


def test_ac_coverage_passthrough(tmp_path, monkeypatch):
    """ac_coverage is additive/optional on the verdict — when the skill emits it, it must reach the
    final sit_report untouched (frozen-contract discipline: add, never rename/drop)."""
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)
    ac_rows = [{"ac": "AC3", "test": "test_x", "result": "passed"}]

    real_fake_run_skill = agents.run_skill

    async def fake_run_skill_with_ac(**kw):
        await real_fake_run_skill(**kw)
        if kw["node"] == "sit_triage":
            path = config.automation_verdict_path(kw["ticket_id"])
            data = json.loads(path.read_text())
            data["ac_coverage"] = ac_rows
            path.write_text(json.dumps(data))

    monkeypatch.setattr(agents, "run_skill", fake_run_skill_with_ac)
    final = _run()
    assert final["final_status"] == "completed"
    assert final["sit_report"]["ac_coverage"] == ac_rows


# ----------------------------------------------------------------- cli.py resume ticket_id recovery
def test_resume_recovers_real_ticket_id_from_checkpoint(tmp_path, monkeypatch):
    """Regression: _resume used to hardcode ticket_id="" when calling _execute, even though the
    checkpoint has held the real value all along — blanking the Ticket field in run-report.md on
    every resume, and telemetrizing a failed resume with an empty ticket_id. _resume_ticket_id must
    recover the real value from the persisted graph state before _execute runs."""
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    monkeypatch.setattr(config, "CHECKPOINT_DB", str(tmp_path / "checkpoints.sqlite"))
    monkeypatch.setattr(config, "REQUIRE_APPROVAL", True)
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)

    thread = {"configurable": {"thread_id": "EXE-resume-test"}, "recursion_limit": 100}

    async def _pause_at_gate():
        async with AsyncSqliteSaver.from_conn_string(config.CHECKPOINT_DB) as saver:
            app = graph.compile_app(saver)
            await app.ainvoke(_initial(ticket="MM-9999", exe="EXE-resume-test"), config=thread)

    asyncio.run(_pause_at_gate())

    recovered = asyncio.run(cli._resume_ticket_id("EXE-resume-test", thread))
    assert recovered == "MM-9999"


def test_resume_ticket_id_best_effort_on_missing_checkpoint(tmp_path, monkeypatch):
    """A resume against an unknown/corrupt execution_id must not raise here — _execute's own error
    handling is the right place for that to surface, not the ticket_id recovery helper."""
    monkeypatch.setattr(config, "CHECKPOINT_DB", str(tmp_path / "checkpoints.sqlite"))
    thread = {"configurable": {"thread_id": "EXE-does-not-exist"}, "recursion_limit": 100}
    recovered = asyncio.run(cli._resume_ticket_id("EXE-does-not-exist", thread))
    assert recovered == ""


def test_resume_passes_recovered_ticket_id_to_execute(tmp_path, monkeypatch):
    """End-to-end regression: _resume itself (not just the helper in isolation) must pass the
    recovered ticket_id through to _execute — catches the class of bug where the helper exists but
    the call site still passes "" directly. Mocks _execute (and _preflight, so the test doesn't
    need FK_AIDEVELOPER_DIR/`claude` on PATH) to capture exactly what it was called with."""
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    monkeypatch.setattr(config, "CHECKPOINT_DB", str(tmp_path / "checkpoints.sqlite"))
    monkeypatch.setattr(config, "REQUIRE_APPROVAL", True)
    s = Script(review_seq=["APPROVE"], sit_seq=["passed"])
    _install(s, tmp_path, monkeypatch)

    thread = {"configurable": {"thread_id": "EXE-resume-e2e"}, "recursion_limit": 100}

    async def _pause_at_gate():
        async with AsyncSqliteSaver.from_conn_string(config.CHECKPOINT_DB) as saver:
            app = graph.compile_app(saver)
            await app.ainvoke(_initial(ticket="MM-8888", exe="EXE-resume-e2e"), config=thread)

    asyncio.run(_pause_at_gate())

    captured: dict = {}

    async def fake_execute(execution_id, ticket_id, initial, thread):
        captured["ticket_id"] = ticket_id

    monkeypatch.setattr(cli, "_preflight", lambda: None)
    monkeypatch.setattr(cli, "_execute", fake_execute)
    asyncio.run(cli._resume("EXE-resume-e2e", resume_value="approve"))
    assert captured["ticket_id"] == "MM-8888"


# ----------------------------------------------------------------- gitops.py _gh() timeout
def test_gh_raises_git_op_error_on_timeout(monkeypatch):
    """Regression: a hung/stalled `gh` call used to block its node (and the whole run) forever —
    no subprocess timeout at all. A TimeoutExpired must now surface as a clean GitOpError instead
    of an uncaught exception, so the caller's existing except-and-report handling covers it."""
    def fake_run(*args, **kwargs):
        assert kwargs.get("timeout") == config.GH_TIMEOUT_SECONDS
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(gitops.GitOpError, match="timed out"):
        gitops._gh(["pr", "list"])


def test_gh_still_raises_on_nonzero_exit(monkeypatch):
    """The pre-existing exit-code-check behavior must survive the timeout-handling refactor."""
    class FakeProc:
        returncode = 1
        stdout = ""
        stderr = "not found"

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    with pytest.raises(gitops.GitOpError, match="not found"):
        gitops._gh(["pr", "list"])


# ----------------------------------------------------------------- cli.py _preflight gh check
def _preflight_ready_dirs(tmp_path, monkeypatch):
    """Make the non-gh preflight checks pass so a test can isolate the gh-specific behavior."""
    agents_dir = tmp_path / "fk-aideveloper" / "agents" / "pipeline"
    agents_dir.mkdir(parents=True)
    # G1 version-pin guard: preflight now requires the 4 ocean SME files (under ocean-coding-agent/
    # agents) AND the 6 ocean-coding-agent workers to exist on the checked-out branch — seed both.
    oca = tmp_path / "fk-aideveloper" / "skills" / "ocean-coding-agent"
    agents_home = oca / "agents"; agents_home.mkdir(parents=True)
    for f in ("sme-callback-notification.md", "sme-load-creation.md",
              "sme-ocean-milestones.md", "sme-ocean-data-quality.md"):
        (agents_home / f).write_text("# stub SME\n")
    workers_dir = oca / "workers"; workers_dir.mkdir(parents=True)
    for f in ("research.md", "dep-resolve.md", "reachability.md", "code.md", "review.md", "rca-research.md"):
        (workers_dir / f).write_text("# stub worker\n")
    monkeypatch.setattr(config, "FK_AIDEVELOPER_DIR", tmp_path / "fk-aideveloper")
    monkeypatch.setattr(config, "AGENTS_DIR", agents_dir)
    monkeypatch.setattr(config, "OCEAN_WORKERS_DIR", workers_dir)
    monkeypatch.setattr(config, "OCEAN_AGENTS_DIR", agents_home)


def test_preflight_fails_when_sme_files_missing(tmp_path, monkeypatch):
    """G1: preflight must catch the README's 'checkout state, not just presence' trap — a
    FK_AIDEVELOPER_DIR on a branch missing the ocean SME files (origin/main doesn't carry them)
    must fail upfront with the #fk-aideveloper Slack hint, not deep inside sme_consult."""
    _preflight_ready_dirs(tmp_path, monkeypatch)
    (config.OCEAN_AGENTS_DIR / "sme-load-creation.md").unlink()   # simulate the wrong branch
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")

    class FakeProc:
        returncode = 0
        stdout = "Logged in"
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    with pytest.raises(SystemExit, match="sme-load-creation.md"):
        cli._preflight()


def test_preflight_fails_when_ocean_workers_missing(tmp_path, monkeypatch):
    """MM-14620: the ocean coding workers now live in fk-aideveloper's ocean-coding-agent; a checkout
    without them must fail preflight upfront (they'd otherwise fail deep at the first agent node)."""
    _preflight_ready_dirs(tmp_path, monkeypatch)
    (config.OCEAN_WORKERS_DIR / "code.md").unlink()   # simulate a branch without ocean-coding-agent
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")

    class FakeProc:
        returncode = 0
        stdout = "Logged in"
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    with pytest.raises(SystemExit, match="code.md"):
        cli._preflight()


def test_preflight_fails_when_gh_missing(tmp_path, monkeypatch):
    """gitops.py's own docstring says preflight checks `gh auth` — this is that check. Regression:
    _preflight previously never looked for `gh` at all, so a missing/unauthenticated `gh` surfaced
    as a raw GitOpError deep inside open_pr/flip_ready instead of a clean upfront message."""
    _preflight_ready_dirs(tmp_path, monkeypatch)
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/claude" if name == "claude" else None)
    with pytest.raises(SystemExit, match="gh.*not on PATH"):
        cli._preflight()


def test_preflight_fails_when_gh_not_authenticated(tmp_path, monkeypatch):
    _preflight_ready_dirs(tmp_path, monkeypatch)
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")

    class FakeProc:
        returncode = 1
        stdout = ""
        stderr = "You are not logged into any GitHub hosts"

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    with pytest.raises(SystemExit, match="gh auth login"):
        cli._preflight()


def test_preflight_passes_when_gh_authenticated(tmp_path, monkeypatch):
    _preflight_ready_dirs(tmp_path, monkeypatch)
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")

    class FakeProc:
        returncode = 0
        stdout = "Logged in to github.com"
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    cli._preflight()  # must not raise


# ----------------------------------------------------------------- cli.py _pause_message
def test_pause_message_rca_review_gate_suggests_approve_reject():
    """rca_review_gate shares --approve/--reject with human_gate (both a plain 2-way choice) --
    safe because they can never both be the pending interrupt at once (the RCA gate resolves
    long before the run could reach dep_resolver/coder/.../human_gate). Regression coverage for
    this branch specifically, since it was added without its own test the first time around."""
    msg = cli._pause_message("EXE-abc123", ("rca_review_gate",))
    assert "--resume EXE-abc123 --approve" in msg
    assert "--resume EXE-abc123 --reject" in msg
    assert "--qa" not in msg


def test_pause_message_qa_review_gate_suggests_qa_flag_not_approve():
    """Regression: the [PAUSED] message used to hardcode 'awaiting human approval ... --approve/
    --reject' regardless of which interrupt() gate actually paused. qa_review_gate pauses by
    DEFAULT (human_gate only pauses if OCEAN_PIPELINE_REQUIRE_APPROVAL is set), so that generic
    message pointed at the wrong flags in the common case — --approve isn't a decision
    after_qa_review recognizes, so it silently fell through to that function's default branch
    instead of doing what the user actually asked for."""
    msg = cli._pause_message("EXE-abc123", ("qa_review_gate",))
    assert "--qa approve-testrail" in msg
    assert "--qa approve-no-testrail" in msg
    assert "--qa changes" in msg
    assert "--approve" not in msg.replace("approve-testrail", "").replace("approve-no-testrail", "")
    assert "EXE-abc123" in msg


def test_pause_message_human_gate_suggests_approve_reject():
    msg = cli._pause_message("EXE-abc123", ("human_gate",))
    assert "--resume EXE-abc123 --approve" in msg
    assert "--resume EXE-abc123 --reject" in msg
    assert "--qa" not in msg


def test_pause_message_unknown_gate_does_not_claim_a_known_gate():
    """An interrupt() gate added later without an entry in _pause_message must surface its
    real node name rather than silently reusing qa_review_gate's or human_gate's flags."""
    msg = cli._pause_message("EXE-abc123", ("some_new_gate",))
    assert "some_new_gate" in msg
    assert "--qa" not in msg
    assert "--approve" not in msg


# ----------------------------------------------------------------- telemetry.py _STATUS mapping
def test_learn_repo_phases_report_correct_status(monkeypatch):
    """Regression: learn_repo_start/learn_repo_end had no entry in _STATUS, so the fallback
    silently mapped BOTH to "completed" -- a learn_repo run still in progress (the start event)
    reported as already done on the aidev-db dashboard."""
    captured = []
    monkeypatch.setattr(telemetry, "_dispatch", lambda tool, args: captured.append(args))
    telemetry.station_event("EXE-x", 5.95, "learn_repo_start", onboard_repo="r", attempt=1)
    telemetry.station_event("EXE-x", 5.95, "learn_repo_end", onboard_repo="r", attempt=1)
    assert captured[0]["status"] == "started"
    assert captured[1]["status"] == "completed"


# ----------------------------------------------------------------- agents.py _frontmatter_tools
def test_frontmatter_tools_parses_inline_json_array(tmp_path):
    p = tmp_path / "w.md"
    p.write_text('---\nname: x\ntools: ["Read", "Write", "Bash"]\n---\n\n# X\n')
    assert agents._frontmatter_tools(p) == ["Read", "Write", "Bash"]


def test_frontmatter_tools_parses_yaml_block_list(tmp_path):
    """Regression: the parser used to ONLY match single-line tools: [...] syntax. A file using
    standard multi-line YAML block-list style would silently return None (no restriction) with
    nothing catching the regression. Must now parse this style directly."""
    p = tmp_path / "w.md"
    p.write_text("---\nname: x\ntools:\n  - Read\n  - Write\n  - Bash\n---\n\n# X\n")
    assert agents._frontmatter_tools(p) == ["Read", "Write", "Bash"]


def test_frontmatter_tools_raises_on_unparseable_tools_key(tmp_path):
    """A tools: key that exists but parses as neither style must fail loudly, not silently return
    None (which would make the file LOOK unrestricted while its author believed it was scoped)."""
    p = tmp_path / "w.md"
    p.write_text("---\nname: x\ntools: not_a_list_or_json\n---\n\n# X\n")
    with pytest.raises(ValueError, match="tools:"):
        agents._frontmatter_tools(p)


def test_frontmatter_tools_returns_none_when_no_tools_key(tmp_path):
    """No tools: key at all is the legitimate "no restriction" case — must stay None, not raise."""
    p = tmp_path / "w.md"
    p.write_text("---\nname: x\ndescription: something\n---\n\n# X\n")
    assert agents._frontmatter_tools(p) is None


# ----------------------------------------------------------------- dep_resolver blocking parity
def test_dep_resolver_surfaces_its_own_blocking_claim(monkeypatch):
    """Regression: dep_resolver and reachability_gate both use ReachabilityVerdict, but
    dep_resolver silently dropped v.blocking from its return dict while reachability_gate
    surfaced it as reachability_blocking -- same schema, inconsistent treatment. dep_resolver
    must surface its own claim the same way, as dependency_blocking."""
    async def fake_run_agent(**kw):
        return schemas.DependencyVerdict(report_path="/tmp/x.json", blocking=True, notes="blocked on X")

    monkeypatch.setattr(agents, "run_agent", fake_run_agent)
    result = asyncio.run(nodes.dep_resolver({"ticket_id": "MM-1", "execution_id": "EXE-x"}))
    assert result["dependency_blocking"] is True
    assert result["dependency_report"]["notes"] == "blocked on X"


def test_ui_highlight_shows_dep_resolver_blocking():
    assert ui._highlight("dep_resolver", {"dependency_blocking": True,
                                          "dependency_report": {"notes": "blocked on X"}}) \
        == "BLOCKING: blocked on X"
    assert ui._highlight("dep_resolver", {"dependency_blocking": False,
                                          "dependency_report": {"notes": ""}}) == "no blockers"


# ----------------------------------------------------------------- gitops.py real logic
class _FakeGhProc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_find_pr_for_branch_returns_none_when_no_pr(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _FakeGhProc(stdout="[]"))
    assert gitops.find_pr_for_branch("org/repo", "MM-1/b") is None


def test_find_pr_for_branch_returns_existing_number(monkeypatch):
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: _FakeGhProc(stdout='[{"number": 42, "state": "OPEN"}]'))
    assert gitops.find_pr_for_branch("org/repo", "MM-1/b") == 42


def test_open_draft_pr_is_idempotent_reuses_existing(monkeypatch):
    """If a PR already exists for the branch, open_draft_pr must return it WITHOUT calling
    `gh pr create` again — the idempotency guard its docstring promises (a rework loop or a
    re-run must never open a second PR for the same branch)."""
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return _FakeGhProc(stdout='[{"number": 7, "state": "OPEN"}]')

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = gitops.open_draft_pr("org/repo", "MM-1/b", "title", "body")
    assert result == 7
    assert len(calls) == 1
    assert "create" not in calls[0]


def test_open_draft_pr_creates_when_none_exists(monkeypatch):
    responses = iter([
        _FakeGhProc(stdout="[]"),                                # 1st find: no existing PR
        _FakeGhProc(stdout=""),                                  # gh pr create (stdout unused)
        _FakeGhProc(stdout='[{"number": 9, "state": "OPEN"}]'),  # 2nd find: the new PR
    ])
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: next(responses))
    assert gitops.open_draft_pr("org/repo", "MM-1/b", "title", "body") == 9


def test_open_draft_pr_raises_if_number_unreadable_after_create(monkeypatch):
    responses = iter([
        _FakeGhProc(stdout="[]"),
        _FakeGhProc(stdout=""),
        _FakeGhProc(stdout="[]"),   # still nothing after create -- real failure
    ])
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: next(responses))
    with pytest.raises(gitops.GitOpError, match="could not read its number back"):
        gitops.open_draft_pr("org/repo", "MM-1/b", "title", "body")


def test_cross_link_and_ready_adds_link_when_missing(monkeypatch):
    calls = []
    link = "https://github.com/x/test-automation/pull/1"

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return _FakeGhProc(stdout="original body") if "view" in cmd else _FakeGhProc(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    gitops.cross_link_and_ready("org/repo", 5, link)
    edit_calls = [c for c in calls if "edit" in c]
    assert len(edit_calls) == 1
    assert any(link in arg for arg in edit_calls[0])
    assert any("ready" in c for c in calls)


def test_cross_link_and_ready_skips_edit_when_link_already_present(monkeypatch):
    """Idempotent: re-running against a service PR that already carries the link must not
    append a duplicate."""
    calls = []
    link = "https://github.com/x/test-automation/pull/1"

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return _FakeGhProc(stdout=f"original body\n{link}") if "view" in cmd else _FakeGhProc(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    gitops.cross_link_and_ready("org/repo", 5, link)
    assert not any("edit" in c for c in calls)
    assert any("ready" in c for c in calls)


def test_cross_link_and_ready_no_test_pr_url_just_flips_ready(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: (calls.append(cmd), _FakeGhProc(stdout=""))[1])
    gitops.cross_link_and_ready("org/repo", 5)
    assert not any("view" in c for c in calls)
    assert not any("edit" in c for c in calls)
    assert any("ready" in c for c in calls)


# ----------------------------------------------------------------- jira.py real logic
class _FakeJiraResponse:
    def __init__(self, body: str):
        self._body = body.encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_jira_transition_finds_matching_and_posts(monkeypatch):
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req.get_method())
        if req.get_method() == "GET":
            return _FakeJiraResponse(json.dumps({"transitions": [
                {"id": "31", "name": "In Progress"}, {"id": "41", "name": "Done"},
            ]}))
        return _FakeJiraResponse("")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    jira.transition("MM-1", "in progress")
    assert calls == ["GET", "POST"]


def test_jira_transition_no_match_skips_post(monkeypatch):
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req.get_method())
        return _FakeJiraResponse(json.dumps({"transitions": [{"id": "1", "name": "Backlog"}]}))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    jira.transition("MM-1", "in progress")
    assert calls == ["GET"]   # no matching transition -> no POST


def test_jira_transition_swallows_errors(monkeypatch):
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")

    def fake_urlopen(req, timeout=None):
        raise OSError("network down")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    jira.transition("MM-1", "in progress")  # must not raise


def test_jira_comment_posts_body(monkeypatch):
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append((req.get_method(), req.data))
        return _FakeJiraResponse("")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    jira.comment("MM-1", "hello world")
    assert len(calls) == 1
    method, data = calls[0]
    assert method == "POST"
    assert json.loads(data.decode())["body"] == "hello world"


def test_jira_comment_swallows_errors(monkeypatch):
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")

    def fake_urlopen(req, timeout=None):
        raise OSError("network down")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    jira.comment("MM-1", "hello")  # must not raise


# ----------------------------------------------------------------- findings field typing
def test_sme_verdict_findings_accepts_str_and_dict():
    """SmeVerdict.findings used to be a bare `list` (any element type accepted). Confirm the
    tightened list[str | dict] still accepts both real shapes seen across the codebase (plain
    string notes and {file, note}-style dicts) while excluding clearly-wrong element types."""
    v = schemas.SmeVerdict(findings=["reuse helper X", {"file": "x.rb", "note": "extend here"}])
    assert v.findings[0] == "reuse helper X"
    assert v.findings[1] == {"file": "x.rb", "note": "extend here"}
    with pytest.raises(Exception):
        schemas.SmeVerdict(findings=[123])   # an int is neither a str nor a dict


def test_rca_verdict_findings_for_coder_accepts_str_and_dict():
    v = schemas.RcaVerdict(report_path="/tmp/r.json",
                           findings_for_coder=["fix X in ocean-worker",
                                               {"repo": "ocean-worker", "file": "x.rb"}])
    assert v.findings_for_coder[0] == "fix X in ocean-worker"
    with pytest.raises(Exception):
        schemas.RcaVerdict(report_path="/tmp/r.json", findings_for_coder=[None])


def test_automation_verdict_findings_for_coder_requires_dicts():
    """AutomationVerdict mirrors the ocean-automation-testing skill's own established contract
    (test/cause-shaped dicts) -- unlike the SME/RCA findings fields, this one has unambiguous
    evidence for a dict-only shape."""
    v = schemas.AutomationVerdict(ticket_id="MM-1", automation_result="failed",
                                  findings_for_coder=[{"test": "test_x", "cause": "bug"}])
    assert v.findings_for_coder[0]["cause"] == "bug"
    with pytest.raises(Exception):
        schemas.AutomationVerdict(ticket_id="MM-1", automation_result="failed",
                                  findings_for_coder=["a bare string, not a dict"])


def test_automation_verdict_ac_coverage_requires_dicts():
    v = schemas.AutomationVerdict(ticket_id="MM-1", automation_result="passed",
                                  ac_coverage=[{"ac": "AC1", "test": "test_x", "result": "passed"}])
    assert v.ac_coverage[0]["ac"] == "AC1"
    with pytest.raises(Exception):
        schemas.AutomationVerdict(ticket_id="MM-1", automation_result="passed",
                                  ac_coverage=["AC1 passed"])


# ----------------------------------------------------------------- nodes._docker_resources real logic
def test_docker_resources_none_when_docker_not_on_path(monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda name: None)
    assert nodes._docker_resources() is None


def test_docker_resources_none_when_docker_info_fails(monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/local/bin/docker")

    class FakeProc:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    assert nodes._docker_resources() is None


def test_docker_resources_none_when_expected_fields_missing(monkeypatch):
    """Regression: an alternate Docker backend (colima/Podman/Rancher Desktop) whose `docker info
    --format '{{json .}}'` omits MemTotal/NCPU used to silently produce (0.0, 0) -- the SAME tuple
    used for "Docker isn't running" -- misreporting a genuinely running environment as down. Must
    now return None distinctly, same as the not-reachable case, rather than a misleading 0.0 GB."""
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/local/bin/docker")

    class FakeProc:
        returncode = 0
        stdout = json.dumps({"SomeOtherField": 123})   # no MemTotal/NCPU at all

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    assert nodes._docker_resources() is None


def test_docker_resources_parses_real_fields(monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/local/bin/docker")

    class FakeProc:
        returncode = 0
        stdout = json.dumps({"MemTotal": 8 * 1024 ** 3, "NCPU": 4})

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeProc())
    mem_gb, cpus = nodes._docker_resources()
    assert mem_gb == 8.0
    assert cpus == 4


# ----------------------------------------------------------------- sit_resolve pr_number prompt
def test_sit_resolve_prompt_omits_pr_number_when_unset(tmp_path, monkeypatch):
    """Regression: sit_resolve is shared by the main graph (pr_number always known by now) and
    qa_batch's subgraph (no coder/open_pr step -- pr_number never set), so the prompt used to
    literally say "Service PR #None" for every qa-batch ticket. Must phrase truthfully instead of
    asserting a number that doesn't exist."""
    monkeypatch.setattr(config, "automation_verdict_path", lambda tid: tmp_path / f"{tid}.json")
    captured = {}

    async def fake_run_skill(**kw):
        captured["task_prompt"] = kw["task_prompt"]
        config.automation_verdict_path(kw["ticket_id"]).write_text(
            json.dumps({"ticket_id": kw["ticket_id"], "needs_onboarding": False}))

    monkeypatch.setattr(agents, "run_skill", fake_run_skill)
    asyncio.run(nodes.sit_resolve({"ticket_id": "MM-1", "execution_id": "EXE-x"}))  # no pr_number key
    assert "#None" not in captured["task_prompt"]
    assert "No PR number given" in captured["task_prompt"]


def test_sit_resolve_prompt_includes_pr_number_when_set(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "automation_verdict_path", lambda tid: tmp_path / f"{tid}.json")
    captured = {}

    async def fake_run_skill(**kw):
        captured["task_prompt"] = kw["task_prompt"]
        config.automation_verdict_path(kw["ticket_id"]).write_text(
            json.dumps({"ticket_id": kw["ticket_id"], "needs_onboarding": False}))

    monkeypatch.setattr(agents, "run_skill", fake_run_skill)
    asyncio.run(nodes.sit_resolve({"ticket_id": "MM-1", "execution_id": "EXE-x", "pr_number": 42}))
    assert "Service PR #42" in captured["task_prompt"]
    assert "No PR number given" not in captured["task_prompt"]


# ----------------------------------------------------------------- metrics.py (previously untested)
def test_metrics_add_accumulates_across_calls():
    metrics.reset()
    metrics.add(input_tokens=100, output_tokens=50, tools=2)
    metrics.add(input_tokens=200, output_tokens=25, tools=1)
    t = metrics.totals()
    assert t == {"input": 300, "output": 75, "tools": 3, "stations": 2}


def test_metrics_reset_clears_all_fields():
    metrics.add(input_tokens=999, output_tokens=999, tools=99)
    metrics.reset()
    assert metrics.totals() == {"input": 0, "output": 0, "tools": 0, "stations": 0}


def test_metrics_fmt_formats_tokens_and_tool_calls():
    assert metrics.fmt(500, 500, 3) == "1k tokens · 3 tool calls"
    assert metrics.fmt(52000, 3000, 12) == "55k tokens · 12 tool calls"


def test_metrics_fmt_omits_empty_parts():
    assert metrics.fmt(0, 0, 0) == ""
    assert metrics.fmt(100, 0, 0) == "100 tokens"
    assert metrics.fmt(0, 0, 5) == "5 tool calls"


# ----------------------------------------------------------------- tracing.py (previously untested)
def _clear_langfuse_env(monkeypatch):
    for k in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_BASE_URL", "LANGFUSE_HOST"):
        monkeypatch.delenv(k, raising=False)


def test_load_fk_secrets_sets_env_without_overriding_existing(tmp_path, monkeypatch):
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "# a comment\n\nLANGFUSE_PUBLIC_KEY=pk-from-file\nLANGFUSE_SECRET_KEY='sk-from-file'\n"
    )
    monkeypatch.setattr(tracing, "_FK_SECRETS", secrets)
    _clear_langfuse_env(monkeypatch)
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-already-set")  # must NOT be overridden

    tracing._load_fk_secrets()
    assert os.environ["LANGFUSE_PUBLIC_KEY"] == "pk-from-file"
    assert os.environ["LANGFUSE_SECRET_KEY"] == "sk-already-set"  # setdefault, not overwrite


def test_load_fk_secrets_noop_when_file_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(tracing, "_FK_SECRETS", tmp_path / "does-not-exist.env")
    _clear_langfuse_env(monkeypatch)
    tracing._load_fk_secrets()  # must not raise
    assert "LANGFUSE_PUBLIC_KEY" not in os.environ


def test_ensure_host_bridges_base_url_to_host(monkeypatch):
    _clear_langfuse_env(monkeypatch)
    monkeypatch.setenv("LANGFUSE_BASE_URL", "https://custom.example.com")
    tracing._ensure_host()
    assert os.environ["LANGFUSE_HOST"] == "https://custom.example.com"


def test_ensure_host_defaults_when_nothing_set(monkeypatch):
    _clear_langfuse_env(monkeypatch)
    tracing._ensure_host()
    assert os.environ["LANGFUSE_HOST"] == "https://langfuse.fourkites.com"


def test_host_returns_configured_value(monkeypatch):
    _clear_langfuse_env(monkeypatch)
    assert tracing.host() == "https://langfuse.fourkites.com"


def test_callback_handler_none_without_credentials(monkeypatch):
    monkeypatch.setattr(tracing, "_FK_SECRETS", Path("/nonexistent/does-not-exist.env"))
    _clear_langfuse_env(monkeypatch)
    assert tracing.callback_handler() is None


def test_flush_swallows_errors(monkeypatch):
    """flush() must never raise, even when langfuse isn't installed/configured (the common
    dev/test case) -- best-effort, exactly like telemetry."""
    tracing.flush()  # must not raise


# ----------------------------------------------------------------- report.py (previously untested)
def test_report_finish_returns_none_when_never_started():
    report._meta.clear()
    report._rows.clear()
    assert report.finish({"final_status": "completed"}, Path("/tmp/unused")) is None


def test_report_full_flow_writes_json_and_markdown(tmp_path, monkeypatch):
    metrics.reset()
    metrics.add(input_tokens=1000, output_tokens=500, tools=4)
    report.start("MM-1", "EXE-report-test")
    report.record("researcher", 12.3, {"route": "coding"})
    report.record("coder", 45.6, {"branch": "MM-1/fix", "files_changed": 2})

    final = {"final_status": "completed", "final_outcome": "sit_passed; PR ready",
             "pr_number": 42, "ready_flipped": True,
             "test_automation_pr_url": "https://github.com/x/test-automation/pull/9"}
    md_path = report.finish(final, tmp_path)

    assert md_path == tmp_path / "run-report.md"
    doc = json.loads((tmp_path / "run-report.json").read_text())
    assert doc["ticket"] == "MM-1"
    assert doc["execution_id"] == "EXE-report-test"
    assert doc["final_status"] == "completed"
    assert doc["pr_number"] == 42
    assert doc["usage"] == {"input_tokens": 1000, "output_tokens": 500,
                            "tool_calls": 4, "station_runs": 1}
    assert len(doc["timeline"]) == 2
    assert doc["timeline"][0]["node"] == "researcher"

    md = md_path.read_text()
    assert "**Ticket:** MM-1" in md
    assert "**Result:** COMPLETED" in md
    assert "**Service PR:** #42 (ready-for-review)" in md
    assert "Test-automation PR:" in md
    assert "researcher" in md and "coder" in md


def test_report_markdown_pr_number_without_ready_flip_shows_draft():
    doc = {
        "ticket": "MM-2", "execution_id": "EXE-x", "started": "2026-01-01 00:00:00",
        "finished": "2026-01-01 00:01:00", "duration_seconds": 60.0,
        "final_status": "failed", "final_outcome": "",
        "pr_number": 7, "test_automation_pr_url": "", "ready_flipped": False,
        "usage": {"input_tokens": 0, "output_tokens": 0, "tool_calls": 0, "station_runs": 0},
        "timeline": [],
    }
    md = report._markdown(doc)
    assert "**Service PR:** #7 (draft)" in md


# ----------------------------------------------------------------- ui.py log-level tiers
def test_management_level_shows_only_header_no_outcome_or_milestone(monkeypatch, capsys):
    """management = station_start header only. No step() outcome line, no milestone."""
    monkeypatch.setattr(config, "LOG_LEVEL", "management")
    ui.station_start("researcher")
    ui.milestone("querying jira: getJiraIssue")
    ui.step("researcher", {"route": "coding"}, 12.0)
    out = capsys.readouterr().out
    assert "Research & routing" in out          # the header
    assert "querying jira" not in out           # milestone suppressed
    assert "routed to coding" not in out        # step()'s outcome line suppressed entirely


def test_team_level_shows_header_and_exactly_one_outcome_line(monkeypatch, capsys):
    """team = header + step()'s one outcome line. No milestone, no detail bullets."""
    monkeypatch.setattr(config, "LOG_LEVEL", "team")
    ui.station_start("researcher")
    ui.milestone("querying jira: getJiraIssue")
    ui.step("researcher", {"route": "coding", "target_repos": [{"repo": "ocean-worker"}]}, 12.0)
    out = capsys.readouterr().out
    assert "Research & routing" in out
    assert "routed to coding" in out             # the one outcome line
    assert "querying jira" not in out            # still no milestone at team
    assert "repo: ocean-worker" not in out       # still no detail bullets at team


def test_developer_level_shows_milestones_and_detail_bullets_too(monkeypatch, capsys):
    """developer = team + milestones + detail bullets (the raw per-agent dump is separate,
    emitted by agents._drive, not by these ui.py functions)."""
    monkeypatch.setattr(config, "LOG_LEVEL", "developer")
    ui.station_start("researcher")
    ui.milestone("querying jira: getJiraIssue")
    ui.step("researcher", {"route": "coding", "target_repos": [{"repo": "ocean-worker"}]}, 12.0)
    out = capsys.readouterr().out
    assert "Research & routing" in out
    assert "routed to coding" in out
    assert "querying jira" in out                # milestone now shown
    assert "repo: ocean-worker" in out           # detail bullet now shown


# ----------------------------------------------------------------- agents.py _format_message: Task/SystemMessage family
def _task_message(cls_name, **kw):
    """Build a real SDK dataclass instance for the given Task/SystemMessage subclass —
    exercising the actual attribute names, not an assumption about their shape."""
    from claude_agent_sdk import types as sdk_types
    return getattr(sdk_types, cls_name)(**kw)


def test_format_message_surfaces_task_started():
    msg = _task_message("TaskStartedMessage", subtype="task_started", data={}, task_id="t1",
                        description="research MM-1", uuid="u1", session_id="s1")
    lines = agents._format_message(msg)
    assert any("sub-agent started" in l and "research MM-1" in l for l in lines)


def test_format_message_surfaces_task_progress_with_last_tool():
    msg = _task_message("TaskProgressMessage", subtype="task_progress", data={}, task_id="t1",
                        description="research MM-1", usage={"total_tokens": 500, "tool_uses": 3,
                        "duration_ms": 1200}, uuid="u1", session_id="s1", last_tool_name="Bash")
    lines = agents._format_message(msg)
    assert any("sub-agent progress" in l and "Bash" in l for l in lines)


def test_format_message_surfaces_task_notification():
    msg = _task_message("TaskNotificationMessage", subtype="task_notification", data={}, task_id="t1",
                        status="completed", output_file="/tmp/out.json",
                        summary="found the owning repo", uuid="u1", session_id="s1")
    lines = agents._format_message(msg)
    assert any("completed" in l and "found the owning repo" in l for l in lines)


def test_format_message_surfaces_task_updated():
    msg = _task_message("TaskUpdatedMessage", subtype="task_updated", data={}, task_id="t1",
                        patch={"status": "completed"})
    lines = agents._format_message(msg)
    assert any("task update" in l and "completed" in l for l in lines)


def test_format_message_surfaces_unknown_system_subtype_rather_than_dropping_it():
    """Regression: the entire SystemMessage family previously had NEITHER .content nor
    .result, so it fell through _format_message silently -- developer level (\"give me
    all logs\") was quietly missing every sub-agent lifecycle event. A subtype this
    function doesn't special-case must still surface something, not vanish."""
    msg = _task_message("SystemMessage", subtype="rate_limit_notice", data={"remaining": 10})
    lines = agents._format_message(msg)
    assert any("rate_limit_notice" in l for l in lines)


def test_format_message_suppresses_thinking_tokens_progress_pings():
    """thinking_tokens is a live "still thinking, ~N tokens so far" progress ping the CLI
    fires roughly every ~50 thinking-tokens -- not a discrete event, pure noise (a single
    long thinking burst emits dozens). Deliberately suppressed, unlike the generic unknown-
    subtype fallback: the actual thinking CONTENT still surfaces via the separate 💭
    ThinkingBlock line, so nothing is lost, just the redundant running token count."""
    msg = _task_message("SystemMessage", subtype="thinking_tokens",
                        data={"estimated_tokens": 300, "estimated_tokens_delta": 50})
    assert agents._format_message(msg) == []


def test_format_message_result_message_still_shows_result_text_not_swallowed_by_subtype_check():
    """Regression: ResultMessage ALSO has a .subtype (e.g. "success", set when the SDK
    reports a benign completion) but no .data -- checking .subtype alone to detect the
    SystemMessage family would intercept ResultMessage here FIRST and silently swallow its
    actual result text into a useless "system[success]" line, never reaching the `result`
    handling below. This is exactly the shape of bug this whole fix was meant to close, just
    reintroduced one level down -- must gate on .data too, not .subtype alone."""
    msg = _task_message("ResultMessage", subtype="success", duration_ms=100, duration_api_ms=90,
                        is_error=False, num_turns=3, session_id="s1",
                        result="the coder pushed branch MM-1/fix")
    lines = agents._format_message(msg)
    assert lines == ["✔ the coder pushed branch MM-1/fix"]
    assert not any("system[success]" in l for l in lines)
