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

from ocean_pipeline import agents, config, gitops, graph, jira, nodes, schemas, telemetry


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
        if node.startswith("dep_resolver") or node.startswith("reachability_gate"):
            return schemas.ReachabilityVerdict(report_path=nowhere)
        if node.startswith("coder"):
            return schemas.CoderVerdict(branch=f"{kw['ticket_id']}/b", pushed_sha="deadbeef",
                                        repo="cloudqwest/ocean-worker", repo_dir="/tmp/ws/ocean-worker",
                                        pr_title="t", pr_body="b")
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
            (config.artifacts_dir(kw["execution_id"]) / "testrail_run.txt").write_text("555")
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

    # Default the QA review gate to AUTO (no human pause) + no TestRail, so the full-path tests run
    # without interrupting. Gate-specific tests override these.
    monkeypatch.setattr(config, "QA_REVIEW_AUTO", True)
    monkeypatch.setattr(config, "QA_TESTRAIL", False)

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


# ----------------------------------------------------------------- vendored workers (Phase B + Workstream B)
def test_vendored_workers_resolve():
    """Every migrated worker is vendored into this repo and resolves BEFORE any fk-aideveloper
    fallback; only the ocean SME agents still fall back (referenced domain knowledge). Guards the
    control-plane decoupling — no graph node should load a heavy fk-aideveloper station file."""
    for name in ("research.md", "code.md", "review.md",           # Phase B
                 "dep-resolve.md", "reachability.md", "rca-research.md",   # Workstream B
                 "graph-augment.md", "release-intel.md"):
        p = agents._agent_path(name)
        assert p == config.VENDORED_AGENTS_DIR / name and p.exists(), f"{name} not vendored"
        assert "Not your job" in p.read_text(), f"{name} missing the process-ownership boundary"
    # ocean SME agents intentionally still resolve to the fk-aideveloper station dir (domain knowledge)
    assert agents._agent_path("sme-load-creation.md") == config.AGENTS_DIR / "sme-load-creation.md"


def test_run_agent_loads_vendored_worker(tmp_path, monkeypatch):
    """run_agent must actually LOAD the vendored worker (regression: it previously read
    config.AGENTS_DIR unconditionally, so a real run would FileNotFoundError on research.md —
    the graph tests never caught it because they mock run_agent itself)."""
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
    monkeypatch.setattr(nodes, "_docker_resources", lambda: (0.0, 0))
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
                        lambda: "could_not_verify: insufficient_docker_resources — have 2.0 GB / 2 CPU")
    final = _run()
    assert final["final_status"] == "failed"
    assert "could_not_verify" in final["final_outcome"]
    assert s.calls["sit_run"] == 0        # short-circuited before the agent call
    assert s.calls["sit_triage"] == 0     # recognized the marker, skipped its own agent call
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
