"""Graph nodes.

Two kinds of node:
  * worker nodes — telemetry -> run ONE narrow agent/skill via the Claude Agent SDK ->
    return a partial state update. The graph (graph.py) owns all sequencing/routing/loops.
  * plain-code nodes (open_pr, flip_ready) — deterministic git/gh operations run directly
    here via gitops.py, NOT delegated to an agent, so the process is exact and testable.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import agents, config, gitops, schemas, telemetry
from .state import OceanState


def _summary(state: OceanState) -> str:
    """<=300-token ticket summary pushed to each worker."""
    s = (
        f"Ticket: {state['ticket_id']}\n"
        f"Context: {state.get('context', '(none)')}\n"
        f"Target repos: {json.dumps(state.get('target_repos', []))}\n"
    )
    if state.get("rca_findings"):
        # RCA-originated fix: the gates + coder work from the RCA brief, not a coding-route packet.
        s += f"Origin: RCA fix. RCA brief: {json.dumps(state['rca_findings'])}\n"
    return s


def _load_json(path: str) -> dict:
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {}


def _brief(obj, limit: int = 6000) -> str:
    """Serialize a state slice to feed a worker INLINE (explicit state, not a file handoff).
    Bounded so a large packet can't blow the prompt."""
    if not obj:
        return "(none)"
    s = json.dumps(obj, indent=2, default=str)
    return s if len(s) <= limit else s[:limit] + "\n… (truncated)"


def _service_slug(state: OceanState) -> str:
    """The `<org>/<name>` slug of the repo whose branch we open/flip the PR on. Prefer what the
    coder reported; fall back to the single target repo when there's exactly one."""
    repo = state.get("service_repo") or ""
    if not repo:
        repos = state.get("target_repos") or []
        if len(repos) == 1:
            repo = repos[0].get("repo", "")
    return gitops.repo_slug(repo)


# ------------------------------------------------------------------ Station 0
async def researcher(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 0, "start")
    v: schemas.ResearchVerdict = await agents.run_agent(
        agent_md="research.md",   # vendored slim worker (Phase B); resolves under workers/
        node="researcher",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Research {state['ticket_id']} and classify the route "
            f"(coding vs rca vs sop vs loft vs ff_onboarding). Record the language-scoped "
            f"build_env for each target repo (ruby=docker, java/go=native).\n\n{_summary(state)}"
        ),
        verdict_model=schemas.ResearchVerdict,
    )
    telemetry.station_event(state["execution_id"], 0, "end", route=v.route)
    return {"route": v.route, "research_packet": _load_json(v.packet_path),
            "target_repos": v.target_repos}


# ------------------------------------------------------------------ Station 1
async def dep_resolver(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 1, "start")
    v: schemas.ReachabilityVerdict = await agents.run_agent(
        agent_md="fk-dependency-resolver.md",
        node="dep_resolver",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Resolve dependencies/blockers for {state['ticket_id']}. Self-solve where possible; "
            f"flag only true blockers.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.ReachabilityVerdict,
    )
    telemetry.station_event(state["execution_id"], 1, "end")
    return {"dependency_report": {"report_path": v.report_path, "notes": v.notes}}


# ------------------------------------------------------------------ Station 1.5
async def reachability_gate(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 1.5, "start")
    v: schemas.ReachabilityVerdict = await agents.run_agent(
        agent_md="fk-reachability-gate.md",
        node="reachability_gate",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Execution-verify every ALREADY_MET / self-solve / blocked / cross-repo claim for "
            f"{state['ticket_id']}. Emit the binding reachability-report.json.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.ReachabilityVerdict,
    )
    telemetry.station_event(state["execution_id"], 1.5, "end", blocking=v.blocking)
    return {"reachability_report": _load_json(v.report_path), "reachability_blocking": v.blocking}


# ------------------------------------------------------------------ Station 4
async def coder(state: OceanState) -> dict:
    iteration = state.get("review_iteration", 0)
    attempt = state.get("coding_attempts", 0)
    telemetry.station_event(state["execution_id"], 4, "start",
                            review_iteration=iteration, coding_attempt=attempt)

    rework = ""
    rca_findings = state.get("rca_findings", [])
    if rca_findings:
        rework += (f"\nThis change originates from an RCA investigation that concluded a fix is "
                   f"needed. Implement the fix per the RCA brief:\n{json.dumps(rca_findings, indent=2)}\n")
    review_findings = state.get("review_findings", [])
    if review_findings:
        rework += (f"\nAddress these Station 5 review findings from the prior pass:\n"
                   f"{json.dumps(review_findings, indent=2)}\n")
    sit_findings = state.get("sit_findings", [])
    if sit_findings:
        rework += (f"\nAddress these Station 6 SIT code-fault findings (real defects a passing "
                   f"SIT would catch):\n{json.dumps(sit_findings, indent=2)}\n")

    v: schemas.CoderVerdict = await agents.run_agent(
        agent_md="code.md",   # vendored slim worker (Phase B)
        node="coder",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Decompose and implement {state['ticket_id']} per FK North Star. isbu: commit + PUSH "
            f"the branch and STOP (no PR — the graph opens it).{rework}\n\n"
            f"Binding reachability report (build what it says is NOT_YET_BUILT; do not re-litigate "
            f"its verdicts):\n{_brief(state.get('reachability_report'))}\n\n"
            f"Research summary:\n{_brief(state.get('research_packet'))}\n\n"
            f"{_summary(state)}"
        ),
        verdict_model=schemas.CoderVerdict,
    )
    telemetry.station_event(state["execution_id"], 4, "end")
    # A fresh code pass supersedes prior SIT findings; clear them once addressed.
    # Persist WHICH repo the coder pushed to + the PR title/body it proposed, so the
    # open_pr code node can open the PR deterministically (preserve prior values if a
    # rework pass leaves them blank).
    return {"branch": v.branch, "pushed_sha": v.pushed_sha,
            "files_changed": v.files_changed, "sit_findings": [],
            "service_repo": v.repo or state.get("service_repo", ""),
            "pr_title": v.pr_title or state.get("pr_title", ""),
            "pr_body": v.pr_body or state.get("pr_body", "")}


# ------------------------------------------------------------------ Station 5
async def harsh_reviewer(state: OceanState) -> dict:
    iteration = state.get("review_iteration", 0)
    telemetry.station_event(state["execution_id"], 5, "start", review_iteration=iteration)
    v: schemas.ReviewVerdict = await agents.run_agent(
        agent_md="review.md",   # vendored slim worker (Phase B)
        node="harsh_reviewer",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Adversarially review the pushed committed diff on branch {state.get('branch')} for "
            f"{state['ticket_id']} (review round {iteration + 1}). Classify every finding "
            f"CRITICAL/MAJOR/MINOR. APPROVE only at zero CRITICAL and zero MAJOR.\n\n"
            f"Research summary (for AC context):\n{_brief(state.get('research_packet'))}\n\n"
            f"{_summary(state)}"
        ),
        verdict_model=schemas.ReviewVerdict,
    )
    telemetry.station_event(state["execution_id"], 5, "end", verdict=v.verdict)
    return {"review_verdict": v.verdict, "review_findings": v.findings,
            "review_iteration": iteration + 1}


# ------------------------------------------------------------------ 3.87 open PR (plain code, idempotent)
async def open_pr(state: OceanState) -> dict:
    """Open the DRAFT service PR — deterministic gh, run by the graph, NOT an agent.
    Idempotent: reuses an existing PR for the branch (the code_fault loop re-enters here)."""
    if state.get("pr_number"):
        return {}  # PR already open (code_fault rework path re-enters here)
    telemetry.station_event(state["execution_id"], 3.87, "start")
    slug, branch = _service_slug(state), state.get("branch")
    if not slug or not branch:
        raise gitops.GitOpError(
            f"cannot open PR: missing repo slug ({slug!r}) or branch ({branch!r}) — the coder "
            f"must report `repo` and `branch`, or provide a single target repo."
        )
    title = state.get("pr_title") or f"{state['ticket_id']}: automated pipeline change"
    body = state.get("pr_body") or f"Automated change for {state['ticket_id']} (FK Ocean pipeline)."
    pr_number = gitops.open_draft_pr(slug, branch, title, body)
    telemetry.station_event(state["execution_id"], 3.87, "end", pr_number=pr_number)
    return {"pr_number": pr_number}


# ------------------------------------------------------------------ 4.5b graph augment (best-effort)
async def graph_augment(state: OceanState) -> dict:
    if not state.get("pr_number"):
        return {"graph_augmented": False}
    telemetry.station_event(state["execution_id"], 4.5, "start")
    try:
        await agents.run_agent(
            agent_md="fk-coder.md",
            node="graph_augment",
            ticket_id=state["ticket_id"],
            execution_id=state["execution_id"],
            task_prompt=f"Run Graph Caller Chain Augmentation for PR #{state['pr_number']}.\n\n{_summary(state)}",
            verdict_model=schemas.CoderVerdict,
        )
        ok = True
    except Exception:
        ok = False  # best-effort: log, do not block
    telemetry.station_event(state["execution_id"], 4.5, "end", graph_augmented=ok)
    return {"graph_augmented": ok}


# ------------------------------------------------------------------ 4.6 release intel
async def release_intel(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 4.6, "start")
    await agents.run_agent(
        agent_md="fk-coder.md",
        node="release_intel",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=f"Run the Release Intelligence Writer for {state['ticket_id']}.\n\n{_summary(state)}",
        verdict_model=schemas.CoderVerdict,
    )
    telemetry.station_event(state["execution_id"], 4.6, "end")
    return {"release_intel_written": True}


# ------------------------------------------------------------------ Station 6 (ocean-automation-testing)
async def automation_testing(state: OceanState) -> dict:
    """Drive the ocean-automation-testing skill end-to-end (headless) and read its canonical verdict.

    The skill owns its internal stations (SKILL.md 0/0.5/1/2/3): resolve the changed repo, gate on
    the Station-5 APPROVED verdict, author/locate the SIT via ocean-qa-agent, run it local + mock-first
    (only the changed ocean repo runs locally under its language-scoped build env; the rest mocked),
    open the test-automation draft PR on pass, and write the verdict to
    memory/tickets/<TICKET>-automation-testing.json. This node reads that file; the graph branches on it.
    A Docker/infra bring-up failure is the skill's own could_not_verify (language-scoped Docker rule);
    a missing verdict file is treated as could_not_verify here as a backstop.
    """
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6, "start", coding_attempt=state.get("coding_attempts", 0))

    verdict_path = config.automation_verdict_path(tid)
    verdict_path.parent.mkdir(parents=True, exist_ok=True)
    if verdict_path.exists():
        verdict_path.unlink()  # avoid reading a stale verdict from a prior loop

    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="automation_testing",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing for {tid} HEADLESS (pipeline Station 6), end to end.\n"
            f"Service PR: #{state.get('pr_number')}. Station 5 harsh-review returned APPROVED "
            f"(zero CRITICAL/MAJOR) and review comments are addressed — Station 0.5's gate auto-passes; "
            f"proceed without asking.\n"
            f"Resolve the NARROWEST changed ocean repo set; author or locate the invariant-compliant SIT "
            f"(reuse only if it still covers the current diff, else update/author); run ONLY the changed "
            f"repo locally per its language-scoped build_env (ruby=docker) and mock the rest "
            f"(ocean_mock_helper + route_local) — never present a native-host Ruby run as passed. Capture "
            f"per-test pass/fail + failure root cause. Triage failures: test_fault (fix + re-run, capped) "
            f"vs code_fault (real defect -> findings_for_coder) vs could_not_verify. On PASS, commit the SIT "
            f"into cloudqwest/test-automation on an {tid}/… branch, open a DRAFT PR (reuse an existing "
            f"{tid} test-automation PR if one is already open — do not duplicate), and set "
            f"test_automation_pr_url. Write the verdict object to {verdict_path} exactly per SKILL.md "
            f"Station 3. Do NOT flip the service PR, merge, or deploy.\n\n{_summary(state)}"
        ),
    )

    if not verdict_path.exists():
        telemetry.station_event(exec_id, 6, "end", automation_result="failed",
                                failure_class="could_not_verify")
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "sit_report": {}, "sit_findings": [], "final_outcome": "sit skill wrote no verdict"}

    v = schemas.AutomationVerdict.model_validate_json(verdict_path.read_text())
    telemetry.station_event(exec_id, 6, "end", automation_result=v.automation_result,
                            failure_class=v.failure_class, execution_mode=v.execution_mode)
    return {
        "automation_result": v.automation_result,
        "failure_class": v.failure_class,
        "execution_mode": v.execution_mode,
        "test_automation_pr_url": v.test_automation_pr_url,
        "sit_findings": v.findings_for_coder,
        "sit_report": {
            "tests": [t.model_dump() for t in v.tests],
            "changed_repo": v.changed_repo.model_dump() if v.changed_repo else None,
            "dependencies": [d.model_dump() for d in v.dependencies],
            "evidence": v.evidence,
            "testrail_run_id": v.testrail_run_id,
        },
    }


# ------------------------------------------------------------------ code_fault rework prep
async def prep_rework(state: OceanState) -> dict:
    """On a Station-6 code_fault, re-enter the FULL loop (coder -> Station 5 -> Station 6).
    Bump the shared coding-attempts budget, reset the per-attempt review counter, and
    clear stale review findings (SIT findings are carried in sit_findings for the coder)."""
    attempt = state.get("coding_attempts", 0) + 1
    telemetry.station_event(state["execution_id"], 5.9, "code_fault_rework", coding_attempt=attempt)
    return {"coding_attempts": attempt, "review_iteration": 0, "review_findings": []}


# ------------------------------------------------------------------ ready-flip (plain code, on GREEN)
async def flip_ready(state: OceanState) -> dict:
    """On PASS: cross-link the test-automation PR into the service PR and flip the service PR to
    ready-for-review — deterministic gh, run by the graph, NOT an agent. This is the intended
    AUTOMATED terminal action; the human boundary is merge/deploy, which the pipeline never performs."""
    telemetry.station_event(state["execution_id"], 6.5, "start")
    slug, pr = _service_slug(state), state.get("pr_number")
    if slug and pr:
        gitops.cross_link_and_ready(slug, int(pr), state.get("test_automation_pr_url") or "")
    telemetry.station_event(state["execution_id"], 6.5, "end", ready_flipped=bool(slug and pr))
    return {"ready_flipped": True, "final_status": "completed",
            "final_outcome": f"sit_passed; service PR #{state.get('pr_number')} ready-for-review"}


# ------------------------------------------------------------------ stop (failed / could_not_verify / budget exhausted)
async def stop_run(state: OceanState) -> dict:
    fc = state.get("failure_class", "")
    if fc == "code_fault":
        reason = "coding_attempts_exhausted"
    elif fc == "could_not_verify":
        reason = "could_not_verify"
    else:
        reason = "sit_failed"
    telemetry.station_event(state["execution_id"], 6, "stop", reason=reason)
    return {"final_status": "failed", "ready_flipped": False,
            "final_outcome": f"sit_failed:{reason}; service PR left draft"}


# ------------------------------------------------------------------ RCA agent
async def rca_agent(state: OceanState) -> dict:
    """Run ocean-rca. Deliverable is the evidence-cited report; the outcome also
    says whether a code fix is needed. On fix_needed the RCA brief is handed to the
    coder (diagram: RCA agent -> RCA Done -> Fix needed -> coder)."""
    telemetry.station_event(state["execution_id"], 0.1, "start", route="rca")
    v: schemas.RcaVerdict = await agents.run_agent(
        agent_md="fk-researcher.md",   # routes into the ocean-rca skill; standalone, no PR
        node="rca_agent",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Produce the ocean-rca evidence-cited report for {state['ticket_id']}. "
            f"POST the completed 5-part report back to {state['ticket_id']} as a Jira comment via the "
            f"Atlassian MCP (addCommentToJiraIssue), prefixed '🤖 Aquaman Ocean RCA': root cause, "
            f"evidence (with proof — specific SigNoz/ClickHouse log lines + the source that produced "
            f"them + read-only Redshift records), affected service/component, and recommended fix. "
            f"STRICT PRODUCTION SAFETY: use rca-app / fourkites MCP tools for READ/GET only; NEVER call "
            f"any create/update/delete/resolve tool against production. Then decide: "
            f"does the root cause require a code fix in an ocean repo? If yes, set fix_needed=true "
            f"and populate findings_for_coder with a concrete implementation brief (repo, file, "
            f"what to change, why). If it is working-as-expected / config / data with no code change, "
            f"set fix_needed=false. Do NOT open a PR.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.RcaVerdict,
    )
    telemetry.station_event(state["execution_id"], 0.1, "end", fix_needed=v.fix_needed)
    return {"rca_fix_needed": v.fix_needed, "rca_findings": v.findings_for_coder}


# ------------------------------------------------------------------ unsupported route (terminal)
async def unsupported_route(state: OceanState) -> dict:
    """Researcher classified a route this Ocean pipeline does not handle
    (sop / loft / ff_onboarding / unclassified). Stop cleanly instead of coding it."""
    route = state.get("route")
    telemetry.station_event(state["execution_id"], 0, "unsupported_route", route=route)
    return {"final_status": "failed",
            "final_outcome": f"unsupported route '{route}' for the isbu Ocean pipeline"}


# ------------------------------------------------------------------ RCA Done (terminal, no fix)
async def rca_done(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 0.1, "done", fix_needed=False)
    return {"final_status": "rca_report", "final_outcome": "rca_report_delivered (no code fix needed)"}
