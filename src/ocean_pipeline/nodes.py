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

from . import agents, config, gitops, jira, schemas, telemetry
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
    jira.transition(state["ticket_id"], "In Progress")   # best-effort; no-op without a Jira token
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
    telemetry.station_event(state["execution_id"], 0, "end", route=v.route,
                            domain_bucket=v.domain_bucket)
    return {"route": v.route, "research_packet": _load_json(v.packet_path),
            "target_repos": v.target_repos, "domain_bucket": v.domain_bucket}


# ------------------------------------------------------------------ Station 0.5 — ocean SME consult
_SME_BY_BUCKET = {
    "callback_notification": "sme-callback-notification.md",
    "load_creation": "sme-load-creation.md",
    "ocean_tracking_milestones": "sme-ocean-milestones.md",
}


async def sme_consult(state: OceanState) -> dict:
    """Graph-owned SME dispatch: the graph (not the researcher) picks the ocean domain SME by
    domain_bucket and consults it for ownership/reuse guidance. A no-op when no bucket applies."""
    bucket = state.get("domain_bucket") or ""
    exec_id = state["execution_id"]
    sme_md = _SME_BY_BUCKET.get(bucket)
    if not sme_md:
        telemetry.station_event(exec_id, 0.5, "skip", domain_bucket=bucket or "(none)")
        return {"sme_findings": {}}
    telemetry.station_event(exec_id, 0.5, "start", domain_bucket=bucket)
    v: schemas.SmeVerdict = await agents.run_agent(
        agent_md=sme_md,   # fk-aideveloper SME (referenced expert knowledge; resolves via fallback)
        node="sme_consult",
        ticket_id=state["ticket_id"],
        execution_id=exec_id,
        task_prompt=(
            f"Static-architecture question for {state['ticket_id']} (domain: {bucket}). Which "
            f"repo/file/mechanism owns the change this ticket needs, and what should the coder reuse "
            f"or extend? Answer from your curated knowledge; fall back to grep / the code graph only "
            f"where uncovered. Do NOT write code or open anything.\n\n"
            f"Research summary:\n{_brief(state.get('research_packet'))}\n\n{_summary(state)}"
        ),
        verdict_model=schemas.SmeVerdict,
    )
    telemetry.station_event(exec_id, 0.5, "end", findings=len(v.findings))
    return {"sme_findings": {"summary": v.summary, "findings": v.findings}}


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

    # The graph owns the clone location: the coder clones into a per-run workspace and works
    # there, so the reviewer and any rework pass run against the SAME tree (reuse on re-entry).
    workspace = config.workspace_dir(state["execution_id"])
    v: schemas.CoderVerdict = await agents.run_agent(
        agent_md="code.md",   # vendored slim worker (Phase B)
        node="coder",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        cwd=workspace,
        task_prompt=(
            f"Decompose and implement {state['ticket_id']} per FK North Star. isbu: commit + PUSH "
            f"the branch and STOP (no PR — the graph opens it).{rework}\n\n"
            f"WORKSPACE: clone the target repo into {workspace} and do all work there. If the clone "
            f"already exists (a rework pass re-enters here), reuse it — `git fetch` + checkout the "
            f"ticket branch — do NOT re-clone. Report its absolute path as repo_dir.\n\n"
            f"Binding reachability report (build what it says is NOT_YET_BUILT; do not re-litigate "
            f"its verdicts):\n{_brief(state.get('reachability_report'), limit=12000)}\n\n"
            f"Ocean SME ownership/reuse guidance:\n{_brief(state.get('sme_findings'))}\n\n"
            f"Research summary:\n{_brief(state.get('research_packet'))}\n\n"
            f"{_summary(state)}"
        ),
        verdict_model=schemas.CoderVerdict,
    )
    telemetry.station_event(state["execution_id"], 4, "end")
    # A fresh code pass supersedes prior SIT findings; clear them once addressed.
    # Persist WHICH repo the coder pushed to + WHERE the clone lives + the PR title/body it
    # proposed, so the reviewer/rework run in the same tree and open_pr opens deterministically
    # (preserve prior values if a rework pass leaves them blank).
    return {"branch": v.branch, "pushed_sha": v.pushed_sha,
            "files_changed": v.files_changed, "sit_findings": [],
            "service_repo": v.repo or state.get("service_repo", ""),
            "worktree_dir": v.repo_dir or state.get("worktree_dir", ""),
            "pr_title": v.pr_title or state.get("pr_title", ""),
            "pr_body": v.pr_body or state.get("pr_body", "")}


# ------------------------------------------------------------------ Station 5
async def harsh_reviewer(state: OceanState) -> dict:
    iteration = state.get("review_iteration", 0)
    telemetry.station_event(state["execution_id"], 5, "start", review_iteration=iteration)
    # Review in the SAME clone the coder pushed from, so `git diff` + independent test
    # re-execution see the real tree (falls back to the default cwd if unset).
    wt = state.get("worktree_dir") or ""
    v: schemas.ReviewVerdict = await agents.run_agent(
        agent_md="review.md",   # vendored slim worker (Phase B)
        node="harsh_reviewer",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        cwd=Path(wt) if wt else None,
        task_prompt=(
            f"Adversarially review the pushed committed diff on branch {state.get('branch')} for "
            f"{state['ticket_id']} (review round {iteration + 1}) in the clone at "
            f"{wt or '(the current directory)'}. Get the diff with `git diff <base>...HEAD`. "
            f"Classify every finding CRITICAL/MAJOR/MINOR. APPROVE only at zero CRITICAL and zero "
            f"MAJOR.\n\n"
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
            f"Station 3. Do NOT flip the service PR, merge, or deploy.\n"
            f"CONTROL-PLANE ONBOARDING (MM-14621): you are running under the Aquaman control plane, which "
            f"OWNS repo onboarding. If the changed repo is an ocean/isbu service NOT in your supported "
            f"local set, do NOT self-clone, profile, or commit a learned profile here — instead set "
            f"needs_onboarding=true and onboard_repo=<repo> in the verdict, set automation_result=failed / "
            f"failure_class=could_not_verify, and STOP. The graph's learn_repo node will onboard it and "
            f"re-run you. (Standalone/interactive runs still self-onboard per local_service_execution.md.)"
            f"\n\n{_summary(state)}"
        ),
    )

    if not verdict_path.exists():
        telemetry.station_event(exec_id, 6, "end", automation_result="failed",
                                failure_class="could_not_verify")
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "needs_onboarding": False, "sit_report": {}, "sit_findings": [],
                "final_outcome": "sit skill wrote no verdict"}

    v = schemas.AutomationVerdict.model_validate_json(verdict_path.read_text())
    telemetry.station_event(exec_id, 6, "end", automation_result=v.automation_result,
                            failure_class=v.failure_class, execution_mode=v.execution_mode,
                            needs_onboarding=v.needs_onboarding)
    return {
        "automation_result": v.automation_result,
        "failure_class": v.failure_class,
        "execution_mode": v.execution_mode,
        "test_automation_pr_url": v.test_automation_pr_url,
        "sit_findings": v.findings_for_coder,
        "needs_onboarding": v.needs_onboarding,
        "onboard_repo": v.onboard_repo,
        "sit_report": {
            "tests": [t.model_dump() for t in v.tests],
            "changed_repo": v.changed_repo.model_dump() if v.changed_repo else None,
            "dependencies": [d.model_dump() for d in v.dependencies],
            "evidence": v.evidence,
            "testrail_run_id": v.testrail_run_id,
        },
    }


# ------------------------------------------------------------------ graph-owned repo onboarding
async def learn_repo(state: OceanState) -> dict:
    """Onboard an ocean repo Station 6 reported as unsupported — the CONTROL PLANE owns this.

    Station 6 (headless) is told NOT to self-onboard: when it meets an unknown ocean repo it reports
    needs_onboarding + onboard_repo and stops. This node then invokes the skill's learn-a-repo MECHANIC
    (`local_service_execution.md` Steps N1-N5) in an explicit, authorized onboarding pass — confirm it's
    an ocean/isbu service, clone it if absent, profile it, and persist+commit the learned profile back
    into the skill references DELIBERATELY (a tracked graph step, not a hidden side effect of a test run).
    Then it loops back to automation_testing to re-run now that the repo is supported. Capped by
    MAX_ONBOARD_ATTEMPTS so a repo that still reports unsupported after profiling ends as could_not_verify.
    """
    repo = state.get("onboard_repo") or ""
    attempt = state.get("onboard_attempts", 0) + 1
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 5.95, "learn_repo_start", onboard_repo=repo, attempt=attempt)
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="learn_repo",
        ticket_id=tid,
        task_prompt=(
            f"AUTHORIZED ONBOARDING PASS for the Aquaman control plane — this is NOT a test run.\n"
            f"Repo to learn: {repo!r} (Station 6 reported it unsupported for {tid}).\n"
            f"Follow 'Onboarding an unsupported repo (learn a new repo)' in "
            f"skills/ocean-qa-agent/references/local_service_execution.md (Steps N1-N5): first confirm it "
            f"is an ocean/isbu service — if it is NOT (a shared/platform lib or another team's repo), do "
            f"NOT onboard it; write nothing and report it as out-of-scope. Otherwise auto-detect it under "
            f"~/Documents/projects/ or clone cloudqwest/{repo} if absent, PROFILE it into the standard "
            f"per-repo local-run anatomy, and WRITE the profile back into local_service_execution.md "
            f"(+ this skill's Key-ocean-repos list + team-repo-manifest.yml). You ARE authorized to "
            f"persist+commit the learned profile in the fk-aideveloper checkout (message "
            f"'{tid}: learn {repo} local-run profile') — the control plane invoked you specifically to "
            f"onboard. Do NOT run the SIT, open a service PR, or flip anything.\n\n{_summary(state)}"
        ),
    )
    telemetry.station_event(exec_id, 5.95, "learn_repo_end", onboard_repo=repo, attempt=attempt)
    # Record what was learned and clear the request so the Station 6 re-run starts clean.
    return {"onboard_attempts": attempt, "repo_onboarded": repo,
            "needs_onboarding": False, "onboard_repo": ""}


# ------------------------------------------------------------------ code_fault rework prep
async def prep_rework(state: OceanState) -> dict:
    """On a Station-6 code_fault, re-enter the FULL loop (coder -> Station 5 -> Station 6).
    Bump the shared coding-attempts budget, reset the per-attempt review counter, and
    clear stale review findings (SIT findings are carried in sit_findings for the coder)."""
    attempt = state.get("coding_attempts", 0) + 1
    telemetry.station_event(state["execution_id"], 5.9, "code_fault_rework", coding_attempt=attempt)
    return {"coding_attempts": attempt, "review_iteration": 0, "review_findings": []}


# ------------------------------------------------------------------ human approval gate (optional)
async def human_gate(state: OceanState) -> dict:
    """Optional human approval before the ready-flip (OCEAN_PIPELINE_REQUIRE_APPROVAL). Default OFF
    -> pass-through (auto-flip on green). When ON, interrupt() pauses the run until an engineer
    resumes with a decision (`ocean-pipeline --resume <exe> --approve|--reject`). Either way the
    pipeline still never merges or deploys — that boundary is unchanged."""
    if not config.REQUIRE_APPROVAL:
        return {}
    from langgraph.types import interrupt
    decision = interrupt({
        "action": "flip_service_pr_ready",
        "ticket_id": state["ticket_id"],
        "pr_number": state.get("pr_number"),
        "test_automation_pr_url": state.get("test_automation_pr_url"),
        "prompt": ("SIT passed. Approve flipping the service PR to ready-for-review? "
                   "Resume with --approve or --reject."),
    })
    return {"approval_decision": str(decision)}


# ------------------------------------------------------------------ ready-flip (plain code, on GREEN)
async def flip_ready(state: OceanState) -> dict:
    """On PASS: cross-link the test-automation PR into the service PR and flip the service PR to
    ready-for-review — deterministic gh, run by the graph, NOT an agent. Also moves the ticket to
    In Review + posts a PR-link comment (best-effort Jira). This is the intended AUTOMATED terminal
    action; the human boundary is merge/deploy, which the pipeline never performs."""
    telemetry.station_event(state["execution_id"], 6.5, "start")
    slug, pr = _service_slug(state), state.get("pr_number")
    if slug and pr:
        gitops.cross_link_and_ready(slug, int(pr), state.get("test_automation_pr_url") or "")
    tid = state["ticket_id"]
    jira.transition(tid, "In Review")
    pr_line = f"service PR #{pr}" + (f" · test PR {state['test_automation_pr_url']}"
                                     if state.get("test_automation_pr_url") else "")
    jira.comment(tid, f"🤖 Aquaman: SIT passed; {pr_line} flipped to ready-for-review. "
                      f"Merge/deploy remain with the engineer.")
    telemetry.station_event(state["execution_id"], 6.5, "end", ready_flipped=bool(slug and pr))
    return {"ready_flipped": True, "final_status": "completed",
            "final_outcome": f"sit_passed; service PR #{state.get('pr_number')} ready-for-review"}


# ------------------------------------------------------------------ stop (rejected / failed / could_not_verify / budget exhausted)
async def stop_run(state: OceanState) -> dict:
    if str(state.get("approval_decision", "")).lower().startswith("reject"):
        # Human rejected the ready-flip at the approval gate — not a SIT failure.
        telemetry.station_event(state["execution_id"], 6.5, "stop", reason="rejected_by_engineer")
        return {"final_status": "failed", "ready_flipped": False,
                "final_outcome": f"human rejected the ready-flip; service PR "
                                 f"#{state.get('pr_number')} left draft"}
    fc = state.get("failure_class", "")
    if state.get("needs_onboarding"):
        reason = "repo_onboarding_exhausted"   # still unsupported after MAX_ONBOARD_ATTEMPTS
    elif fc == "code_fault":
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
