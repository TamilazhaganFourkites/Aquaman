"""Graph nodes.

Two kinds of node:
  * worker nodes — telemetry -> run ONE narrow agent/skill via the Claude Agent SDK ->
    return a partial state update. The graph (graph.py) owns all sequencing/routing/loops.
  * plain-code nodes (open_pr, flip_ready) — deterministic git/gh operations run directly
    here via gitops.py, NOT delegated to an agent, so the process is exact and testable.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from . import agents, config, gitops, jira, schemas, telemetry
from .state import OceanState


def _docker_resources() -> tuple[float, int] | None:
    """(mem_gb, cpus) from `docker info`, or None if Docker isn't reachable OR its output doesn't
    expose the fields we need. Returning None (rather than (0, 0)) for BOTH cases matters: an
    alternate Docker backend (colima/Podman/Rancher Desktop) that omits MemTotal/NCPU from
    `docker info --format '{{json .}}'` is genuinely running, just not introspectable this way —
    conflating that with "Docker isn't running" would misreport a real environment as down.
    Plain deterministic check — no agent call — so sit_run can fail fast (~1s) instead of
    spending an entire expensive agent invocation attempting a bring-up that's going to OOM
    (see config.MIN_DOCKER_MEMORY_GB)."""
    if not shutil.which("docker"):
        return None
    try:
        out = subprocess.run(["docker", "info", "--format", "{{json .}}"],
                             capture_output=True, text=True, timeout=10)
        if out.returncode != 0 or not out.stdout.strip():
            return None
        info = json.loads(out.stdout)
        if "MemTotal" not in info or "NCPU" not in info:
            return None
        return info["MemTotal"] / (1024 ** 3), info["NCPU"]
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError):
        return None


def _docker_preflight_reason() -> str:
    """Empty string if Docker has enough resources per config.MIN_DOCKER_MEMORY_GB/CPUS; otherwise
    a ready-to-use could_not_verify reason string."""
    resources = _docker_resources()
    if resources is None:
        return ("could_not_verify: Docker is not running, not reachable, or `docker info` didn't "
                 "expose memory/CPU (an alternate backend like colima/Podman may need a different "
                 "check) — could not determine available resources.")
    mem_gb, cpus = resources
    if mem_gb < config.MIN_DOCKER_MEMORY_GB or cpus < config.MIN_DOCKER_CPUS:
        return (f"could_not_verify: insufficient_docker_resources — have {mem_gb:.1f} GB / {cpus} CPU, "
                f"need >= {config.MIN_DOCKER_MEMORY_GB} GB / {config.MIN_DOCKER_CPUS} CPU (see "
                f"local_service_execution.md 'Docker memory ceiling'). Raise Docker Desktop/Rancher "
                f"Desktop memory+CPU allocation before retrying.")
    return ""


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
    "ocean_data_quality": "sme-ocean-data-quality.md",
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
        agent_md="dep-resolve.md",   # vendored slim worker (Workstream B)
        node="dep_resolver",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Resolve dependencies/blockers for {state['ticket_id']}. Self-solve where possible; "
            f"flag only true blockers.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.ReachabilityVerdict,
    )
    telemetry.station_event(state["execution_id"], 1, "end", blocking=v.blocking)
    return {"dependency_report": {"report_path": v.report_path, "notes": v.notes},
            "dependency_blocking": v.blocking}


# ------------------------------------------------------------------ Station 1.5
async def reachability_gate(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 1.5, "start")
    v: schemas.ReachabilityVerdict = await agents.run_agent(
        agent_md="reachability.md",   # vendored slim worker (Workstream B)
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
            agent_md="graph-augment.md",   # vendored slim worker (Workstream B)
            node="graph_augment",
            ticket_id=state["ticket_id"],
            execution_id=state["execution_id"],
            task_prompt=f"Run Graph Caller Chain Augmentation for PR #{state['pr_number']}.\n\n{_summary(state)}",
            verdict_model=schemas.NoOutputVerdict,
        )
        ok = True
    except Exception:
        ok = False  # best-effort: log, do not block
    telemetry.station_event(state["execution_id"], 4.5, "end", graph_augmented=ok)
    return {"graph_augmented": ok}


# ------------------------------------------------------------------ 4.6 release intel
async def release_intel(state: OceanState) -> dict:
    """Best-effort, like graph_augment: release-intel.md's own worker doc says this must
    never block the pipeline. Without a try/except here, a worker failure after retries would
    abort the entire run, contradicting that contract."""
    telemetry.station_event(state["execution_id"], 4.6, "start")
    try:
        await agents.run_agent(
            agent_md="release-intel.md",   # vendored slim worker (Workstream B)
            node="release_intel",
            ticket_id=state["ticket_id"],
            execution_id=state["execution_id"],
            task_prompt=f"Run the Release Intelligence Writer for {state['ticket_id']}.\n\n{_summary(state)}",
            verdict_model=schemas.NoOutputVerdict,
        )
        ok = True
    except Exception:
        ok = False  # best-effort: log, do not block
    telemetry.station_event(state["execution_id"], 4.6, "end", release_intel_written=ok)
    return {"release_intel_written": ok}


# ============================ Station 6 — local SIT, decomposed into graph nodes ============================
# LangGraph owns the Station-6 sequence: sit_resolve -> sit_run -> sit_triage, driving the
# ocean-automation-testing skill one `--only <phase>` at a time (state flows through the skill's own
# memory/tickets/<TICKET>-automation-testing.json). The graph branches at the two real decision points:
# after resolve (onboard an unsupported repo) and after triage (pass / code_fault / could_not_verify).

# ------------------------------------------------------------------ Station 6a — resolve (+ gate)
async def sit_resolve(state: OceanState) -> dict:
    """Resolve the narrowest changed repo + existing SIT (skill Station 0, `--only resolve`). Because
    the control plane OWNS onboarding, this reports-only on an unsupported repo (needs_onboarding) so
    the graph can branch to learn_repo BEFORE any authoring/execution. Clears the prior verdict first."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.0, "start", coding_attempt=state.get("coding_attempts", 0))
    verdict_path = config.automation_verdict_path(tid)
    verdict_path.parent.mkdir(parents=True, exist_ok=True)
    if verdict_path.exists():
        verdict_path.unlink()  # fresh Station-6 attempt (drop a prior loop's verdict)
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_resolve",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 0 (resolve) ONLY for {tid} (`--only resolve`), "
            f"HEADLESS. Service PR #{state.get('pr_number')}; Station 5 APPROVED so Station 0.5's gate "
            f"auto-passes. Resolve the NARROWEST changed ocean repo set, the existing SIT (reuse-aware), "
            f"the domain bucket, and pr_number; persist them to {verdict_path}.\n"
            f"CONTROL-PLANE ONBOARDING (MM-14621): the Aquaman control plane OWNS repo onboarding. If the "
            f"changed repo is an ocean/isbu service NOT in your supported local set, do NOT self-clone, "
            f"profile, or commit — set needs_onboarding=true + onboard_repo=<repo> in {verdict_path} and "
            f"STOP. Do NOT author or run the SIT here.\n\n{_summary(state)}"
        ),
    )
    partial = _load_json(str(verdict_path))
    needs = bool(partial.get("needs_onboarding"))
    telemetry.station_event(exec_id, 6.0, "end", needs_onboarding=needs)
    return {"needs_onboarding": needs, "onboard_repo": partial.get("onboard_repo", "")}


# ------------------------------------------------------------------ Station 6b — author (draft + STOP)
async def sit_author(state: OceanState) -> dict:
    """Draft the SIT scenarios + sample test (skill Station 1) and STOP — no TestRail cases, no run.
    A human reviews the draft at qa_review_gate before anything executes or gets committed. On a
    'changes' loop-back, the reviewer's note is fed in so ocean-qa-agent revises the draft."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    it = state.get("qa_review_iteration", 0)
    telemetry.station_event(exec_id, 6.1, "start", qa_review_iteration=it)
    note = state.get("qa_note") or ""
    revise = (f"\nThe reviewer requested CHANGES to the prior draft — revise the scenarios/test to "
              f"address this feedback:\n{note}\n") if note else ""
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_author",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 1 (author) ONLY for {tid} (`--only author`), HEADLESS. "
            f"Station 0 (resolve) already ran — do NOT re-resolve. Draft the invariant-compliant SIT "
            f"scenarios + the pytest file via ocean-qa-agent with `--no-review --skip-testrail`: "
            f"AUTHOR/UPDATE the test file (reuse only if it still covers the current diff) and record the "
            f"scenarios + the test path, then STOP. Do NOT create TestRail cases and do NOT execute the "
            f"SIT — a human reviews this draft next.{revise}\n\n{_summary(state)}"
        ),
    )
    partial = _load_json(str(config.automation_verdict_path(tid)))
    telemetry.station_event(exec_id, 6.1, "end")
    return {"qa_test_path": partial.get("test_path") or partial.get("existing_test_path", ""),
            "qa_review_iteration": it + 1, "qa_note": ""}


# ------------------------------------------------------------------ Station 6b.5 — QA review gate (human 3-way)
async def qa_review_gate(state: OceanState) -> dict:
    """Human review of the drafted SIT — the same 3-way choice ocean-qa-agent offers interactively
    (approve-with-TestRail / approve-without-TestRail / changes), surfaced at the graph level so it
    works headless. Default ON (interrupt + wait). QA_REVIEW_AUTO skips the pause and auto-approves
    (with TestRail only if QA_TESTRAIL is set)."""
    exec_id = state["execution_id"]
    if config.QA_REVIEW_AUTO:
        decision = "approve_testrail" if config.QA_TESTRAIL else "approve_no_testrail"
        telemetry.station_event(exec_id, 6.15, "auto", qa_decision=decision)
        return {"qa_decision": decision, "qa_note": ""}
    from langgraph.types import interrupt
    raw = interrupt({
        "action": "qa_review",
        "ticket_id": state["ticket_id"],
        "test_path": state.get("qa_test_path"),
        "prompt": ("Review the drafted SIT scenarios + sample test, then resume with ONE of: "
                   "`--qa approve-testrail` | `--qa approve-no-testrail` | "
                   "`--qa changes --note '<feedback>'`."),
    })
    decision = raw.get("decision") if isinstance(raw, dict) else str(raw)
    note = raw.get("note", "") if isinstance(raw, dict) else ""
    telemetry.station_event(exec_id, 6.15, "decision", qa_decision=decision)
    return {"qa_decision": decision, "qa_note": note}


# ------------------------------------------------------------------ Station 6c — execute the approved SIT
async def sit_run(state: OceanState) -> dict:
    """Execute the approved SIT local + mock-first (skill Station 2). Authoring + human review already
    happened; this only runs the changed repo locally, mocks the rest, and captures per-test results."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.2, "start")

    # Resource pre-flight (ocean-qa-agent-ac-driven-plan.md Workstream 3.4): fail fast, deterministically,
    # BEFORE spending an entire agent invocation on a Docker bring-up that's going to OOM (MM-13437's
    # full-chain attempt ground for ~40 min before hitting this exact documented ceiling).
    reason = _docker_preflight_reason()
    if reason:
        verdict_path = config.automation_verdict_path(tid)
        verdict_path.parent.mkdir(parents=True, exist_ok=True)
        # Merge onto whatever sit_resolve/sit_author already recorded (test_path, domain_bucket, ...)
        # rather than clobbering it — `_preflight_short_circuit` is the explicit marker sit_triage
        # checks for; it is NEVER written by the skill itself, so it can't collide with a real verdict.
        partial = _load_json(str(verdict_path))
        partial.update({
            "ticket_id": tid, "automation_result": "failed", "failure_class": "could_not_verify",
            "execution_mode": "local-mock-first", "evidence": reason,
            "_preflight_short_circuit": True,
        })
        verdict_path.write_text(json.dumps(partial))
        telemetry.station_event(exec_id, 6.2, "end", automation_result="failed",
                                failure_class="could_not_verify", preflight="insufficient_resources")
        return {}

    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_run",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 2 (execute) ONLY for {tid} (`--only run`), HEADLESS. "
            f"The SIT was authored + human-approved already — do NOT re-author. EXECUTE it local + "
            f"mock-first: run ONLY the changed repo locally per its language-scoped build_env "
            f"(ruby=docker) and mock the rest (ocean_mock_helper + route_local); never present a "
            f"native-host Ruby run as passed. Capture per-test pass/fail to reports/junit.xml. Do NOT "
            f"run Station 3 (report/verdict) — the graph's sit_triage node does that next."
            f"\n\n{_summary(state)}"
        ),
    )
    telemetry.station_event(exec_id, 6.2, "end")
    return {}


# ------------------------------------------------------------------ Station 6c' — TestRail cases (parallel)
async def sit_testrail(state: OceanState) -> dict:
    """Create the TestRail cases for the approved SIT (Project 22 / Suite 197). Runs IN PARALLEL with
    sit_run — TestRail's API is slow + rate-limited, so it must not block the functional gate. Returns
    the run id via STATE (a dedicated file, not the shared verdict json) to avoid a write race with
    the concurrent sit_run/sit_triage."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.3, "start")
    tr_path = config.artifacts_dir(exec_id) / "testrail_run.txt"
    if tr_path.exists():
        tr_path.unlink()
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_testrail",
        ticket_id=tid,
        task_prompt=(
            f"Create the TestRail cases for the SIT already authored + approved for {tid} via "
            f"ocean-qa-agent (Project 22 / Suite 197 — its Steps 6/6a). Do ONLY TestRail case creation "
            f"for the EXISTING authored test at {state.get('qa_test_path') or '(the ticket SIT)'} — do "
            f"NOT re-author, execute, or open any PR. This runs in parallel with the local SIT run, so "
            f"touch ONLY TestRail (respect its rate limits). Write ONLY the integer TestRail run id to "
            f"{tr_path}.\n\n{_summary(state)}"
        ),
    )
    run_id = 0
    if tr_path.exists():
        try:
            run_id = int(tr_path.read_text().strip())
        except (ValueError, OSError):
            run_id = 0
    telemetry.station_event(exec_id, 6.3, "end", testrail_run_id=run_id)
    return {"testrail_run_id": run_id}


# ------------------------------------------------------------------ Station 6c — report + triage + verdict
async def sit_triage(state: OceanState) -> dict:
    """Parse junit, triage the run, write the canonical verdict, and (on PASS) open the test-automation
    draft PR (skill Station 3, `--only report`). This node reads that verdict; the graph branches on it.
    A missing verdict file is treated as could_not_verify as a backstop."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.4, "start")
    verdict_path = config.automation_verdict_path(tid)
    # The verdict file is progressively enriched across stations (sit_resolve/sit_author already wrote
    # partial data by this point in the normal path), so mere existence isn't a safe signal. sit_run's
    # resource-preflight short-circuit (Workstream 3.4) stamps an explicit `_preflight_short_circuit`
    # marker that the skill itself never writes — only THAT means "pytest never ran, skip the redundant,
    # expensive Station-3 agent call" (there's no reports/junit.xml for it to parse anyway).
    partial = _load_json(str(verdict_path))
    if partial.get("_preflight_short_circuit"):
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="could_not_verify", preflight_short_circuit=True)
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "execution_mode": partial.get("execution_mode", "local-mock-first"),
                "test_automation_pr_url": "", "sit_findings": [],
                "needs_onboarding": False, "onboard_repo": "",
                "sit_report": {"tests": [], "changed_repo": None, "dependencies": [],
                               "evidence": partial.get("evidence", ""), "testrail_run_id": 0,
                               "ac_coverage": []}}
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_triage",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 3 (report) ONLY for {tid} (`--only report`): parse "
            f"reports/junit.xml for the authoritative per-test pass/fail; TRIAGE any failure — test_fault "
            f"(fix + re-run, capped) vs code_fault (real defect -> findings_for_coder) vs could_not_verify. "
            f"On PASS, commit the SIT into cloudqwest/test-automation on an {tid}/… branch, open a DRAFT PR "
            f"(reuse an existing {tid} test-automation PR — do not duplicate), and set test_automation_pr_url. "
            f"Write the verdict object to {verdict_path} exactly per SKILL.md Station 3. Do NOT flip the "
            f"service PR, merge, or deploy.\n\n{_summary(state)}"
        ),
    )
    if not verdict_path.exists():
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="could_not_verify")
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "needs_onboarding": False, "sit_report": {}, "sit_findings": [],
                "final_outcome": "sit skill wrote no verdict"}

    v = schemas.AutomationVerdict.model_validate_json(verdict_path.read_text())
    telemetry.station_event(exec_id, 6.4, "end", automation_result=v.automation_result,
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
            # prefer the id from the parallel sit_testrail branch (via state) over the skill's verdict
            "testrail_run_id": state.get("testrail_run_id") or v.testrail_run_id,
            # AC traceability (additive, optional — [] if the skill hasn't started emitting it yet).
            "ac_coverage": v.ac_coverage,
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
    Then it loops back to sit_resolve to re-resolve now that the repo is supported. Capped by
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
            f"per-repo local-run anatomy, and WRITE the profile back per Step N4's two-file split: the "
            f"canonical REGISTRY ROW (repo/lang/role/run-cmd/port/health/boot-gates + the language-scoped "
            f"build-rule bucket) into skills/_shared/ocean-knowledge/ocean-repos.md, and the OPERATIONAL "
            f"profile (infra/mock bullets, boot recipe, gotchas, api_base_urls row) into "
            f"local_service_execution.md (+ this skill's Key-ocean-repos list + team-repo-manifest.yml). "
            f"Do NOT duplicate the registry columns across both files. You ARE authorized to "
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
        agent_md="rca-research.md",   # vendored slim worker (Workstream B); drives the ocean-rca approach
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
