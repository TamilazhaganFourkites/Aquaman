"""The single state object threaded through every node.

This replaces the ad-hoc context passing in fk-execute's prose checklist.
Each station reads what it needs and returns a partial dict; LangGraph merges
it (last-write-wins for scalars/lists — the pipeline is a single sequential
path, so no reducers are needed).
"""
from __future__ import annotations

from typing import Literal, TypedDict

Route = Literal["coding", "rca", "sop", "loft", "ff_onboarding", "unclassified"]
ReviewVerdict = Literal["APPROVE", "CHANGES_REQUIRED"]
AutomationResult = Literal["passed", "failed"]
FailureClass = Literal["", "code_fault", "could_not_verify", "environment_failure"]


class TargetRepo(TypedDict):
    repo: str
    language: str            # ruby | java | go | python | frontend
    build_env: str           # "docker" (ruby workers) | "native" (java/go) — LANGUAGE-SCOPED
    branch: str


class OceanState(TypedDict, total=False):
    # --- identity / telemetry ---
    ticket_id: str
    execution_id: str        # EXE-<hex>, generated at entry (Write Point 1)
    profile: str             # "isbu"
    context: str             # free-form --context passed in

    # --- Station 0: research + routing ---
    route: Route
    research_packet: dict
    target_repos: list[TargetRepo]   # carries the language-scoped Docker decision
    domain_bucket: str               # ocean domain; drives the SME-consult node
    sme_findings: dict               # ownership/reuse guidance from the consulted ocean SME

    # --- RCA branch (diagram: RCA agent -> rca_report (plain-code Jira post) -> RCA review gate -> RCA Done -> Fix needed -> coder) ---
    rca_fix_needed: bool
    rca_findings: list               # implementation brief handed to the coder on fix_needed
    rca_report_path: str             # absolute path the rca worker wrote the 5-part report to; rca_report posts it
    rca_approval_decision: str       # "approve" | "reject" (set on resume); "" when auto-approved

    # --- Station 1 / 1.5 ---
    dependency_report: dict
    dependency_blocking: bool        # dep_resolver's own blocking claim; surfaced in the runner log
    reachability_report: dict        # binding artifact fk-coder must obey
    reachability_blocking: bool      # surfaced in the runner log

    # --- Station 4: code ---
    branch: str
    files_changed: int
    worktree_dir: str        # absolute path of the coder's local clone; reviewer + rework cwd here

    # --- Station 5: review loop (gated at MAX_REVIEW_ITERATIONS) ---
    review_verdict: ReviewVerdict
    review_iteration: int
    review_findings: list            # replaced each review pass

    # --- latency #1: orchestrator-owned persistent container (config.PERSISTENT_CONTAINER) ---
    container_name: str      # the booted container the Docker stations reuse ("" when not started)
    container_ready: bool    # True only if prep_container actually started it; else stations self-serve

    # --- 3.87 ---
    pr_number: int           # primary repo's PR number (back-compat; = pr_numbers[first slug])
    pr_numbers: dict         # {slug: pr_number} — one draft PR per changed repo (multi-repo tickets)
    service_repo: str        # owner/name slug the coder pushed to; used by the git/PR code nodes
    pr_title: str            # PR title the coder proposed; the code node opens the PR with it
    pr_body: str             # PR body the coder proposed

    # --- Station 6: local SIT (decomposed: resolve -> author -> qa gate -> run[+testrail] -> triage) ---
    automation_result: AutomationResult
    failure_class: FailureClass
    execution_mode: str              # local-mock-first | qat-fallback
    sit_report: dict                 # tests[] / changed_repos[] / dependencies / evidence
    test_automation_pr_url: str      # opened by the skill on pass
    sit_findings: list               # findings_for_coder (code_fault -> fk-coder)
    # sit_run Docker-resource preflight short-circuit — carried as TYPED STATE, not a marker Python
    # writes into the skill's own verdict file. When Docker can't take the chain, sit_run fails fast
    # BEFORE the (expensive) skill call and flags it here; sit_triage reads this from state (not a
    # file marker) and emits the could_not_verify verdict without its own redundant skill call.
    preflight_failed: bool
    preflight_reason: str

    # --- qa_scenarios: GAN-hardened test scenarios, authored pre-code right after reachability_gate
    # (MM-14738) — sit_author reads this back post-review and passes it to ocean-qa-agent as
    # `--use-scenarios` so the pytest is written from these, not designed fresh.
    qa_scenarios_path: str            # artifact path ocean-qa-agent wrote on `--scenarios-only`
    qa_gan_verdict: str               # APPROVE | APPROVE WITH FIXES | REJECT (Step 5d) -- surfaced at qa_review_gate
    qa_gan_phase0_gaps: list          # HIGH spec gaps from Step 2e, headless-deferred to qa_review_gate

    # --- QA review gate (human 3-way: approve+TestRail / approve / changes) ---
    qa_test_path: str                # drafted SIT file, shown to the reviewer
    qa_decision: str                 # approve_testrail | approve_no_testrail | changes
    qa_note: str                     # reviewer feedback carried back to sit_author on "changes"
    qa_review_iteration: int         # capped by MAX_QA_REVIEW_ITERATIONS
    testrail_run_id: int             # from the parallel sit_testrail branch (via state, not the verdict file)

    # --- graph-owned repo onboarding (MM-14621): unsupported ocean repo -> learn_repo -> re-run SIT ---
    needs_onboarding: bool           # Station 6 reported the changed repo is unsupported locally
    onboard_repo: str                # which repo to learn
    repo_onboarded: str              # the repo learn_repo profiled+persisted (for telemetry/report)
    onboard_attempts: int            # capped by MAX_ONBOARD_ATTEMPTS so an un-onboardable repo can't loop

    # --- code_fault full-loop budget (shared across coder re-runs) ---
    coding_attempts: int

    # --- environment_failure retry budget (sit_run only, NOT the full coder loop) ---
    # capped by MAX_ENV_RETRY_ATTEMPTS; only bumped for agent-diagnosed environment_failure, never
    # for the deterministic preflight_failed short-circuit (that never retries, see graph.py)
    env_retry_attempts: int

    # --- optional human-approval gate before ready-flip ---
    approval_decision: str   # "approve" | "reject" (set on resume); "" when the gate is off

    # --- ready-flip / terminal ---
    ready_flipped: bool
    final_status: str        # completed | failed | rca_report | awaiting_approval
    final_outcome: str
