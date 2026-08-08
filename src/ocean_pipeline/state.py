"""The single state object threaded through every node.

This replaces the ad-hoc context passing in fk-execute's prose checklist.
Each station reads what it needs and returns a partial dict; LangGraph merges
it (last-write-wins for scalars/lists). NOTE (F8, architecture review): the graph
is NOT a single sequential path — it has parallel supersteps (the prep_image/
analysis fan-out, and the `sit_run ∥ sit_testrail` fan-out). Last-write-wins is
safe only because those concurrent branches write DISJOINT keys; if you add a
branch where two concurrent nodes write the SAME key, you MUST give that key an
explicit reducer (Annotated[..., reducer]) — otherwise LangGraph raises
InvalidUpdateError on the concurrent write (it does not silently pick a winner).
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
    # Finding 3 (architecture review, "Give Pipeline Memory"): recurring failure patterns this same
    # domain_bucket has hit on PRIOR tickets (lessons.recall_lessons, written by stop_run's
    # record_failure) — [{domain_bucket, action_sig, fail_sig, recurrence_count, tickets, note}, ...].
    # Folded into _summary() so every downstream station sees it. [] when nothing recurring yet, or
    # for a route with no domain_bucket.
    recurring_lessons: list

    # --- RCA branch (diagram: RCA agent -> rca_report (plain-code Jira post) -> RCA review gate -> RCA Done -> Fix needed -> coder) ---
    rca_fix_needed: bool
    rca_findings: list               # implementation brief handed to the coder on fix_needed
    rca_report_path: str             # absolute path the rca worker wrote the 7-part report to; rca_report posts it
    # Finding 3: problems found by the mechanical report gate (`nodes._check_rca_report`) — [] when the
    # report passed. MUST be declared here: LangGraph silently DROPS any key a node returns that the
    # state schema doesn't know about, which is exactly how this went from "gate result" to dead field
    # on its first cut (a judge review caught it).
    rca_report_gate_problems: list
    # Non-empty when the gate could NOT run (checker missing/crashed/timed out) and the report was
    # therefore posted UNVERIFIED. Distinct from gate_problems, which means the gate ran and refused.
    # Declared here for the same reason as the line above — an undeclared key is silently dropped.
    rca_report_unverified: str
    rca_approval_decision: str       # "approve" | "reject" (set on resume); "" when auto-approved

    # --- Station 1 / 1.5 ---
    dependency_report: dict
    dependency_blocking: bool        # dep_resolver's own blocking claim; surfaced in the runner log
    reachability_report: dict        # binding artifact fk-coder must obey
    reachability_blocking: bool      # surfaced in the runner log
    # MM-14816 (G20 placeholder resolution). Written by dep_resolver only. resolved_placeholders here is
    # the resolver's RAW resolved set ([{placeholder,resolved_value,source,evidence}]); reachability_gate
    # VERIFIES it and writes the verified set into reachability_report (NOT back into this state key —
    # the coder reads the verified values from the report via _reachability_for_coder). unresolved:
    # [{placeholder,sources_searched,why_unresolved,blocks_ac}]; blocking_open_questions: genuine
    # product/UX decisions that stop the run BEFORE coding (graph → stop_run blocked branch).
    resolved_placeholders: list
    unresolved_placeholders: list
    blocking_open_questions: list
    # MM-14816 (G20): the human's Monitor-UI decision at blocked_review_gate on those open questions.
    # blocked_decision: "answer" | "post" | "reject". blocked_answers: free-text answers the human gave
    # (answer path) — authoritative context for the coder. open_question_caveats: the questions carried
    # forward UNANSWERED (reject path) — the coder proceeds best-effort and flags them in the PR body.
    blocked_decision: str
    blocked_answers: str
    open_question_caveats: list

    # --- Station 4: code ---
    branch: str
    files_changed: int
    worktree_dir: str        # absolute path of the coder's local clone; reviewer + rework cwd here

    # --- Station 4.5: deterministic quality gate (plain code, no agent) ---
    # Every one of these MUST be declared: LangGraph silently DROPS any key a node returns that the
    # schema doesn't know about (see the rca_report_gate_problems comment above — this repo has now
    # been bitten by that three times, twice while building gates exactly like this one).
    quality_gate_findings: list      # [{severity,file,summary,line?,fix_direction?}] — [] on a clean pass
    quality_gate_unverified: str     # non-empty when a language slice could NOT be checked (fails OPEN, loudly)
    quality_gate_checked_files: int  # evidence the gate ACTUALLY ran; 0 with files derived == inert
    quality_gate_attempts: int       # capped by MAX_QUALITY_GATE_ATTEMPTS; reset ONLY by prep_rework
    quality_gate_stopped: bool       # set only on the stop branch, so stop_run can label it without
                                     # keying on `attempts >= MAX` (which stays true for the rest of
                                     # the run and would mislabel any LATER, unrelated stop)

    # --- B1: multi-repo review coverage (harsh_reviewer has ONE cwd; open_pr acts on N) ---
    review_branch_repos: list        # DISK-derived slugs alone — the honest instrument. `covered ∪ gap`
                                     # is contaminated by service_repo, so it cannot answer "what did
                                     # the disk actually show?" on its own.
    review_repos_covered: list       # slugs a review actually ran in (always a singleton today)
    review_coverage_gap: list        # required - covered; non-empty == a repo would ship unreviewed
    review_coverage_unverified: str  # non-empty when coverage COULD NOT be derived (fails OPEN, loudly)
    review_coverage_attempts: int    # capped by MAX_COVERAGE_ATTEMPTS; reset ONLY by prep_rework
    review_coverage_stopped: bool    # set only on the stop branch — see quality_gate_stopped

    # --- D4: mechanical secret scan at the ready-flip ---
    secret_findings: list        # [{severity,file,summary,line?,fix_direction?}] — CRITICAL, blocks the flip
    secret_scan_unverified: str  # non-empty when a repo could NOT be scanned (fails OPEN, loudly)

    # --- D5/D6: independent per-node accuracy evaluation (advisory unless EVAL_ENFORCE) ---
    # Declared for the same reason as the quality_gate block above: LangGraph silently DROPS any key
    # a node returns that the schema doesn't know about, so an undeclared key here would make the
    # whole evaluator inert while every unit test on the node itself still passed.
    node_evaluations: list      # append-only [{node, accuracy, verdict, dimensions, issues}] — one per eval
    eval_gap: str               # non-empty == an ENFORCED failing evaluation; the reason, human-readable
    eval_unverified: str        # non-empty when the judge could not be run or its verdict was unreadable
    eval_attempts: int          # capped by MAX_EVAL_ATTEMPTS; reset ONLY by prep_rework
    eval_stopped: bool          # set only on the stop branch, so stop_run can label it without keying
                                # on `attempts >= MAX` (true for the rest of the run — see quality_gate)

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
    # Finding 2c (architecture review): mirrors schemas.AutomationVerdict.fidelity_rung exactly (0
    # trivial-green / 1 cross-repo-reached / 2 full-fidelity) -- see that field's own docstring for
    # the canonical rung definitions. after_sit_triage gates on this: a "passed" result at rung 0
    # routes to stop_run instead of flip_ready. Default 0 via .get(..., 0) everywhere it's read, so
    # an older run's state (recorded before this field existed) is treated as unverified, never as a
    # silent full-fidelity pass.
    fidelity_rung: int
    # Finding 2 (②-a): non-empty when a Rung-2 claim could NOT be corroborated against the
    # mock's SUT-activity audit (older mock / no audit path). Declared here because LangGraph
    # silently drops any key a node returns that the schema does not know about.
    # C3: was `fidelity_rung` actually EMITTED by the skill, or merely absent? Both arrive as 0 and
    # both stop the run, so without this the operator cannot tell "the skill is not honouring its
    # contract" (0 of 18 recorded verdicts carry the field, though SKILL.md marks it REQUIRED on
    # every PASS) from "this ticket genuinely proved nothing". Different problems, different fixes.
    rung_emitted: bool
    rung_corroboration: str
    ref_load_used: bool              # true iff the SIT used --ref-load real reference data
    sit_report: dict                 # tests[] / changed_repos[] / dependencies / evidence
    test_automation_pr_url: str      # opened by the skill on pass
    sit_findings: list               # findings_for_coder (code_fault -> fk-coder)
    # sit_run Docker-resource preflight short-circuit — carried as TYPED STATE, not a marker Python
    # writes into the skill's own verdict file. When Docker can't take the chain, sit_run fails fast
    # BEFORE the (expensive) skill call and flags it here; sit_triage reads this from state (not a
    # file marker) and emits the could_not_verify verdict without its own redundant skill call.
    preflight_failed: bool
    preflight_reason: str

    # --- SIT junit handoff (sit_run -> sit_triage): the exact run-scoped junit + whether THIS run made it.
    # MUST be declared or LangGraph drops them and sit_triage can't tell this run's evidence from a prior
    # run's (EXE-f749212a: triage scored a stale/prior junit and false-verdicted a passing run).
    sit_junit_path: str
    sit_junit_present: bool

    # --- qa_scenarios: GAN-hardened test scenarios, authored pre-code right after reachability_gate
    # (MM-14738) — sit_author reads this back post-review and passes it to ocean-qa-agent as
    # `--use-scenarios` so the pytest is written from these, not designed fresh.
    qa_scenarios_path: str            # artifact path ocean-qa-agent wrote on `--scenarios-only`
    qa_gan_verdict: str               # APPROVE | APPROVE WITH FIXES | REJECT (Step 5d) -- surfaced at qa_review_gate
    qa_gan_stop_reason: str           # D1 Step 2: converged | scores_below_threshold | plateau |
                                      # round_ceiling — WHY the GAN loop ended, not just its verdict
    qa_gan_residual_gaps: list        # D1 Step 1: residual HIGH gaps the GAN could not close, read
                                      # via the 6-name alias reader + severity filter, wired to coder
    qa_gan_phase0_gaps: list          # HIGH spec gaps from Step 2e, headless-deferred to qa_review_gate
                                      # -- always [] while Step 2e is disabled in ocean-qa-agent/SKILL.md (MM-14738)

    # --- QA review gate (human 3-way: approve+TestRail / approve / changes) ---
    qa_test_path: str                # drafted SIT file, shown to the reviewer
    # Finding 2e: sha256[:16] of qa_test_path as sit_author left it. sit_triage re-hashes before its
    # own TCNOTADDED substitution; a mismatch proves sit_run rewrote the test mid-run and forces a
    # human acknowledgement at the ready-flip. Deterministic — not a self-reported flag.
    qa_test_sha: str
    qa_decision: str                 # approve_testrail | approve_no_testrail | changes
    qa_note: str                     # reviewer feedback carried back to sit_author on "changes"
    qa_review_iteration: int         # capped by MAX_QA_REVIEW_ITERATIONS
    testrail_run_id: int             # VESTIGIAL (MM-14738): no add_run call exists in either skill, so
                                      # nothing has ever populated this from a real TestRail run. Declared
                                      # for schema back-compat only -- use qa_testrail_case_map below.
    qa_testrail_case_map: dict       # {"TCNOTADDED{N}": <real_case_id>} from sit_testrail (MM-14738); sit_triage
                                      # substitutes these into qa_test_path before the commit-skill call fires

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
