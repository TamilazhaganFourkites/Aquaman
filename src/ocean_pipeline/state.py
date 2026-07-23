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
FailureClass = Literal["", "code_fault", "could_not_verify"]


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

    # --- RCA branch (diagram: RCA agent -> RCA Done -> Fix needed -> coder) ---
    rca_fix_needed: bool
    rca_findings: list               # implementation brief handed to the coder on fix_needed

    # --- Station 1 / 1.5 ---
    dependency_report: dict
    reachability_report: dict        # binding artifact fk-coder must obey

    # --- Station 4: code ---
    branch: str
    pushed_sha: str

    # --- Station 5: review loop (gated at MAX_REVIEW_ITERATIONS) ---
    review_verdict: ReviewVerdict
    review_iteration: int
    review_findings: list            # replaced each review pass

    # --- 3.87 / 4.5b / 4.6 ---
    pr_number: int
    graph_augmented: bool
    release_intel_written: bool

    # --- Station 6: local SIT (ocean-automation-testing skill, run end-to-end) ---
    automation_result: AutomationResult
    failure_class: FailureClass
    execution_mode: str              # local-mock-first | qat-fallback
    sit_report: dict                 # tests[] / changed_repo / dependencies / evidence
    test_automation_pr_url: str      # opened by the skill on pass
    sit_findings: list               # findings_for_coder (code_fault -> fk-coder)

    # --- code_fault full-loop budget (shared across coder re-runs) ---
    coding_attempts: int

    # --- ready-flip / terminal ---
    ready_flipped: bool
    final_status: str        # completed | failed | rca_report
    final_outcome: str
