"""Structured verdicts.

Most worker nodes write to <artifacts>/<node>.verdict.json (see agents.run_agent).
Station 6 is the exception: the ocean-automation-testing skill already defines its
own machine-readable contract (SKILL.md Station 3) and writes it to the canonical
memory/tickets/<TICKET>-automation-testing.json. AutomationVerdict mirrors that
contract exactly so the node can read the skill's own file, not re-impose a schema.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, computed_field


# ---- generic per-node verdicts (written via run_agent's contract) -------
class ResearchVerdict(BaseModel):
    route: Literal["coding", "rca", "sop", "loft", "ff_onboarding", "unclassified"]
    packet_path: str
    target_repos: list[dict]              # [{repo, language, build_env, branch}]
    # Ocean domain the ticket touches, so the graph can consult the right SME node.
    domain_bucket: str = ""               # callback_notification | load_creation | ocean_tracking_milestones | ocean_data_quality | ""


class SmeVerdict(BaseModel):
    """Ocean domain SME answer: which repo/file/mechanism owns the change + reuse guidance."""
    summary: str = ""
    # Free-form guidance items — the SME worker prompts don't commit to one shape (a plain string
    # note, or a {file, note} dict), so this stays a union rather than forcing one and risking a
    # currently-valid verdict failing validation. Still excludes the clearly-wrong element types
    # (int/float/bool/None/nested list) bare `list` allowed.
    findings: list[str | dict] = Field(default_factory=list)


class ReachabilityVerdict(BaseModel):
    report_path: str
    blocking: bool = False
    notes: str = ""


class DependencyVerdict(BaseModel):
    """Station 1 (dependency resolution) outcome. Same shape as ReachabilityVerdict but a distinct
    type so the two stations don't share a schema — a dependency block and a reachability block are
    semantically different verdicts and should be validated (and evolved) independently."""
    report_path: str
    blocking: bool = False
    notes: str = ""


class RcaVerdict(BaseModel):
    """ocean-rca outcome. A human reviews the posted report at rca_review_gate before the graph
    acts on it; if fix_needed (and approved), the RCA hands an implementation brief to the coder
    (diagram: RCA agent -> RCA review gate -> RCA Done | Fix needed -> coder); otherwise the
    evidence-cited report is the deliverable and the run ends."""
    report_path: str
    fix_needed: bool = False
    # rca-research.md describes content requirements ("a concrete implementation brief: repo,
    # file, what to change, why") without committing to one JSON shape — stays a union rather
    # than risking a currently-valid verdict failing validation on a real production run.
    findings_for_coder: list[str | dict] = Field(default_factory=list)


class CoderVerdict(BaseModel):
    branch: str
    files_changed: int = 0
    # The graph opens the PR itself (deterministic code), so the coder only reports WHICH repo
    # it pushed to and the human-readable title/body to use — it never runs `gh pr create`.
    repo: str = ""            # owner/name (or bare name) of the repo the branch was pushed to
    repo_dir: str = ""        # absolute path of the local clone (reviewer + rework run here)
    pr_title: str = ""        # PR title the open_pr code node will use
    pr_body: str = ""         # PR body the open_pr code node will use


class ReviewVerdict(BaseModel):
    verdict: Literal["APPROVE", "CHANGES_REQUIRED"]
    findings: list[dict] = Field(default_factory=list)   # [{severity, file, summary}]

    # R3: the review worker emits critical_count/major_count/minor_count but often leaves them
    # null even when findings[] is non-empty, so telemetry/gating that reads counts sees nothing.
    # Derive them here from findings[] (the single source of truth) so they are ALWAYS populated
    # and can never disagree with the findings list — whatever the worker emitted is ignored.
    def _sev_count(self, sev: str) -> int:
        return sum(1 for f in self.findings
                   if isinstance(f, dict) and str(f.get("severity", "")).upper() == sev)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def critical_count(self) -> int:
        return self._sev_count("CRITICAL")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def major_count(self) -> int:
        return self._sev_count("MAJOR")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def minor_count(self) -> int:
        return self._sev_count("MINOR")


# ---- Station 6: mirrors ocean-automation-testing SKILL.md Station 3 verdict --
class AutomationTest(BaseModel):
    name: str
    testrail_id: int = 0
    result: str = ""
    detail: str = ""


class ChangedRepo(BaseModel):
    repo: str = ""
    ran_on: str = ""          # local | real-local | mocked
    port: int | None = None


class DependencyRun(BaseModel):
    repo: str = ""
    ran_on: str = ""          # mocked | real-local | qat
    reason: str = ""


class AutomationVerdict(BaseModel):
    ticket_id: str
    pr_number: int = 0
    automation_result: Literal["passed", "failed"]
    # "environment_failure" (Docker/mock/network infra broke, retriable) is distinct from
    # "could_not_verify" (structurally undiscriminable by SIT, e.g. additive defensive code) —
    # see fk-aideveloper skills/ocean-automation-testing/SKILL.md for the classification contract.
    # graph.py::after_sit_triage branches on this: an AGENT-DIAGNOSED environment_failure retries
    # sit_run once (capped by MAX_ENV_RETRY_ATTEMPTS); the deterministic resource-insufficient
    # preflight short-circuit (preflight_failed=True) never retries, since more Docker memory
    # doesn't appear between attempts.
    failure_class: Literal["", "code_fault", "could_not_verify", "environment_failure"] = ""
    execution_mode: str = "local-mock-first"
    tests: list[AutomationTest] = Field(default_factory=list)
    changed_repo: ChangedRepo | None = None
    dependencies: list[DependencyRun] = Field(default_factory=list)
    testrail_run_id: int = 0
    evidence: str = ""
    test_automation_pr_url: str = ""
    findings_for_coder: list[dict] = Field(default_factory=list)   # [{test, cause, ...}]
    # Graph-owned onboarding (MM-14621): under the Aquaman control plane the skill does NOT
    # self-clone/commit an unsupported ocean repo — it reports the gap here and the graph's
    # learn_repo node owns the decision + persistence. Absent/false on a normal run, so a skill
    # that never emits these is fully backward-compatible.
    needs_onboarding: bool = False
    onboard_repo: str = ""            # the ocean repo the SIT could not run because it is unsupported
    # AC traceability (ocean-qa-agent-ac-driven-plan.md): which acceptance criterion each test verifies.
    # Additive + optional — a skill that doesn't emit this yet is fully backward-compatible.
    ac_coverage: list[dict] = Field(default_factory=list)   # [{"ac": "AC3", "test": "test_...", "result": "passed"}]
