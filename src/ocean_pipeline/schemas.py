"""Structured verdicts.

Most worker nodes write to <artifacts>/<node>.verdict.json (see agents.run_agent).
Station 6 is the exception: the ocean-automation-testing skill already defines its
own machine-readable contract (SKILL.md Station 3) and writes it to the canonical
memory/tickets/<TICKET>-automation-testing.json. AutomationVerdict mirrors that
contract exactly so the node can read the skill's own file, not re-impose a schema.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, computed_field, field_validator


# ---- generic per-node verdicts (written via run_agent's contract) -------
class ResearchVerdict(BaseModel):
    route: Literal["coding", "rca", "sop", "loft", "ff_onboarding", "unclassified"]
    packet_path: str
    target_repos: list[dict]              # [{repo, language, build_env, branch}]
    # Ocean domain the ticket touches, so the graph can consult the right SME node.
    domain_bucket: str = ""               # callback_notification | load_creation | ocean_tracking_milestones | ocean_data_quality | jt_data_quality | event_processing_failure | ""


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

    @field_validator("verdict", mode="before")
    @classmethod
    def _normalize_verdict(cls, v):
        """Normalize the near-miss forms a reviewer naturally writes so a wording variant can't crash
        Station 5 (A1-5): case, and APPROVED→APPROVE / CHANGES_REQUESTED→CHANGES_REQUIRED. Anything that
        isn't clearly an approval maps to CHANGES_REQUIRED (fail-safe — never auto-approve on ambiguity)."""
        if not isinstance(v, str):
            return v
        s = v.strip().upper().replace("-", "_").replace(" ", "_")
        if s in ("APPROVE", "APPROVED", "APPROVAL", "LGTM", "PASS"):
            return "APPROVE"
        if s in ("CHANGES_REQUIRED", "CHANGES_REQUESTED", "REQUEST_CHANGES", "REJECT", "REJECTED", "FAIL"):
            return "CHANGES_REQUIRED"
        return "CHANGES_REQUIRED"

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
    name: str = ""            # was required; defaulted so a test element missing `name` can't crash the run
    testrail_id: int = 0
    result: str = ""
    detail: str = ""


class ChangedRepo(BaseModel):
    repo: str = ""
    ran_on: str = ""          # local | real-local — a CHANGED repo is always run real (never mocked; a
                              # mocked repo belongs in dependencies[]). Kept a free str (not a Literal) so
                              # one unexpected value can't fail-parse the whole verdict.
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
    # MM-14628: a ticket's PR can change 1..N ocean repos and the SIT runs EVERY changed repo real
    # (see ocean-automation-testing SKILL.md §2 + Station 3 verdict `changed_repos[]`).
    changed_repos: list[ChangedRepo] = Field(default_factory=list)
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

    @field_validator("changed_repos", "dependencies", "tests", "findings_for_coder", mode="before")
    @classmethod
    def _coerce_list_fields(cls, v, info):
        """Tolerate the shapes a triage worker naturally emits, so a run that ACTUALLY completed the SIT
        never dies at the finish line on a JSON-shape nit (EXE-968500e9: a 70-min run FAILED only because
        the verdict serialized `changed_repos` as bare strings and `dependencies` as null). Works for
        single- AND multi-repo tickets — it maps a list of ANY length:
          * `null` → `[]`  (a worker often nulls an empty list field)
          * repo-bearing lists (`changed_repos`/`dependencies`): a bare string `"repo-name"` (the
            CHANGED_REPOS input form) → `{"repo": "repo-name"}`
          * `tests`: a bare string `"test_x"` → `{"name": "test_x"}` (A1-4)
          * already-correct objects pass through untouched."""
        if v is None:
            return []
        if isinstance(v, list):
            if info.field_name in ("changed_repos", "dependencies"):
                return [{"repo": x} if isinstance(x, str) else x for x in v]
            if info.field_name == "tests":
                return [{"name": x} if isinstance(x, str) else x for x in v]
        return v

    @field_validator("automation_result", mode="before")
    @classmethod
    def _coerce_result(cls, v):
        """Only `passed`/`failed` are canonical, but the skill's prose invites `could_not_run` /
        `could_not_verify` / `error` at the top level (A1-2). Map every non-pass to `failed` (the real
        pass/fail lives in tests[]; the failure NUANCE lives in failure_class), so a vocabulary slip
        can't crash the run at model_validate. FAIL-SAFE: only the exact `passed`/`pass` count as a pass —
        NOT weak synonyms like `ok`/`green` (which a worker might mean as 'the process ran ok', not 'tests
        passed'), so this validator can never manufacture a false-green flip to ready (judge MINOR-4)."""
        if not isinstance(v, str):
            return v
        return "passed" if v.strip().lower() in ("passed", "pass") else "failed"

    @field_validator("failure_class", mode="before")
    @classmethod
    def _coerce_failure_class(cls, v):
        """Keep only the 4 canonical classes; fold the skill's other triage words in (A1-3): `test_fault`
        is never terminal → "" (cleared); `could_not_run` → `could_not_verify`; an obvious code-fault
        SYNONYM → `code_fault` so the coder rework loop still fires (judge MINOR-5: `after_sit_triage`
        loops back to the coder ONLY on exact `code_fault`); anything else unknown → `could_not_verify`
        (the safe non-looping terminal). None/"" stays ""."""
        if not isinstance(v, str):
            return ""
        s = v.strip().lower()
        if s in ("", "code_fault", "could_not_verify", "environment_failure"):
            return s
        if s == "test_fault":
            return ""
        if s in ("code_defect", "coder_fault", "code_bug", "bug", "defect", "codefault"):
            return "code_fault"
        return "could_not_verify"
    # AC traceability (ocean-qa-agent-ac-driven-plan.md): which acceptance criterion each test verifies.
    # Additive + optional — a skill that doesn't emit this yet is fully backward-compatible.
    ac_coverage: list[dict] = Field(default_factory=list)   # [{"ac": "AC3", "test": "test_...", "result": "passed"}]
