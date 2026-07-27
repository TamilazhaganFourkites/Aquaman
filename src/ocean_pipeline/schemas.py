"""Structured verdicts.

Most worker nodes write to <artifacts>/<node>.verdict.json (see agents.run_agent).
Station 6 is the exception: the ocean-automation-testing skill already defines its
own machine-readable contract (SKILL.md Station 3) and writes it to the canonical
memory/tickets/<TICKET>-automation-testing.json. AutomationVerdict mirrors that
contract exactly so the node can read the skill's own file, not re-impose a schema.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ---- generic per-node verdicts (written via run_agent's contract) -------
class ResearchVerdict(BaseModel):
    route: Literal["coding", "rca", "sop", "loft", "ff_onboarding", "unclassified"]
    packet_path: str
    target_repos: list[dict]              # [{repo, language, build_env, branch}]


class ReachabilityVerdict(BaseModel):
    report_path: str
    blocking: bool = False
    notes: str = ""


class RcaVerdict(BaseModel):
    """ocean-rca outcome. If fix_needed, the RCA hands an implementation brief to
    the coder (diagram: RCA agent -> RCA Done -> Fix needed -> coder); otherwise
    the evidence-cited report is the deliverable and the run ends."""
    report_path: str
    fix_needed: bool = False
    findings_for_coder: list = Field(default_factory=list)


class CoderVerdict(BaseModel):
    branch: str
    pushed_sha: str
    files_changed: int = 0
    # The graph opens the PR itself (deterministic code), so the coder only reports WHICH repo
    # it pushed to and the human-readable title/body to use — it never runs `gh pr create`.
    repo: str = ""            # owner/name (or bare name) of the repo the branch was pushed to
    pr_title: str = ""        # PR title the open_pr code node will use
    pr_body: str = ""         # PR body the open_pr code node will use


class ReviewVerdict(BaseModel):
    verdict: Literal["APPROVE", "CHANGES_REQUIRED"]
    findings: list[dict] = Field(default_factory=list)   # [{severity, file, summary}]


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
    failure_class: Literal["", "code_fault", "could_not_verify"] = ""
    execution_mode: str = "local-mock-first"
    tests: list[AutomationTest] = Field(default_factory=list)
    changed_repo: ChangedRepo | None = None
    dependencies: list[DependencyRun] = Field(default_factory=list)
    testrail_run_id: int = 0
    evidence: str = ""
    test_automation_pr_url: str = ""
    findings_for_coder: list = Field(default_factory=list)
