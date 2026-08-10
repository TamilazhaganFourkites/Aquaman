"""Structured verdicts.

Most worker nodes write to <artifacts>/<node>.verdict.json (see agents.run_agent).
Station 6 is the exception: the ocean-automation-testing skill already defines its
own machine-readable contract (SKILL.md Station 3) and writes it to the canonical
memory/tickets/<TICKET>-automation-testing.json. AutomationVerdict mirrors that
contract exactly so the node can read the skill's own file, not re-impose a schema.
"""
from __future__ import annotations

import re

from typing import Literal

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator


# ---- generic per-node verdicts (written via run_agent's contract) -------
# The ocean domain buckets, in ONE place. `nodes._SME_BY_BUCKET` maps each to its SME file and is
# asserted against this set at import (nodes.py) — a bucket added to one and not the other is the
# fork class this codebase keeps rediscovering, and it degrades silently to "no SME dispatched".
# Lives here rather than in nodes.py because nodes imports schemas, not the reverse.
DOMAIN_BUCKETS: frozenset[str] = frozenset({
    "callback_notification",
    "load_creation",
    "ocean_tracking_milestones",
    "ocean_data_quality",
    "jt_data_quality",
    "event_processing_failure",
})


class ResearchVerdict(BaseModel):
    route: Literal["coding", "rca", "sop", "loft", "ff_onboarding", "unclassified"]
    packet_path: str
    target_repos: list[dict]              # [{repo, language, build_env, branch}]
    # Ocean domain the ticket touches, so the graph can consult the right SME node.
    # Validated below — this used to be a bare `str` whose vocabulary lived in a COMMENT, one line
    # under `route`, which IS a Literal.
    domain_bucket: str = ""
    # What the researcher said, iff it matched no known bucket -- otherwise "". `sme_consult` logs
    # the already-blanked `domain_bucket`, so without this a typo'd bucket and a ticket with no
    # bucket both read as "(none)" and a misrouted ticket looks correctly skipped.
    #
    # This MUST stay on the verdict rather than in a module-level record: `qa_batch` runs every
    # ticket of a batch in ONE process, so anything module-level reports the first ticket's typo
    # against every later bucket-less ticket. DERIVED, never model-supplied -- the validator below
    # writes it unconditionally.
    domain_bucket_raw: str = ""

    @model_validator(mode="before")
    @classmethod
    def _keep_unmatched_bucket(cls, data):
        """Stash the raw token BEFORE the field validator canonicalizes it away."""
        if isinstance(data, dict):
            raw = str(data.get("domain_bucket") or "").strip()
            data = {**data, "domain_bucket_raw": "" if cls._canonical_bucket(raw) else raw}
        return data

    @field_validator("domain_bucket", mode="before")
    @classmethod
    def _canonical_bucket(cls, v):
        """Canonicalize to a known bucket, or "" — never raise.

        NOT a `Literal`, deliberately, even though `route` above is one. This value is LLM-authored:
        a Literal makes a single typo a ValidationError that fail-parses the ENTIRE ResearchVerdict,
        losing the route, the packet path and the target repos along with it. That is a strictly
        worse outcome than the defect it would prevent, and it is the failure mode every validator in
        this file is written to avoid.

        So near-misses are RECOVERED (case, hyphens, surrounding whitespace/punctuation) and anything
        still unrecognised degrades to "" — which `sme_consult` already handles: no SME is dispatched
        and a `skip` station_event records the bucket it could not match. The consequence of a real
        typo is unchanged (no SME); what changes is that `Ocean_Data-Quality` now finds its SME
        instead of silently skipping it."""
        if not isinstance(v, str):
            return ""
        token = v.strip().strip(".,;:'\"`").lower().replace("-", "_").replace(" ", "_")
        return token if token in DOMAIN_BUCKETS else ""


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
    # MM-14816 (G20 placeholder resolution): a `[ENGINEER TO FILL]` / fix-critical blank the resolver
    # RESOLVED from code/logs/comments (each {placeholder, resolved_value, source, evidence}). Carried
    # on the VERDICT (not just the report file) so the dep_resolver NODE can thread it into state →
    # reachability worker (which verifies it) → coder. A report-file-only record reaches nobody (the
    # reachability/coder workers are banned from cross-station file reads) — that was the MM-14457 miss.
    resolved_placeholders: list = Field(default_factory=list)
    # Blanks the resolver could NOT ground from any source (each {placeholder, sources_searched,
    # why_unresolved, blocks_ac}). Non-blocking ones are surfaced; blocking ones go below.
    unresolved_placeholders: list = Field(default_factory=list)
    # Genuine product/UX/business decisions (not discoverable facts) that BLOCK an AC — the graph stops
    # BEFORE coding and raises these on the Jira ticket (see nodes.stop_run blocked branch + cli marker).
    blocking_open_questions: list[str] = Field(default_factory=list)


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
    # EVERY repo the branch was pushed to, comma-separated when there is more than one
    # ("cloudqwest/ocean-worker, cloudqwest/tracking-service"). `nodes._service_slugs` splits this
    # and opens one PR per repo — its docstring has always asserted the comma-joined contract, but
    # NOTHING TOLD THE CODER: this comment said "the repo" (singular) and code.md said "the `repo`
    # you pushed to". So the likely multi-repo outcome was never "N-1 PRs ship unreviewed" — it was
    # one PR plus an ORPHANED branch on every other repo, with the ticket reading as delivered.
    repo: str = ""
    repo_dir: str = ""        # absolute path of the local clone (reviewer + rework run here)
    pr_title: str = ""        # PR title the open_pr code node will use
    pr_body: str = ""         # PR body the open_pr code node will use


# --------------------------------------------------------------- severity, canonicalized ONCE
# F2 and F3 are supposed to be independent gates: F2 downgrades an APPROVE that still carries
# CRITICAL/MAJOR findings, F3 refuses to stop-with-approval while blocking findings are open. A review
# found they were not independent at all — both compared `str(f.get("severity","")).upper()` against a
# literal tuple, duplicated in two files, so ONE malformed severity string defeated BOTH in lockstep:
#     {'severity': ' CRITICAL '}   -> not blocking, verdict stays APPROVE
#     {'severity': 'blocker'}      -> not blocking, verdict stays APPROVE
#     {'severity': 'P0'}           -> not blocking, verdict stays APPROVE
#     {'Severity': 'CRITICAL'}     -> not blocking, verdict stays APPROVE
# These are LLM-authored strings; a trailing space or a vocabulary slip is not an exotic input. One
# canonicalizer, imported by both, is the actual fix — the duplication WAS the defect.
BLOCKING_SEVERITIES = frozenset({"CRITICAL", "MAJOR"})

# Vocabulary a reviewer might reasonably reach for instead of the canonical three.
_SEVERITY_ALIASES = {
    "BLOCKER": "CRITICAL", "BLOCKING": "CRITICAL", "FATAL": "CRITICAL", "SEVERE": "CRITICAL",
    "P0": "CRITICAL", "S0": "CRITICAL", "SEV0": "CRITICAL",
    # "SEV" alone: the first-word split below turns "SEV-0"/"SEV 1" into "SEV", and an unqualified
    # severity marker is the reviewer flagging something serious, not a nit.
    "SEV": "CRITICAL",
    "HIGH": "MAJOR", "P1": "MAJOR", "S1": "MAJOR", "SEV1": "MAJOR", "IMPORTANT": "MAJOR",
    "MEDIUM": "MINOR", "LOW": "MINOR", "MINOR/NIT": "MINOR", "NIT": "MINOR", "NITPICK": "MINOR",
    "INFO": "MINOR", "INFORMATIONAL": "MINOR", "TRIVIAL": "MINOR", "SUGGESTION": "MINOR",
    "P2": "MINOR", "P3": "MINOR", "S2": "MINOR", "S3": "MINOR",
}


def gate_decision(raw, allowed: tuple[str, ...]) -> str:
    """The human's decision at a resume gate, as one of `allowed`, or "" if it is not one of them.

    THE GATES ARE NOT INTERCHANGEABLE AND THE RESUME FLAGS ARE NOT VALIDATED AGAINST THE PAUSED ONE.
    `cli.py`'s flag dispatch is a flat if/elif that never asks which gate is waiting, so
    `--blocked reject` — a flag for a DIFFERENT gate — reaches a paused human_gate as the DICT
    `{"decision": "reject", "note": None}`. `nodes.human_gate` then stores `str(decision)`, and
    `after_human_gate` prefix-tested that string:

        str({"decision": "reject", ...}).startswith("reject")   ->  False   (it starts "{")
        ->  "approve"  ->  the service PR is FLIPPED READY-FOR-REVIEW

    A reject read as an approve, from one keystroke, irreversibly. `argparse choices=` does not help;
    it is what PRODUCES the well-formed dict that defeats the prefix test. Measured before the fix:
    `--blocked reject`, `--blocked post`, `--qa changes` and an unset decision ALL routed to approve.

    So: unwrap the dict, match EXACT tokens and return "" for anything unrecognised, so each router
    can pick its own fail-safe.

    EXACT means exact. It does NOT mean tolerant: it said
    `"rejected-by-x"` and `"reject"` are "the same intent". They are — but this function does NOT
    treat them as such. `"rejected-by-x"` normalizes to `rejected_by_x`, is not in `allowed`, and
    returns `""`. What that means depends on the caller, and it is NOT uniform: the three
    reject-gates pass `("reject", "rejected")` and read `""` as not-a-rejection, so a suffixed
    value would fail OPEN there; `graph.after_human_gate` passes `("approve", "approved")` and
    reads `""` as reject, so the same value fails CLOSED. (Do not describe this as uniform:
    said "every caller here passes `("reject", "rejected")`", which is false and inverts the
    consequence for the fourth.) Nothing produces such a value today (`cli.py`'s `argparse choices=` sees to that),
    and widening the match is how the dict bug got in, so the behaviour stands. The docstring is
    corrected instead, because a comment that describes tolerance the code lacks is how the next
    person adds a suffix and never learns it was dropped. This mirrors `blocked_review_gate`
    (`nodes.blocked_review_gate`), the one gate that already survived this, and follows `normalize_severity` above:
    one canonicalizer, imported by every consumer, because the duplication IS the defect."""
    if isinstance(raw, dict):
        raw = raw.get("decision", "")
    token = str(raw or "").strip().strip("-_ ").lower().replace("-", "_")
    return token if token in allowed else ""


def normalize_severity(finding) -> str:
    """A finding's severity as one of CRITICAL / MAJOR / MINOR / "" (absent).

    Tolerates what LLM-authored JSON actually contains: surrounding whitespace, markdown emphasis
    (`**CRITICAL**`), a trailing colon, any capitalization, a non-canonical key (`Severity`), a
    single-element list, and the alias vocabulary above.

    UNRECOGNIZED severities return "" and do NOT block. Defaulting them to CRITICAL was tried and was
    the wrong direction: a judge probe found 48 of 55 plausible strings then blocked an APPROVE,
    including ones meaning the OPPOSITE — `none`, `N/A`, `resolved`, `FIXED`, `non-blocking`,
    `cosmetic`, `FYI`. And because F2 and F3 now share this predicate, a single such string wedged
    BOTH: round 1 `rework`, budget exhausted, `stop`, no PR — the exact lockstep coupling this
    function exists to remove. review.md tells the reviewer to re-verify prior findings on round 2+,
    which is precisely where `resolved`/`FIXED` appear.

    The value here is the CANONICALIZATION — whitespace, markdown emphasis, case, a `Severity` key, a
    one-element list, and the alias vocabulary — not a guess about unknown words. On an unmapped
    string this returns exactly what the old inline comparison did (non-blocking), so the fix is
    strictly an improvement over the previous behaviour and never a new way to block."""
    if not isinstance(finding, dict):
        return ""
    raw = None
    for k, v in finding.items():
        if isinstance(k, str) and k.strip().lower() == "severity":
            raw = v
            break
    if isinstance(raw, (list, tuple)):
        raw = raw[0] if len(raw) == 1 else None
    if raw is None:
        return ""
    s = str(raw).strip().strip("*_`#:.-").strip().upper()
    s = " ".join(s.split())
    # Some reviewers write the label into the value ("Severity: CRITICAL"); drop it so the
    # canonical match below sees the actual level.
    if s.startswith("SEVERITY") and len(s) > 8:
        s = s[8:].lstrip(": -").strip()
    if not s:
        return ""
    if s in ("CRITICAL", "MAJOR", "MINOR"):
        return s
    if s in _SEVERITY_ALIASES:
        return _SEVERITY_ALIASES[s]
    # A prefix match catches "CRITICAL_BUG" / "MAJOR - correctness" / "critical (data loss)".
    for canon in ("CRITICAL", "MAJOR", "MINOR"):
        if s.startswith(canon):
            return canon
    # An alias appearing as the FIRST word ("P0 - data loss", "blocker: nil deref").
    head = re.split(r"[\s:_\-/(]", s, maxsplit=1)[0]
    if head in _SEVERITY_ALIASES:
        return _SEVERITY_ALIASES[head]
    return ""   # unmapped -> non-blocking; see the docstring for why CRITICAL was wrong here


def is_blocking_finding(finding) -> bool:
    """True when this finding must block an APPROVE. The single predicate F2 and F3 both use."""
    return normalize_severity(finding) in BLOCKING_SEVERITIES


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
        return sum(1 for f in self.findings if normalize_severity(f) == sev)

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

    @model_validator(mode="after")
    def _gate_verdict_on_severity(self) -> "ReviewVerdict":
        # F2 (Aquaman architecture review): the "severity gate computed then discarded" gap — the review
        # worker can emit verdict=APPROVE while findings[] still holds CRITICAL/MAJOR items, and nothing
        # cross-checks the two. critical_count/major_count are the single source of truth (derived from
        # findings above), and the reviewer's own contract is "APPROVE only at zero CRITICAL and zero
        # MAJOR" — so enforce that HERE in code instead of trusting the model: an APPROVE that disagrees
        # with the counts is downgraded to CHANGES_REQUIRED. This makes "APPROVE with open CRITICAL/MAJOR"
        # structurally impossible; every consumer (state review_verdict, telemetry, graph.after_review)
        # then sees a verdict consistent with the findings, and after_review routes to rework/stop with no
        # change of its own. (Fail-safe direction only — it can never UPgrade CHANGES_REQUIRED to APPROVE.)
        if self.verdict == "APPROVE" and (self.critical_count > 0 or self.major_count > 0):
            self.verdict = "CHANGES_REQUIRED"
        return self


# ---- Station 6: mirrors ocean-automation-testing SKILL.md Station 3 verdict --
class AutomationTest(BaseModel):
    name: str = ""            # was required; defaulted so a test element missing `name` can't crash the run
    testrail_id: int = 0
    result: str = ""          # "passed" | "failed" | "needs_env" (S4/I12: a Step 9b CANNOT-VERIFY method,
                               # never executed against the mock — not a pass, not a fail). Free str, not a
                               # Literal, so one unexpected value can't fail-parse the whole verdict.
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
    # Finding 2d: kept a free str (an unexpected value must never fail-parse a whole verdict — the
    # convention every other field here follows), but NO LONGER unvalidated: _coerce_execution_mode
    # below normalizes the two real values and maps anything else to "unknown", which
    # nodes._real_service_gap treats as "cannot prove how this ran" and routes to a human. Before
    # this, the field was recorded and displayed but read by nothing at all — the finding's exact
    # complaint ("the verdict knows it ran on mocks; nothing reads it").
    execution_mode: str = "local-mock-first"
    # Fidelity ladder (architecture review of PR Aquaman#4/fk-aideveloper#294, Finding 2c): mirrors
    # fk-aideveloper skills/ocean-qa-agent/references/local_service_execution.md's 3-rung definition
    # EXACTLY (that doc is the single source of truth for what each rung means — do not redefine it
    # here). 0 = trivial green (SUT errored before touching the receiving service; the assertion held
    # by inaction). 1 = cross-repo reached (SUT calls the receiving service, but the reviewed logic
    # branch is short-circuited). 2 = full fidelity (the reviewed logic ran end-to-end against real
    # local services). Defaults to 0 -- the SAFEST/most-suspicious value -- so a skill run that hasn't
    # been updated yet to emit this field is treated as UNVERIFIED (gated, see graph.py::
    # after_sit_triage) rather than silently trusted the way a passed-with-no-rung-field run was
    # before this field existed. This was previously defined in prose only and never wired into the
    # verdict or any gate -- a mock-only false-pass (catch-all 200 / manufactured polled value) could
    # flip a PR to ready with nobody able to tell from the verdict alone.
    fidelity_rung: int = 0
    # Finding 2d (PARTIAL — see needs_ref_load below): true iff the SIT used --ref-load real
    # reference data, not just the mock's synthesized defaults. Read ONLY inside
    # _cap_rung_on_missing_ref_load's three-way self-contradiction check; never gated on directly.
    ref_load_used: bool = False
    # Finding 2d. `execution_mode` IS now gated: _coerce_execution_mode maps an unrecognized value to
    # "unknown", and nodes._real_service_gap refuses the automatic ready-flip on it (as it does on
    # fidelity_rung < 2, and on a changed repo that did not run for real / none recorded at all). What
    # "a real-service run" means here is DELIBERATELY defined as "the repos under review actually
    # executed for real, and the reviewed logic ran end-to-end (Rung 2)" -- not "QAT was used", since
    # the ocean SIT's whole design is to run every CHANGED repo real locally and mock the rest, and QAT
    # is simply a different real environment. A run that cannot demonstrate that goes to a human
    # instead of auto-flipping. The narrower needs_ref_load contradiction check below is an ADDITIONAL
    # signal on top of that gate, not the gate itself.
    #
    # What needs_ref_load itself means: local_service_execution.md's fidelity ladder requires real-load replay
    # for Rung 2 ONLY on "deep-engine" tests -- a test whose reviewed logic is a real business-logic
    # computation (load-refresh/enrichment, ETA calculation, milestone processing, etc.) that only
    # behaves realistically against real-shaped reference data, as opposed to a simple pass-through/
    # CRUD assertion where the mock's synthesized defaults are already sufficient for Rung 2. Rather
    # than guess this from repo/file names (an unreliable heuristic, and a repo can hold both kinds of
    # logic), the classifying skill self-declares it explicitly: true iff THIS test's Rung-2 claim
    # specifically depends on deep-engine logic. Defaults False (no claim of needing it -- most tests).
    # The validator below only ever downgrades a SELF-CONTRADICTION (claimed deep-engine + claimed
    # Rung 2 + didn't use ref-load), never a test that genuinely doesn't need ref-load for Rung 2 --
    # so this can't wrongly punish a legitimately-Rung-2 simple test the way a blanket rung-vs-
    # ref_load_used check would have.
    needs_ref_load: bool = False
    # ── Finding 2e: a pass that FOLLOWS a test edit is not the same as a pass ──────────────────────
    # The review: "the agent that calls a failure `test_fault` then edits the test and re-runs it" —
    # SKILL.md legitimately allows fixing a stale/wrong test and re-running (capped at 2), but nothing
    # recorded that it happened, so a first-try pass and a pass-after-rewriting-the-assertion were
    # indistinguishable in the verdict. Worse, `_coerce_failure_class` maps test_fault -> "" (it is
    # not a terminal failure), which ERASED the only trace. `_capture_test_edit_signal` below now
    # latches test_edited=True from that same raw value BEFORE the coercion runs, so the signal
    # survives even if the skill forgets to set it, and `nodes._test_edit_ack_reason` forces a human
    # acknowledgement before the ready-flip.
    test_edited: bool = False          # a test_fault fix + re-run happened somewhere in this run
    test_diff: str = ""               # `git diff` of the test file across that edit (truncated is fine)
    ac_before: list[str] = Field(default_factory=list)   # AC ids the test cited BEFORE the edit
    ac_after: list[str] = Field(default_factory=list)    # AC ids it cites AFTER — a shrink is a red flag
    # Finding 2a: any request path that fell through ocean_mock_helper.py's lenient catch-all. The
    # skill reports what it saw here, but it is not the only source — the mock writes its own deduped
    # audit to <artifacts>/<exec_id>/unmocked_paths.json and nodes._read_unmocked_paths unions that in
    # at triage, so a skipped self-report is usually still caught. (Caveat, stated honestly: if the mock
    # was launched with no OCEAN_PIPELINE_EXEC_ID in env, it writes no audit at all, and "audit
    # disabled" then looks identical to "zero unmocked paths".) Non-empty means
    # some collaborator call got a synthesized `__unmocked__` 200 rather than a genuine answer — a
    # negative-path AC routed there couldn't be exercised. Additive/optional; [] is backward-compatible.
    unmocked_paths_hit: list[str] = Field(default_factory=list)
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

    @model_validator(mode="before")
    @classmethod
    def _capture_test_edit_signal(cls, data):
        """Finding 2e: latch `test_edited` from a raw `failure_class: "test_fault"` BEFORE
        _coerce_failure_class erases it to "". Runs mode="before" specifically so it sees the
        worker's original value. Never clears an explicitly-set test_edited=True; only ever turns it
        ON, so a skill that reports the edit properly and one that only reports test_fault both end
        up with the signal the ready-flip gate needs."""
        if isinstance(data, dict):
            raw = data.get("failure_class")
            if isinstance(raw, str) and raw.strip().lower() == "test_fault":
                data = {**data, "test_edited": True}
        return data

    @field_validator("execution_mode", mode="before")
    @classmethod
    def _coerce_execution_mode(cls, v):
        """Finding 2d: normalize the two documented values; map anything else (including None/
        non-str) to "unknown" rather than letting an unrecognized mode read as if it were a known,
        trusted one. "unknown" is not inert — nodes._real_service_gap refuses the automatic ready-flip
        on it, so a verdict that can't say how it executed goes to a human instead of through."""
        if not isinstance(v, str):
            return "unknown"
        s = v.strip().lower().replace("_", "-")
        if s in ("local-mock-first", "qat-fallback"):
            return s
        return "unknown" if s else "local-mock-first"

    @field_validator("fidelity_rung", mode="before")
    @classmethod
    def _coerce_fidelity_rung(cls, v):
        """Only 0/1/2 are canonical (see the field's own docstring). Any non-numeric value, or a
        number outside that range, coerces to 0 — the safest/most-suspicious value — rather than
        raising and failing the whole verdict parse over a worker's vocabulary slip, and rather than
        silently accepting an out-of-range number as if it meant something. Fail-safe in the SAME
        direction as _coerce_result/_coerce_failure_class: an unparseable rung can never accidentally
        read as fully-verified."""
        try:
            n = int(v)
        except (TypeError, ValueError):
            return 0
        return n if n in (0, 1, 2) else 0

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

    @model_validator(mode="after")
    def _cap_rung_on_unmocked_hit(self) -> "AutomationVerdict":
        """Finding 2a (judge review of this same fix): the skill is INSTRUCTED (SKILL.md's
        "Check for unmocked-catch-all hits") to cap fidelity_rung at 1 itself when
        unmocked_paths_hit is non-empty, but nothing enforced that -- a self-report the model could
        just as easily forget under time pressure, exactly the class of gap ReviewVerdict's own
        _gate_verdict_on_severity above exists to close for review findings. Enforce it here instead
        of trusting the prose instruction: any non-empty unmocked_paths_hit structurally caps the
        rung, regardless of what the skill claimed. Fail-safe direction only (can only lower the
        rung, never raise it)."""
        if self.unmocked_paths_hit and self.fidelity_rung > 1:
            self.fidelity_rung = 1
        return self

    @model_validator(mode="after")
    def _cap_rung_on_missing_ref_load(self) -> "AutomationVerdict":
        """Finding 2d, PARTIAL only — this does NOT "require a real-service run before a PR goes to
        review" (the review's actual fix text); see the needs_ref_load field's own comment above for
        exactly what's still missing. The skill self-declares needs_ref_load
        when THIS test's Rung-2 claim specifically depends on deep-engine logic (see that field's own
        docstring for why this isn't inferred/guessed here). If it claims BOTH "this needs real-load
        replay for full fidelity" AND fidelity_rung == 2 AND ref_load_used is False, that's a direct
        self-contradiction — not a guess about which tests need ref-load, just enforcing the skill's
        own stated requirement against its own stated result. Downgrade to Rung 1 (cross-repo
        reached, real signal just not the full deep-engine path) rather than trusting the model to
        remember its own rule under time pressure — same rationale as _cap_rung_on_unmocked_hit
        above. Never touches a verdict where needs_ref_load is False (the overwhelming majority of
        tests, which never claimed to need it) or where ref_load_used is already True."""
        if self.needs_ref_load and self.fidelity_rung == 2 and not self.ref_load_used:
            self.fidelity_rung = 1
        return self


# ---- D5/D6: the node-accuracy evaluator's contract ----------------------
# The worker prompt (fk-aideveloper skills/ocean-coding-agent/workers/node-evaluator.md) ends with
# "Return your judgment in the NodeEvaluation contract appended below" -- `run_agent` is what
# appends it, by injecting this model's JSON schema via VERDICT_INSTRUCTION. The spec was written
# end to end (3 scored dimensions, a PASS/WARN/FAIL enum, per-node rubrics for 8 named nodes) and
# monitor/app.py already carries handling for its output; only this contract and its driver were
# missing.
EVAL_VERDICTS: tuple[str, ...] = ("PASS", "WARN", "FAIL")


def eval_verdict(raw) -> str:
    """One of EVAL_VERDICTS, or "" when the judge did not return a recognizable one.

    Deliberately NOT a `Literal`: a Literal makes ONE typo fail-parse the ENTIRE evaluation, so a
    judge that scored all three dimensions and wrote "Pass." would yield no evaluation at all rather
    than a usable one. Same reasoning as `gate_decision` and `normalize_severity` above -- one
    canonicalizer, imported by every consumer, because the duplication IS the defect.

    "" is meaningful and is NOT a PASS: it means the judge ran but its verdict could not be read,
    which `_eval_gate` must treat as "could not measure" rather than "measured and passed"."""
    if isinstance(raw, dict):
        raw = raw.get("verdict", "")
    if isinstance(raw, list) and len(raw) == 1:
        raw = raw[0]
    token = str(raw or "").strip().strip("*_ .:").upper()
    return token if token in EVAL_VERDICTS else ""


class EvalDimensions(BaseModel):
    """The three dimensions node-evaluator.md scores 0-100.

    `None` is the default, not 0. "The judge did not report this dimension" and "the judge scored it
    zero" are opposite facts, and a 0 default would silently convert the first into the second --
    the recurring defect this codebase keeps paying for ("could not measure" and "measured and
    failed" must never share a number)."""
    correctness: int | None = None
    completeness: int | None = None
    grounding: int | None = None


class NodeEvaluation(BaseModel):
    """One independent accuracy judgment of one node's output. ADVISORY unless EVAL_ENFORCE is on."""
    node: str = ""
    accuracy: int | None = None          # 0-100; None == not reported (see EvalDimensions)
    verdict: str = ""                    # canonicalized to PASS/WARN/FAIL, or "" if unreadable
    dimensions: EvalDimensions = Field(default_factory=EvalDimensions)
    issues: list = Field(default_factory=list)
    rationale: str = ""

    @field_validator("verdict", mode="before")
    @classmethod
    def _canonical_verdict(cls, v):
        return eval_verdict(v)

    @field_validator("accuracy", mode="before")
    @classmethod
    def _sane_accuracy(cls, v):
        """Out-of-range or non-numeric becomes None ("not reported"), never a clamped number: a
        judge that emits 150 or "high" has not given a score on this scale, and inventing 100 from
        it would manufacture a pass."""
        if v is None or isinstance(v, bool):
            return None
        try:
            n = int(float(v))
        except (TypeError, ValueError):
            return None
        return n if 0 <= n <= 100 else None
