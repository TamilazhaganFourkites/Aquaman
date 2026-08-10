"""`report.finish` is a WRITER with real READERS, and nothing tested the keys they share.

The audit's list of production changes with no test at all named `report.finish`'s `service_repo`
and `pr_numbers` keys directly — and this is the one place where an untested key has already caused
a measurable wrong answer. `scripts/gan_effect.py` pre-registers `automation_result == "passed"` as
its PRIMARY dependent variable. That key was absent from the report, so the analyser silently fell
back to `final_status == "completed"` — a DIFFERENT variable — and reported a result against a
pre-registration nobody amended. A pre-registration that quietly substitutes its own outcome measure
is worse than none, because it still reads as one.

`finish` selects EXPLICIT keys rather than dumping state, which is the right design and also the
exact reason a key can go missing while every node computing it is correct. So these tests assert
the writer's output shape, and assert it against what the reader actually looks for.

Nothing here touches Jira, QAT, TestRail or Jenkins: `report.finish` writes two files into a
tmp_path and appends one line to the already-isolated timings log.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from ocean_pipeline import report

# Every RUN-REPORT key gan_effect.py consumes. Derived by reading that file, not guessed — and
# `failure_class` is deliberately NOT here: `report.finish` writes it, nothing reads it. This set
# is a FLOOR that proves the scan below still works; it is never used to filter what the scan finds.
_REPORT_KEYS_GAN_EFFECT_READS = {
    "execution_id", "automation_result", "final_status", "coding_attempts",
    "qa_gan_residual_gaps", "review_findings", "ticket",
}

# Keys the scan picks up that are NOT read from a run-report: gan_effect's own output records, its
# arm labels, and config lookups. Every entry is asserted LIVE by
# `test_the_denylist_has_no_dead_entries` — five of them were dead when this set was first written
# (the scan never emitted them), which is how a denylist quietly becomes a place to bury a real
# missing key.
_NOT_REPORT_KEYS = {"arm", "artifacts_root", "clean", "gaps", "comparisons", "underpowered",
                    "verdict", "n_total", "n_by_arm", "n_clean", "n_gaps", "clean_mean",
                    "gaps_mean", "primary_is_proxy"}

_FINAL = {
    "final_status": "completed",
    "automation_result": "passed",
    "failure_class": "",
    "coding_attempts": 2,
    "final_outcome": "shipped",
    "pr_number": 4321,
    "test_automation_pr_url": "https://example.invalid/pr/9",
    "ready_flipped": True,
    "service_repo": "cloudqwest/ocean-worker",
    "pr_numbers": {"cloudqwest/ocean-worker": 4321, "cloudqwest/tracking-service": 4322},
    "review_branch_repos": ["cloudqwest/ocean-worker", "cloudqwest/tracking-service"],
    "review_repos_covered": ["cloudqwest/ocean-worker"],
    "review_coverage_gap": ["cloudqwest/tracking-service"],
    "review_coverage_unverified": "",
    "node_evaluations": [{"node": "coder", "accuracy": 0.9}],
    "eval_unverified": "",
}


def _finish(tmp_path: Path) -> dict:
    report.start("MM-REPORT-CONTRACT", "EXE-report-contract")
    report.record("coder", 12.5, {})
    report.finish(dict(_FINAL), tmp_path)
    return json.loads((tmp_path / "run-report.json").read_text())


def test_the_primary_dependent_variable_reaches_the_report(tmp_path):
    """`automation_result` is what `gan_effect.py` pre-registered. Its absence is not a missing
    field, it is a silent change of outcome measure."""
    doc = _finish(tmp_path)
    assert doc["automation_result"] == "passed"
    assert doc["final_status"] == "completed", "the fallback variable must still be present too"
    assert doc["automation_result"] != doc["final_status"], (
        "the two are DIFFERENT variables — a test that let them coincide could not tell which one "
        "the analyser read")


def test_the_analysers_pre_registered_key_is_the_one_the_writer_emits(tmp_path):
    """Reader and writer asserted against each other, not against a literal I typed twice. If
    `gan_effect.py` renames its primary variable, or `report.py` renames the key, this fails."""
    doc = _finish(tmp_path)
    analyser = (Path(__file__).resolve().parents[1] / "scripts" / "gan_effect.py").read_text()
    keys = set(re.findall(r'\b\w+\.get\(\s*["\']([a-z_0-9]+)["\']|\b\w+\[\s*["\']([a-z_0-9]+)["\']\s*\]', analyser))
    referenced = {a or b for a, b in keys} - _NOT_REPORT_KEYS
    # THE LITERAL IS A FLOOR, NEVER A FILTER. An earlier version intersected with
    # `_REPORT_KEYS_GAN_EFFECT_READS`, which meant a key gan_effect newly started reading — but
    # nobody added to the literal — was invisible: exactly the `automation_result` defect this test
    # exists for. A judge proved it by appending `rep.get("fidelity_rung")` to the analyser and
    # watching the suite stay green. So the scan is now open-ended, and the literal only asserts
    # that the scan still WORKS (it went blind once already, when the oracle was anchored on the
    # local name `rep` and a rename emptied it).
    assert _REPORT_KEYS_GAN_EFFECT_READS <= referenced, (
        f"the key scan no longer sees {sorted(_REPORT_KEYS_GAN_EFFECT_READS - referenced)} in "
        f"gan_effect.py — it has stopped matching how that file reads the report, so it can no "
        f"longer detect a missing key")
    missing = sorted(k for k in referenced if k not in doc)
    assert not missing, (
        f"gan_effect.py reads keys report.finish never writes: {missing} — the analyser will "
        f"silently substitute a default for each one")


def test_the_multi_repo_coverage_instrument_is_recorded_whole(tmp_path):
    """B1's instrument is only interpretable if all three lists survive. `review_branch_repos` in
    particular is recorded as its own key rather than reconstructed from covered+gap, because that
    reconstruction is contaminated by `service_repo` and cannot answer "what did the disk show?"."""
    doc = _finish(tmp_path)
    assert doc["review_branch_repos"] == _FINAL["review_branch_repos"]
    assert doc["review_repos_covered"] == _FINAL["review_repos_covered"]
    assert doc["review_coverage_gap"] == _FINAL["review_coverage_gap"]
    assert doc["service_repo"] == _FINAL["service_repo"]
    assert doc["pr_numbers"] == _FINAL["pr_numbers"], (
        "every repo that got a PR must be named — a single `pr_number` cannot represent a "
        "multi-repo run, which is the shape the coverage gate exists for")


def test_could_not_measure_never_shares_a_value_with_measured_and_clean(tmp_path):
    """The standing rule, at the report boundary. `review_coverage_unverified` and
    `eval_unverified` are kept SEPARATE from the lists they describe precisely so that "the judge
    could not run" cannot be read as "the judge found nothing"."""
    doc = _finish(tmp_path)
    for key in ("review_coverage_unverified", "eval_unverified"):
        assert key in doc, f"{key} is absent, so an unverified run is indistinguishable from a clean one"

    unverified = dict(_FINAL, node_evaluations=[], eval_unverified="the judge could not be reached",
                      review_coverage_gap=[], review_coverage_unverified="git was unavailable")
    report.start("MM-REPORT-CONTRACT", "EXE-report-contract-2")
    report.finish(unverified, tmp_path)
    doc2 = json.loads((tmp_path / "run-report.json").read_text())
    assert doc2["node_evaluations"] == [] and doc2["eval_unverified"]
    assert doc2["review_coverage_gap"] == [] and doc2["review_coverage_unverified"]


def test_the_markdown_is_written_beside_the_json(tmp_path):
    """The JSON is for the analyser; the markdown is the thing a human is handed. A run that
    produced only one of them has half a report."""
    _finish(tmp_path)
    md = (tmp_path / "run-report.md").read_text()
    assert "MM-REPORT-CONTRACT" in md and "EXE-report-contract" in md
    assert "COMPLETED" in md, "the result line is missing from the human-facing report"


def test_the_gan_arm_assignment_distinguishes_absent_from_empty(tmp_path):
    """`gan_effect.py` splits runs into `gaps` / `clean` arms on `qa_gan_residual_gaps`, and treats
    ABSENT as `unknown` rather than assuming zero — correctly. So the writer must emit the key with
    a real value, and must emit `null` (not `[]`) when the run genuinely never recorded one.

    This was inert in exactly the way that is hardest to see: the analyser ran, read every report,
    and put 100% of runs in `unknown`. It produced no error, no warning, and no comparison."""
    report.start("MM-REPORT-CONTRACT", "EXE-arm-clean")
    report.finish(dict(_FINAL, qa_gan_residual_gaps=[]), tmp_path)
    clean = json.loads((tmp_path / "run-report.json").read_text())
    assert clean["qa_gan_residual_gaps"] == [], "a run WITH the key recorded must land in an arm"

    report.start("MM-REPORT-CONTRACT", "EXE-arm-gaps")
    report.finish(dict(_FINAL, qa_gan_residual_gaps=[{"id": "BUG-1"}]), tmp_path)
    gaps = json.loads((tmp_path / "run-report.json").read_text())
    assert gaps["qa_gan_residual_gaps"], "the `gaps` arm is unreachable"

    report.start("MM-REPORT-CONTRACT", "EXE-arm-unknown")
    report.finish(dict(_FINAL), tmp_path)
    unknown = json.loads((tmp_path / "run-report.json").read_text())
    assert unknown["qa_gan_residual_gaps"] is None, (
        "a run that never recorded gaps must serialize as null — coercing it to [] would put it in "
        "the `clean` arm and silently invent an observation")


def test_the_secondary_outcome_variable_is_emitted(tmp_path):
    """`len(review_findings)` is gan_effect.py's pre-registered SECONDARY variable. Absent, it
    counted 0 on every run — a constant, which cannot support or refute anything."""
    report.start("MM-REPORT-CONTRACT", "EXE-secondary")
    report.finish(dict(_FINAL, review_findings=[{"severity": "MAJOR"}, {"severity": "MINOR"}]),
                  tmp_path)
    doc = json.loads((tmp_path / "run-report.json").read_text())
    assert len(doc["review_findings"]) == 2


def test_the_credential_gates_could_not_measure_signal_reaches_the_report(tmp_path):
    """`flip_ready` fails OPEN when gitleaks is absent and its comment says the reason is "carried
    to the terminal so it can never read as 'scanned and clean'". The terminal scrolls away; this
    file is what survives — and `secret_scan_unverified` was not in it, so a run that scanned
    NOTHING produced a run-report byte-identical to one that scanned clean.

    That is the same collapse `review_coverage_unverified` and `eval_unverified` are kept separate
    for, in the one check whose whole job is finding live credentials."""
    report.start("MM-REPORT-CONTRACT", "EXE-secret-unverified")
    report.finish(dict(_FINAL, secret_scan_unverified="gitleaks is not installed — NOT scanned"),
                  tmp_path)
    unscanned = json.loads((tmp_path / "run-report.json").read_text())

    report.start("MM-REPORT-CONTRACT", "EXE-secret-clean")
    report.finish(dict(_FINAL, secret_scan_unverified="", secret_findings=[]), tmp_path)
    clean = json.loads((tmp_path / "run-report.json").read_text())

    assert unscanned["secret_scan_unverified"], "an unscanned run records no reason"
    assert not clean["secret_scan_unverified"]
    assert unscanned["secret_scan_unverified"] != clean["secret_scan_unverified"], (
        "'nothing was scanned' and 'scanned clean' are indistinguishable in the durable report")


def test_the_denylist_has_no_dead_entries():
    """A denylist is a place to hide a finding, so every entry must be earning its place.

    `_NOT_REPORT_KEYS` suppresses keys the scan legitimately picks up. Five of its original twelve
    entries were never emitted by the scan at all — two did not occur anywhere in `gan_effect.py`.
    Dead entries make the set look considered while leaving room to silence a genuinely missing
    report key with a one-word edit, which a judge demonstrated."""
    analyser = (Path(__file__).resolve().parents[1] / "scripts" / "gan_effect.py").read_text()
    found = {a or b for a, b in re.findall(
        r'\b\w+\.get\(\s*["\']([a-z_0-9]+)["\']|\b\w+\[\s*["\']([a-z_0-9]+)["\']\s*\]', analyser)}
    dead = sorted(_NOT_REPORT_KEYS - found)
    assert not dead, (
        f"these denylist entries are never emitted by the scan, so they suppress nothing and "
        f"only widen the hole: {dead}")


def test_the_floor_and_the_denylist_do_not_overlap():
    """A key in both sets is suppressed by one and demanded by the other — the test then fails for
    a reason that has nothing to do with the report. Caught exactly that while trimming the dead
    denylist entries: `ticket` is a real report key and had been added to both."""
    both = _REPORT_KEYS_GAN_EFFECT_READS & _NOT_REPORT_KEYS
    assert not both, f"these keys are both required and suppressed: {sorted(both)}"


def test_the_denylist_is_frozen_against_silent_widening():
    """The remaining way to bury a missing key: add it to the denylist. Every existing guard passes
    by construction — the entry is not dead (the scan emits it), it is not written by the report
    (so the writes-it check cannot see it), and it is not in the floor.

    So the denylist is PINNED. Growing it is a deliberate act that fails here and has to be
    justified in the same commit, which is the whole point: `_NOT_REPORT_KEYS` exists to name
    gan_effect's own output records, and that set does not change when a REPORT key goes missing."""
    assert _NOT_REPORT_KEYS == {
        "arm", "artifacts_root", "clean", "gaps", "comparisons", "underpowered", "verdict",
        "n_total", "n_by_arm", "n_clean", "n_gaps", "clean_mean", "gaps_mean", "primary_is_proxy",
    }, ("_NOT_REPORT_KEYS changed. If gan_effect gained an output record, add it here AND say so; "
        "if you are silencing a key `report.finish` should be writing, write the key instead.")


# NOT COVERED, deliberately: an INDIRECT read. A key hoisted into a constant and read through a
# variable — `_EXTRA = ("fidelity_rung",) ... rep.get(k) for k in _EXTRA` — is invisible to any
# `.get("literal")` scan. A version of this file tried to close it by scanning every string literal
# in the analyser; that flags `"completed"`, `"passed"`, `"store_true"`, `"workspace"` and
# gan_effect's own output records, so it needs an allowlist — which is the denylist problem again,
# one indirection along, and a second place to bury a key rather than none.
#
# The judgement: that escape is an adversarial construction, not plausible drift. Nobody hoists a
# report key into a tuple by accident. The DIRECT read is what actually happens and is covered, and
# `test_the_denylist_is_frozen_against_silent_widening` closes the easy way round. Stated here so
# the gap is a known limit rather than an assumed guarantee.

def test_the_denylist_cannot_suppress_a_key_the_report_actually_writes():
    """The denylist's real hazard: silencing a genuinely missing report key with a one-word edit.

    `test_the_denylist_has_no_dead_entries` stops it filling with entries the scan never emits;
    this stops the opposite abuse. If `report.finish` writes a key, that key is a report key by
    definition and may never appear in the suppression set — so the only way to quiet this test is
    to make the writer emit the key, which is the outcome it exists to force."""
    written = set(json.loads((_finish_into_tmp() / "run-report.json").read_text()))
    suppressed = sorted(_NOT_REPORT_KEYS & written)
    assert not suppressed, (
        f"these keys are written to the run report AND suppressed by the denylist, so a reader "
        f"that stops emitting one would never be caught: {suppressed}")


def _finish_into_tmp():
    import tempfile

    out = Path(tempfile.mkdtemp())
    report.start("MM-REPORT-CONTRACT", "EXE-denylist-probe")
    report.finish(dict(_FINAL), out)
    return out
