"""D1 Step 1 — the GAN's residual HIGH gaps finally reach the coder.

`qa_scenarios` runs BEFORE any code exists and produces scenarios that nodes.py itself calls "the
fixed target coder must satisfy" — but the coder never saw them. Confirmed by four independent
traces in important-notes/gan-decision.md (judge-approved v5, after v1/v2/v3 were rejected).

The shapes below are the REAL ones measured across the eight surviving artifacts, not invented:
six key names of which only FOUR are residual-gap lists, inner field drift on both the summary and
the verification field, and a nested `gan_rounds[]` form whose early rounds use a DIFFERENT key for
gaps that were already fixed.

The failure mode this reader must not have is specific and worse than reading nothing: handing the
coder items the GAN panel explicitly REJECTED.
"""
from __future__ import annotations

import pytest

from ocean_pipeline import nodes


# --------------------------------------------------------------- the four real shapes
def test_each_of_the_four_residual_gap_shapes_is_read():
    """MM-14457 / MM-14475 / MM-14312 (nested under qa_gan) / MM-14381 (nested in gan_rounds)."""
    shapes = {
        "MM-14457 residual_high_gaps": {
            "residual_high_gaps": [{"severity": "HIGH", "gap": "retry counter keyed on unlocode only"}]},
        "MM-14475 qa_gan_residual_gaps": {
            "qa_gan_residual_gaps": [{"severity": "HIGH", "gap": "originalStopName not emitted"}]},
        "MM-14312 qa_gan.remaining_gaps": {
            "qa_gan": {"remaining_gaps": [{"severity": "HIGH", "summary": "cutoff source priority"}]}},
        "MM-14381 gan_rounds[].real_gaps": {
            "gan_rounds": [{"round": 3,
                            "real_gaps": [{"severity": "HIGH", "summary": "pol_matches? reads the "
                                                                          "non-EDI port_of_loading"}]}]},
    }
    for label, artifact in shapes.items():
        got = nodes._gan_gaps(artifact)
        assert len(got) == 1, f"{label} was not read: {got}"
        assert got[0]["severity"] == "HIGH"
        assert got[0]["summary"], label


def test_the_summary_field_alias_is_handled():
    """`summary` (14312, 14381) vs `gap` (14457, 14475)."""
    for field in ("summary", "gap"):
        got = nodes._gan_gaps({"residual_high_gaps": [{"severity": "HIGH", field: "the text"}]})
        assert got == [{"severity": "HIGH", "summary": "the text"}], field


# --------------------------------------------------------------- the harmful keys
def test_the_two_non_gap_keys_are_never_read():
    """THE failure mode. MM-14060's `residual_open_items_for_downstream` is LOW/ADVISORY with zero
    HIGH; MM-14132's `documented_gaps_and_deferrals` has NO severity field and statuses DEFERRED /
    OUT OF SCOPE / MOVE TO UNIT LEVEL, one of them recording the panel ruling the bug a FALSE
    POSITIVE. Handing either to the coder is worse than handing it nothing."""
    mm14060 = {"residual_open_items_for_downstream": [
        {"severity": "LOW", "summary": "nice to have"},
        {"severity": "ADVISORY", "summary": "observability only"},
        # Even if one were mislabelled HIGH, the key is excluded BY NAME.
        {"severity": "HIGH", "summary": "should never reach the coder"}]}
    assert nodes._gan_gaps(mm14060) == []

    mm14132 = {"documented_gaps_and_deferrals": [
        {"status": "DEFERRED", "summary": "later"},
        {"status": "OUT OF SCOPE", "summary": "not this ticket"},
        {"status": "FALSE POSITIVE", "summary": "the panel ruled this bug is not real"}]}
    assert nodes._gan_gaps(mm14132) == []


def test_already_fixed_gaps_are_not_handed_back():
    """MM-14381's rounds 1-2 use `real_gaps_fixed` — gaps the GAN ALREADY CLOSED. A prefix or
    substring match on "real_gaps" catches it and hands the coder completed work."""
    artifact = {"gan_rounds": [
        {"round": 1, "real_gaps_fixed": [{"severity": "HIGH", "summary": "already fixed in round 1"}]},
        {"round": 2, "real_gaps_fixed": [{"severity": "HIGH", "summary": "already fixed in round 2"}]},
        {"round": 3, "real_gaps": [{"severity": "HIGH", "summary": "genuinely residual"}]}]}
    got = nodes._gan_gaps(artifact)
    assert got == [{"severity": "HIGH", "summary": "genuinely residual"}], got


# --------------------------------------------------------------- the severity filter
@pytest.mark.parametrize("sev,kept", [("HIGH", True), ("high", True), ("**HIGH**", True),
                                      ("MEDIUM", False), ("MED", False), ("LOW", False),
                                      ("ADVISORY", False), ("", False), (None, False)])
def test_only_high_survives(sev, kept):
    """Measured payload is 1-2 HIGH per run. Widening to MEDIUM turns a short, code-actionable list
    into noise the coder will skim — MM-14381 alone would add two."""
    got = nodes._gan_gaps({"residual_high_gaps": [{"severity": sev, "summary": "x"}]})
    assert bool(got) is kept, f"{sev!r} -> {got}"


def test_test_metadata_never_reaches_the_coder():
    """Each entry is wrapped in the GAN's own bookkeeping (`drafted_fix: "S15 ..."`, `verified`,
    `reverified`, `status`). That is about the TEST, and passing it on reads to the coder as an
    instruction to go edit the test."""
    got = nodes._gan_gaps({"residual_high_gaps": [{
        "severity": "HIGH", "gap": "the real defect",
        "drafted_fix": "S15 add an assertion", "verified": False, "reverified": "no",
        "status": "open"}]})
    assert got == [{"severity": "HIGH", "summary": "the real defect"}]
    assert "drafted_fix" not in repr(got) and "S15" not in repr(got)


# --------------------------------------------------------------- robustness
def test_the_reader_never_raises_on_a_malformed_artifact():
    """An agent emitting a seventh, unanticipated shape must degrade the gap list, not crash the
    node — the same standing `_gan_verdict` already has."""
    for junk in ({}, {"qa_gan": "a bare string"}, {"residual_high_gaps": "not a list"},
                 {"gan_rounds": ["not a dict"]}, {"residual_high_gaps": [None, 3, "x"]},
                 {"gan_rounds": None}, "not even a dict", None):
        assert nodes._gan_gaps(junk) == []


def test_duplicates_across_key_names_are_collapsed():
    """Two artifacts in one batch persisted the verdict under different keys; the same is possible
    for gaps, and the coder should not be handed the same defect twice."""
    got = nodes._gan_gaps({"residual_high_gaps": [{"severity": "HIGH", "summary": "same defect"}],
                           "qa_gan_residual_gaps": [{"severity": "HIGH", "gap": "same defect"}]})
    assert len(got) == 1


# --------------------------------------------------------------- the wiring
def test_the_coder_actually_injects_the_gaps():
    """The whole point of D1 Step 1. A reader nothing consumes is the inertness defect — and this
    key ALREADY had a sibling (`qa_gan_phase0_gaps`) that is produced, surfaced at the QA gate, and
    read by no coder, which is exactly how this went unnoticed."""
    import inspect
    src = inspect.getsource(nodes.coder)
    assert 'state.get("qa_gan_residual_gaps")' in src, "the coder never reads the gaps"
    assert "GAN RESIDUAL HIGH GAPS" in src
    assert "hypothetical implementation choices" in src, (
        "the caveat is missing — MM-14312's own rationale says a share of these are hypothetical, "
        "and without it the coder invents changes to satisfy moot findings")


def test_qa_scenarios_returns_the_key_and_state_declares_it():
    """LangGraph silently DROPS undeclared keys, which would make the whole reader inert."""
    import inspect

    from ocean_pipeline.state import OceanState
    assert "qa_gan_residual_gaps" in OceanState.__annotations__
    assert '"qa_gan_residual_gaps": gan_gaps' in inspect.getsource(nodes.qa_scenarios)


def test_the_excluded_keys_are_named_in_the_source():
    """Pinned so a future reader cannot "complete" the alias list by adding the two harmful keys —
    they look like the other four and the reason to exclude them is not self-evident."""
    assert set(nodes._GAN_GAP_KEYS_EXCLUDED) == {
        "residual_open_items_for_downstream", "documented_gaps_and_deferrals"}
    for key in nodes._GAN_GAP_KEYS_EXCLUDED:
        assert key not in nodes._GAN_GAP_KEYS
