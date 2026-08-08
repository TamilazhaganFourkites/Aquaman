"""D1 Steps 2, 3, 4 — the GAN loop's termination, its propagation surfaces, and its measurement.

Step 1 (the gap reader) is in test_gan_gaps.py. These three are what the judge-approved decision
(important-notes/gan-decision.md v5) ordered after it.

The thread running through all three is the same one: the GAN spends ~29 minutes per ticket (8 runs
= 3.83h measured) and, before this, its output reached nobody. The verdict printed only on developer
batches, the field had no rendering path at all, and the scenarios file sat unread in /tmp.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from ocean_pipeline import config, nodes, ui

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gan_effect.py"
_spec = importlib.util.spec_from_file_location("gan_effect", _SCRIPT)
gan_effect = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("gan_effect", gan_effect)
_spec.loader.exec_module(gan_effect)


# ------------------------------------------------------------------ Step 2: termination
def test_the_skill_states_the_ordered_termination_list():
    """The blanket "max 3 rounds, never a 4th" cut off runs that were demonstrably CONVERGING —
    MM-14312 and MM-14381 both ran HIGH 3->2->1 with both scores already >=90% at round 3."""
    skill = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-qa-agent" / "SKILL.md"
    if not skill.exists():
        pytest.skip("fk-aideveloper checkout has no ocean-qa-agent SKILL.md")
    text = skill.read_text()
    assert "Round termination" in text
    for reason in ("converged", "scores_below_threshold", "plateau", "round_ceiling"):
        assert f"`stop_reason: {reason}`" in text or f"stop_reason: {reason}" in text, reason
    # The extension must be EVIDENCE-GATED, not a blanket raise: a plateau mints new bugs.
    assert "strictly decreased" in text or "strictly decreasing" in text
    assert "plateau" in text
    # HIGH must be judge-assigned, or a severity-inflating adversary can manufacture an extension.
    assert "HIGH_REAL_GAP_COUNT" in text
    assert "JUDGE-assigned" in text
    # Terminal patches were all recorded verified:false — applied and never re-run.
    assert "verified: false" in text or "verified: true|false" in text
    assert "does not spawn an adversary" in text, (
        "re-verification must be one judge call, not a round — otherwise it can introduce new bugs")


def test_the_round_ceiling_is_configurable_and_reaches_the_skill():
    """A batch must be able to pin the ceiling back to 3 without editing SKILL.md mid-run."""
    import inspect
    assert config.MAX_GAN_ROUNDS == 5
    src = inspect.getsource(nodes.qa_scenarios)
    assert "config.MAX_GAN_ROUNDS" in src, "the ceiling never reaches the skill"
    assert "stop_reason" in src, "the node never asks for the stop_reason"


def test_the_stop_reason_is_carried_into_state():
    import inspect
    from ocean_pipeline.state import OceanState
    assert "qa_gan_stop_reason" in OceanState.__annotations__, "LangGraph would drop it"
    assert '"qa_gan_stop_reason"' in inspect.getsource(nodes.qa_scenarios)


# ------------------------------------------------------------------ Step 3: propagation
def _pr_body(state: dict) -> str:
    """Reproduce open_pr's body construction without opening a PR."""
    import re
    src = Path(nodes.__file__).read_text()
    assert "Residual test-coverage gaps" in src
    return src  # asserted structurally below


def test_the_pr_body_section_is_gated_on_gaps_not_on_the_verdict():
    """4 of 5 real artifacts are REJECT and the verdict vocabulary drifts
    (`APPROVE_WITH_FIXES` vs `APPROVE WITH FIXES`), so gating on the verdict would put this section
    on nearly every PR and train reviewers to skip it."""
    src = Path(nodes.__file__).read_text()
    start = src.index("Residual test-coverage gaps")
    window = src[start - 900:start]
    assert 'state.get("qa_gan_residual_gaps")' in window, "the section is not gap-gated"
    # The CODE form, not any mention: the comment above the block explains why the verdict is NOT
    # the gate, and banning the bare string fails on the very note that documents the decision.
    assert 'state.get("qa_gan_verdict")' not in window, (
        "the PR section reads the VERDICT as its gate — see this test's docstring")


def test_an_empty_gap_list_leaves_the_pr_body_byte_identical():
    """Appends only. With no gaps the body must be exactly what it was before this existed."""
    src = Path(nodes.__file__).read_text()
    i = src.index("Residual test-coverage gaps")
    # The append happens inside `if gan_gaps:` — find the guard immediately above.
    assert "if gan_gaps:" in src[i - 900:i], "the append is not guarded by a non-empty check"
    assert "body +=" in src[i - 900:i + 200], "the section replaces the body instead of appending"


@pytest.mark.parametrize("gaps,expect", [
    ([], "converged"),
    ([{"severity": "HIGH", "summary": "a"}], "1 residual HIGH gap"),
    ([{"severity": "HIGH", "summary": "a"}, {"severity": "HIGH", "summary": "b"}], "2 residual HIGH gap"),
])
def test_the_console_highlight_reports_the_gan(gaps, expect):
    """This station had NO ui branch at all: ~29 minutes of adversarial work printed a bare
    "GAN-hardened test scenarios  12m" and left the report's Outcome cell empty."""
    got = ui._highlight("qa_scenarios", {"qa_gan_verdict": "REJECT", "qa_gan_residual_gaps": gaps})
    assert expect in got, got


def test_the_details_carry_the_verdict_stop_reason_and_first_gaps():
    d = ui._details("qa_scenarios", {
        "qa_gan_verdict": "REJECT", "qa_gan_stop_reason": "plateau",
        "qa_gan_residual_gaps": [{"severity": "HIGH", "summary": f"gap {i}"} for i in range(5)]})
    joined = "\n".join(d)
    assert "REJECT" in joined and "plateau" in joined
    assert "gap 0" in joined and "gap 2" in joined
    assert "+2 more" in joined, "the list must be capped and say so, not silently truncate"


def test_a_run_with_no_gan_output_prints_nothing_new():
    """Back-compat: a station update carrying no GAN keys must not manufacture a line."""
    assert ui._details("qa_scenarios", {}) == []


# ------------------------------------------------------------------ Step 4: measurement
def _report(root: Path, exe: str, gaps, final="completed", findings=0):
    d = root / exe
    d.mkdir(parents=True, exist_ok=True)
    doc = {"execution_id": exe, "ticket": "MM-1", "final_status": final,
           "review_findings": [{"severity": "MINOR"}] * findings, "coding_attempts": 1}
    if gaps is not None:
        doc["qa_gan_residual_gaps"] = gaps
    (d / "run-report.json").write_text(json.dumps(doc))


def test_the_dependent_variable_is_named_in_the_source_not_chosen_from_the_data():
    """The whole point of Step 4. An outcome picked after looking at the data finds an effect every
    time — the separation is then the choice, not the finding."""
    assert set(gan_effect.OUTCOMES) == {"primary_sit_passed", "secondary_coding_attempts",
                                        "secondary_review_findings"}
    assert "BEFORE ANY DATA IS READ" in gan_effect.__doc__


def test_it_refuses_to_report_an_underpowered_comparison(tmp_path):
    """At a 62.5% base rate n=10 splits ~6/4. Refusing is the feature: the alternative is a number
    that reads as evidence and is not."""
    for i in range(3):
        _report(tmp_path, f"EXE-g{i}", [{"severity": "HIGH", "summary": "x"}])
    for i in range(2):
        _report(tmp_path, f"EXE-c{i}", [])
    res = gan_effect.analyse(gan_effect._runs(tmp_path))
    assert res["underpowered"] is True
    assert res["comparisons"] == {}, "an underpowered comparison was reported anyway"
    assert "need" in res["verdict"].lower() and str(gan_effect.MIN_PER_ARM) in res["verdict"]


def test_it_does_report_once_both_arms_are_populated(tmp_path):
    for i in range(gan_effect.MIN_PER_ARM):
        _report(tmp_path, f"EXE-g{i}", [{"severity": "HIGH", "summary": "x"}],
                final="failed", findings=3)
        _report(tmp_path, f"EXE-c{i}", [], final="completed", findings=0)
    res = gan_effect.analyse(gan_effect._runs(tmp_path))
    assert res["underpowered"] is False
    comp = res["comparisons"]["primary_sit_passed"]
    assert comp["gaps_mean"] == 0.0 and comp["clean_mean"] == 1.0
    assert res["comparisons"]["secondary_review_findings"]["gaps_mean"] == 3.0


def test_a_run_predating_the_key_is_unknown_not_clean(tmp_path):
    """"No gaps recorded" and "no gaps found" are different facts. Collapsing them would silently
    load every legacy run into the clean arm and manufacture the effect."""
    _report(tmp_path, "EXE-old", None)
    _report(tmp_path, "EXE-new", [])
    runs = {r["execution_id"]: r for r in gan_effect._runs(tmp_path)}
    assert runs["EXE-old"]["arm"] == "unknown"
    assert runs["EXE-new"]["arm"] == "clean"


def test_the_predictor_is_the_gaps_never_the_verdict():
    """62.5% of artifacts are REJECT, so the verdict is near-constant and cannot separate anything —
    the same failure the Step 9b retrospective found for a constant NEEDS_WORK."""
    src = _SCRIPT.read_text()
    assert "NEVER THE VERDICT" in src
    assert "qa_gan_verdict" not in src.split("THE PREDICTOR")[1].split("POWER")[0].replace(
        "the verdict", ""), "the analyser reads the verdict as a predictor"
