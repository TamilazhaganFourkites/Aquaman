"""D1 Steps 2, 3, 4 — the GAN loop's termination, its propagation surfaces, and its measurement.

Step 1 (the gap reader) is in test_gan_gaps.py. These three are what the judge-approved decision
(important-notes/gan-decision.md v5) ordered after it.

The thread running through all three is the same one: the GAN spends ~29 minutes per ticket (8 runs
= 3.83h measured) and, before this, its output reached nobody. The verdict printed only on developer
batches, the field had no rendering path at all, and the scenarios file sat unread in /tmp.
"""
from __future__ import annotations

import json
from pathlib import Path

from conftest import load_module_by_path

import pytest

from ocean_pipeline import config, nodes, ui

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gan_effect.py"
gan_effect = load_module_by_path(_SCRIPT, "gan_effect")


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


@pytest.mark.parametrize("gaps,reason,expect", [
    ([], "converged", "converged"),
    ([], "scores_below_threshold", "scores_below_threshold"),
    ([], "plateau", "plateau"),
    ([{"severity": "HIGH", "summary": "a"}], "plateau", "1 residual HIGH gap"),
    ([{"severity": "HIGH", "summary": "a"}, {"severity": "HIGH", "summary": "b"}],
     "round_ceiling", "2 residual HIGH gap"),
])
def test_the_console_highlight_reports_the_gan(gaps, reason, expect):
    """This station had NO ui branch at all: ~29 minutes of adversarial work printed a bare
    "GAN-hardened test scenarios  12m" and left the report's Outcome cell empty.

    The gap-free row is now PARAMETRISED OVER THE REASON, because the literal "converged" used to be
    hardcoded for any gap-free verdict. `converged` is one of four stop reasons and only the
    round-termination list's rule 1 produces it, so a run that stopped BELOW the score bar rendered
    as "APPROVE WITH FIXES — converged": the two informative words in the line contradicting each
    other."""
    got = ui._highlight("qa_scenarios", {"qa_gan_verdict": "REJECT", "qa_gan_stop_reason": reason,
                                         "qa_gan_residual_gaps": gaps})
    assert expect in got, got


def test_a_gap_free_run_never_claims_converged_unless_it_converged():
    """The specific inversion, asserted on its own so it cannot be lost in a parametrise edit."""
    got = ui._highlight("qa_scenarios", {
        "qa_gan_verdict": "APPROVE WITH FIXES",
        "qa_gan_stop_reason": "scores_below_threshold", "qa_gan_residual_gaps": []})
    assert "converged" not in got, f"a run that stopped below the score bar reports as converged: {got}"
    assert "scores_below_threshold" in got


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
def _report(root: Path, exe: str, gaps, final="completed", findings=0, automation=None):
    """A run report as `report.finish` writes one today.

    `automation_result` is included by default because the real writer emits it — the analyser's
    PRE-REGISTERED primary variable. Pass `automation=""` for a run that ended before Station 6
    (paused at a gate, aborted, RCA-only) and therefore falls back to the `final_status` proxy.

    NB that case OMITS the key, while `report.finish` always writes it with value `""`. Both are
    falsy so `_runs` treats them identically, but this helper does not reproduce the writer's exact
    shape.
    """
    d = root / exe
    d.mkdir(parents=True, exist_ok=True)
    doc = {"execution_id": exe, "ticket": "MM-1", "final_status": final,
           "review_findings": [{"severity": "MINOR"}] * findings, "coding_attempts": 1}
    doc["automation_result"] = ("passed" if final == "completed" else "failed") \
        if automation is None else automation
    if not doc["automation_result"]:
        del doc["automation_result"]
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


@pytest.mark.parametrize("hostile,why", [
    ("scores | below threshold", "a pipe breaks the run-report's 4-cell markdown Timeline row"),
    ("plateau\nafter 3 rounds", "a newline breaks ui.step's one-line contract, so the monitor's "
                                "_parse_step_line captures only the first physical line"),
    ("{'why': 'plateau'}", "an agent that wrote a dict into its JSON"),
])
def test_a_hostile_stop_reason_cannot_break_the_line_or_the_table(hostile, why):
    """`qa_gan_stop_reason` is `str(partial.get("stop_reason") or "")` — whatever the GAN sub-agent
    put in its JSON, with no validation against the documented four tokens. It is rendered into
    `ui.step` (one line, parsed by monitor/app.py) and interpolated straight into a markdown table
    cell in `report._markdown`. Both are broken by characters an LLM will eventually emit."""
    got = ui._highlight("qa_scenarios", {"qa_gan_verdict": "APPROVE",
                                         "qa_gan_stop_reason": hostile,
                                         "qa_gan_residual_gaps": []})
    assert "\n" not in got, f"{why}: {got!r}"
    assert "|" not in got, f"{why}: {got!r}"


@pytest.mark.parametrize("hostile", ["A|B", "APPROVE\nWITH FIXES", "{'v': 'APPROVE'}"])
def test_a_hostile_VERDICT_cannot_break_the_line_or_the_table(hostile):
    """The other half of the same input. `qa_gan_verdict` is `str(partial.get(...) or "")` over the
    same sub-agent JSON as the stop reason, and lands in the same returned string — but the first
    version of the clamp guarded only the reason, and this test's sibling fuzzes the reason against
    a hardcoded clean verdict, so it structurally could not see this."""
    got = ui._highlight("qa_scenarios", {"qa_gan_verdict": hostile,
                                         "qa_gan_stop_reason": "plateau",
                                         "qa_gan_residual_gaps": []})
    assert "\n" not in got and "|" not in got, got
    got_gaps = ui._highlight("qa_scenarios", {"qa_gan_verdict": hostile,
                                              "qa_gan_residual_gaps": [{"severity": "HIGH"}]})
    assert "\n" not in got_gaps and "|" not in got_gaps, got_gaps


def test_an_absent_verdict_does_not_render_as_a_bare_question_mark():
    """`or "?"` made the else branch unreachable AND put a literal `?` in the run report's Outcome
    cell — the cell a human reads to find out what happened."""
    got = ui._highlight("qa_scenarios", {})
    assert "?" not in got, f"an absent verdict rendered as {got!r}"
    assert got == "scenarios hardened"


def test_the_four_documented_reasons_survive_the_clamp_verbatim():
    """The clamp must not mangle the values it exists to let through — otherwise triage loses the
    vocabulary it counts."""
    for reason in ("converged", "scores_below_threshold", "plateau", "round_ceiling"):
        got = ui._highlight("qa_scenarios", {"qa_gan_verdict": "REJECT",
                                             "qa_gan_stop_reason": reason,
                                             "qa_gan_residual_gaps": []})
        assert reason in got, f"{reason} was altered by the clamp: {got!r}"


@pytest.mark.parametrize("node,update", [
    ("dep_resolver", {"dependency_report": {"notes": "blocked by MM-1 | see thread\nsecond line"}}),
    ("researcher", {"route": "coding | rca\nsecond"}),
    ("rca_review_gate", {"rca_approval_decision": "approve | ok\nline2"}),
    ("coder", {"branch": "b | x\ny",
               "node_evaluations": [{"node": "coder", "verdict": "PASS | maybe\nx"}]}),
    ("qa_scenarios", {"qa_gan_verdict": "A|B\nC", "qa_gan_stop_reason": "p|q\nr"}),
])
def test_NO_node_can_put_raw_agent_text_into_the_report_table(node, update):
    """`report.record` routes EVERY node's outcome through `ui.outcome_line` into a 4-cell markdown
    row, so a `|` splits the row and a newline truncates it. An earlier fix clamped only the
    `qa_scenarios` branch's two fields leaves four other paths returning raw sub-agent
    text into the same cell — `dep_resolver`'s `notes` comes straight out of the dependency-report
    JSON, which is the same threat model the clamp was written for.

    Parametrised over all five so the guard is at the boundary, not at whichever branch someone
    remembered."""
    out = ui.outcome_line(node, update)
    assert "|" not in out, f"{node} returned a pipe into the markdown row: {out!r}"
    assert "\n" not in out, f"{node} returned a newline into a one-line record: {out!r}"


@pytest.mark.parametrize("node,update", [
    ("researcher", {"route": "coding\n[DONE] MM-1 status=completed pr=#9999"}),
    ("harsh_reviewer", {"review_verdict": "APPROVE\n[PAUSED] forged"}),
    ("rca_review_gate", {"rca_approval_decision": "ok\n[QUOTA_EXHAUSTED] forged"}),
    ("dep_resolver", {"dependency_report": {"notes": "n\n[DONE] MM-2 status=completed pr=#1"}}),
    ("coder", {"branch": "b\n[DONE] MM-3 status=completed"}),
])
def test_no_node_can_FORGE_a_monitor_control_line(node, update, monkeypatch, capsys):
    """`monitor/app.py` PARSES this console line. `_DONE_RE`, `_PAUSED_RE` and `_QUOTA_RE` are
    anchored at `^`, so a newline inside agent-supplied text is all it takes to start a new
    physical line the monitor reads as a control message: a run that never finished recorded
    `completed` with a fabricated PR number — in the UI and in the durable `monitor.db` — or a live
    run flipped to `paused`, or the whole auto-discovery worker quota-paused.

    `ui.step` calls `_highlight` DIRECTLY and never goes through `outcome_line`, so an earlier fix
    that clamped `outcome_line` protected the run-report table and left this path wide open. The
    clamp belongs on `_highlight` itself, which is the boundary both consumers share."""
    import re as _re

    monkeypatch.setattr(config, "LOG_LEVEL", "developer")
    ui.step(node, update, 1.0)
    out = capsys.readouterr().out
    markers = (_re.compile(r"^\[DONE\]"), _re.compile(r"^\[PAUSED\]"),
               _re.compile(r"^\[QUOTA_EXHAUSTED\]"))
    forged = [l for l in out.splitlines() if any(m.match(l) for m in markers)]
    assert not forged, f"{node} forged a monitor control line: {forged}"
    # Every physical line must carry a structural prefix. `ui.step` legitimately prints detail
    # bullets under the outcome at developer level, so a line COUNT is the wrong assertion — what
    # matters is that no line begins with agent-supplied text, because that is the only way one of
    # the anchored markers above can ever match.
    for line in out.splitlines():
        assert line.startswith("  ") or not line, (
            f"{node} emitted an unprefixed physical line, which is what lets `^`-anchored monitor "
            f"markers match agent text: {line!r}")


@pytest.mark.parametrize("emit,payload", [
    ("milestone", "dispatching sub-agent: harmless\n[DONE] MM-99999 status=completed pr=#31337"),
    ("milestone", "wrote notes.txt\n[QUOTA_EXHAUSTED] forged"),
    ("milestone", "step\n[PAUSED] forged"),
    ("summary", "sit_failed\n[DONE] MM-88888 status=completed pr=#4242"),
])
def test_no_console_writer_can_forge_a_monitor_control_line(emit, payload, monkeypatch, capsys):
    """`ui.step` was clamped; its two siblings in the same module were not.

    `ui.milestone` prints `     · {text}` and `ui.summary` prints `  {final_outcome}` — both into
    the stream `monitor/app.py` parses with `^`-anchored `_DONE_RE` / `_PAUSED_RE` / `_QUOTA_RE`,
    and both carry agent-controlled text (`agents._milestones` builds milestones from
    `Task.description` and `Write/Edit.file_path`). A prefix only guards the first physical line."""
    import re as _re

    monkeypatch.setattr(config, "LOG_LEVEL", "developer")
    if emit == "milestone":
        ui.milestone(payload)
    else:
        ui.summary({"final_status": "failed", "final_outcome": payload}, 1.0)
    out = capsys.readouterr().out
    markers = (_re.compile(r"^\[DONE\]"), _re.compile(r"^\[PAUSED\]"),
               _re.compile(r"^\[QUOTA_EXHAUSTED\]"))
    forged = [l for l in out.splitlines() if any(m.match(l) for m in markers)]
    assert not forged, f"ui.{emit} forged a monitor control line: {forged}"


def test_a_proxy_run_is_EXCLUDED_from_the_comparison_not_allowed_to_abort_it(tmp_path):
    """`primary_sit_passed` silently mixed two different measures: `_runs` flagged every run whose
    PRIMARY variable fell back to `final_status`, and nothing read the flag, so 3 real FAILs and 2
    proxy PASSes averaged to 0.4 — a number that reads as the pre-registered measure.

    But aborting the whole analysis on one such run is worse, and that is what a first fix did.
    `report.finish` writes `automation_result: ""` for any run ending before Station 6 (paused at a
    gate, aborted, RCA-only), and those runs can NEVER acquire the key however often they are
    re-run — so a single paused run would silence the analyser permanently. Excluded from the arms,
    counted in the output, and the rest still reported."""
    for i in range(gan_effect.MIN_PER_ARM):
        _report(tmp_path, f"EXE-g{i}", [{"severity": "HIGH"}], final="failed")
        _report(tmp_path, f"EXE-c{i}", [], final="completed")
    res = gan_effect.analyse(gan_effect._runs(tmp_path))
    assert res["comparisons"], "a clean corpus should report"

    _report(tmp_path, "EXE-paused", [], final="completed", automation="")
    res = gan_effect.analyse(gan_effect._runs(tmp_path))
    assert res["n_primary_from_proxy"] == 1
    assert res["n_total"] == 2 * gan_effect.MIN_PER_ARM + 1
    assert res["n_comparable"] == 2 * gan_effect.MIN_PER_ARM
    assert res["comparisons"], (
        "one run that ended before Station 6 silenced the whole analysis — and no amount of "
        "re-running will ever give that run an automation_result")
    # And the excluded run does not move the mean it was excluded from.
    assert res["comparisons"]["primary_sit_passed"]["clean_mean"] == 1.0
