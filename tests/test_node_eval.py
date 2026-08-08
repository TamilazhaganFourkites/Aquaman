"""D5/D6 — the independent per-node accuracy evaluator.

The defect this closes is not a bug, it is an ABSENCE: `node-evaluator.md` specified the evaluator
end to end (three scored dimensions, a PASS/WARN/FAIL enum, per-node rubrics for 8 named nodes, and
the enforcement policy), and monitor/app.py:411-425 already carried handling for its output —
including a live `if header_label.startswith("eval_"): continue` guard for output nothing could
produce. Someone wrote both ends and never the middle.

So these tests are weighted toward *inertness*, not toward the happy path: a driver that exists but
is never called, a config knob nothing reads, a state key LangGraph drops, a stop nothing labels.
Each of those would leave the feature exactly as dead as it was before, while looking present.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import re
from pathlib import Path

import pytest

from ocean_pipeline import agents, config, graph, nodes, report, schemas, telemetry, ui


# --------------------------------------------------------------- the contract
def test_verdict_is_canonicalized_not_literal_parsed():
    """A `Literal` would fail-parse the WHOLE evaluation on one typo, discarding three real scores
    because the judge wrote "Pass." — the same reasoning that produced `gate_decision`."""
    for raw, want in [("PASS", "PASS"), ("  fail ", "FAIL"), ("**WARN**", "WARN"),
                      ("Pass.", "PASS"), ({"verdict": "FAIL"}, "FAIL"), (["PASS"], "PASS")]:
        assert schemas.eval_verdict(raw) == want, raw
    for raw in ["maybe", "", None, "APPROVED", 7]:
        assert schemas.eval_verdict(raw) == "", raw

    # A whole verdict with an unreadable enum still parses and keeps its scores.
    ev = schemas.NodeEvaluation.model_validate(
        {"node": "coder", "accuracy": 88, "verdict": "probably fine",
         "dimensions": {"correctness": 90, "completeness": 80, "grounding": 95}})
    assert ev.verdict == "" and ev.accuracy == 88 and ev.dimensions.grounding == 95


def test_unreported_and_zero_are_different_numbers():
    """"Could not measure" and "measured and failed" must never share a value. A 0 default would
    silently convert the first into the second — this repo's most-repeated defect."""
    blank = schemas.NodeEvaluation()
    assert blank.accuracy is None
    assert (blank.dimensions.correctness, blank.dimensions.completeness,
            blank.dimensions.grounding) == (None, None, None)
    assert schemas.NodeEvaluation.model_validate({"accuracy": 0}).accuracy == 0, "0 is a real score"
    # Out of range / non-numeric is NOT clamped — clamping 150 to 100 would manufacture a pass.
    for bad in [150, -3, "high", None, True]:
        assert schemas.NodeEvaluation.model_validate({"accuracy": bad}).accuracy is None, bad


# --------------------------------------------------------------- the gate (pure)
def _st(**kw):
    base = {"ticket_id": "MM-1", "execution_id": "EXE-t"}
    base.update(kw)
    return base


def test_the_gate_routes_nothing_unless_enforce_is_on(monkeypatch):
    """This is what makes NODE_EVAL safe to switch on: turning evaluation ON cannot, by itself,
    change where any run goes."""
    fail = _st(node_evaluations=[{"node": "coder", "verdict": "FAIL", "accuracy": 10}])
    monkeypatch.setattr(config, "EVAL_ENFORCE", False)
    assert nodes._eval_gate(fail) == ""
    monkeypatch.setattr(config, "EVAL_ENFORCE", True)
    assert "FAILED the `coder` node" in nodes._eval_gate(fail)


@pytest.mark.parametrize("verdict,gates", [("FAIL", True), ("WARN", False), ("PASS", False),
                                           ("", False), ("nonsense", False)])
def test_only_fail_gates(verdict, gates, monkeypatch):
    """WARN is "minor gaps, usable" per the spec — gating on it would collapse the three-value enum
    into two and make the middle rung unreachable. An unreadable verdict fails OPEN, matching
    graph.py:330's stated precedent, because a judge outage is an infra fault, not a bad diff."""
    monkeypatch.setattr(config, "EVAL_ENFORCE", True)
    got = nodes._eval_gate(_st(node_evaluations=[{"node": "coder", "verdict": verdict}]))
    assert bool(got) is gates, f"{verdict!r} -> {got!r}"


def test_the_gate_survives_junk_in_the_list(monkeypatch):
    monkeypatch.setattr(config, "EVAL_ENFORCE", True)
    assert nodes._eval_gate(_st(node_evaluations=["oops", None, 42])) == ""
    assert nodes._eval_gate(_st()) == ""
    assert nodes._eval_gate(_st(node_evaluations=None)) == ""


def test_a_failing_gate_reports_an_unreported_score_as_such(monkeypatch):
    monkeypatch.setattr(config, "EVAL_ENFORCE", True)
    msg = nodes._eval_gate(_st(node_evaluations=[{"node": "coder", "verdict": "FAIL"}]))
    assert "accuracy=not reported" in msg, msg
    assert "accuracy=0" not in msg, "an absent score must never be rendered as a zero score"


# --------------------------------------------------------------- the driver
def _fake_eval(**fields):
    async def _run(**kw):
        _run.seen = kw
        return schemas.NodeEvaluation.model_validate(fields)
    return _run


def test_the_driver_is_inert_unless_switched_on(monkeypatch):
    called = []
    monkeypatch.setattr(agents, "evaluate_node", lambda **kw: called.append(kw))
    monkeypatch.setattr(config, "NODE_EVAL", False)
    assert asyncio.run(nodes._eval_node(_st(), "coder", "job", "out")) == {}
    monkeypatch.setattr(config, "NODE_EVAL", True)
    monkeypatch.setattr(config, "EVAL_NODES", ("harsh_reviewer",))
    assert asyncio.run(nodes._eval_node(_st(), "coder", "job", "out")) == {}
    assert called == [], "the judge was dispatched despite being switched off"


def test_a_judge_failure_can_never_fail_the_station(monkeypatch):
    """It spends a full SDK session, so it WILL fail for transport reasons. An accuracy judge that
    can break the run it was added to observe is worse than no judge."""
    async def _boom(**kw):
        raise RuntimeError("transport died")
    monkeypatch.setattr(agents, "evaluate_node", _boom)
    monkeypatch.setattr(config, "NODE_EVAL", True)
    monkeypatch.setattr(config, "EVAL_NODES", ("coder",))

    out = asyncio.run(nodes._eval_node(_st(), "coder", "job", "out"))   # must not raise
    assert "could not run" in out["eval_unverified"]
    assert not out.get("eval_gap") and not out.get("eval_stopped"), "a dead judge must not gate"


def test_the_driver_writes_every_key_on_every_pass(monkeypatch):
    """`quality_gate` learned this the hard way: a "write only what changed" shape leaves a stale gap
    in state that re-injects into every later router decision and coder prompt."""
    monkeypatch.setattr(agents, "evaluate_node", _fake_eval(node="coder", accuracy=91,
                                                            verdict="PASS"))
    monkeypatch.setattr(config, "NODE_EVAL", True)
    monkeypatch.setattr(config, "EVAL_NODES", ("coder",))
    out = asyncio.run(nodes._eval_node(_st(), "coder", "job", "out"))
    assert set(out) == {"node_evaluations", "eval_unverified", "eval_gap", "eval_attempts",
                        "eval_stopped"}
    assert out["node_evaluations"][0]["accuracy"] == 91
    assert out["eval_gap"] == "" and out["eval_attempts"] == 0 and out["eval_stopped"] is False


def test_an_unreadable_verdict_is_recorded_as_unmeasured_not_clean(monkeypatch):
    monkeypatch.setattr(agents, "evaluate_node", _fake_eval(node="coder", verdict="dunno"))
    monkeypatch.setattr(config, "NODE_EVAL", True)
    monkeypatch.setattr(config, "EVAL_NODES", ("coder",))
    out = asyncio.run(nodes._eval_node(_st(), "coder", "job", "out"))
    assert "no readable verdict" in out["eval_unverified"]
    assert out["eval_gap"] == "", "unreadable fails OPEN"


def test_the_budget_bounces_once_then_stops(monkeypatch):
    monkeypatch.setattr(agents, "evaluate_node", _fake_eval(node="coder", accuracy=20,
                                                            verdict="FAIL"))
    monkeypatch.setattr(config, "NODE_EVAL", True)
    monkeypatch.setattr(config, "EVAL_NODES", ("coder",))
    monkeypatch.setattr(config, "EVAL_ENFORCE", True)
    monkeypatch.setattr(config, "MAX_EVAL_ATTEMPTS", 1)

    first = asyncio.run(nodes._eval_node(_st(), "coder", "j", "o"))
    assert first["eval_attempts"] == 1 and first["eval_stopped"] is False, "must bounce first"
    assert graph.after_quality_gate({**_st(), **first}) == "rework"

    second = asyncio.run(nodes._eval_node(_st(**first), "coder", "j", "o"))
    assert second["eval_attempts"] == 2 and second["eval_stopped"] is True, "must stop, not loop"
    assert graph.after_quality_gate({**_st(), **second}) == "stop"


# --------------------------------------------------------------- routing
def test_the_deterministic_gate_outranks_the_judge(monkeypatch):
    """A file that does not parse is a certain defect; the evaluator is an LLM judgment. When both
    fire the certain evidence decides — and the judge's reason is still on `eval_gap` for stop_run."""
    monkeypatch.setattr(config, "MAX_QUALITY_GATE_ATTEMPTS", 1)
    both = _st(quality_gate_findings=[{"severity": "CRITICAL", "summary": "syntax"}],
               quality_gate_attempts=1, eval_gap="judge says no", eval_stopped=True)
    assert graph.after_quality_gate(both) == "rework", "syntax rework must win over the eval stop"


def test_routing_is_unchanged_when_the_feature_is_off():
    """With NODE_EVAL off — the default — `_eval_gate` returns "" so neither key is ever set, and
    this router must behave exactly as it did before D6 existed."""
    assert graph.after_quality_gate(_st()) == "proceed"
    assert graph.after_quality_gate(_st(quality_gate_findings=[])) == "proceed"


# --------------------------------------------------------------- the inertness guards
def _wired_nodes(src: str) -> set:
    """The node names actually passed to `_eval_node(...)`, read from the source.

    Deliberately not one clever regex over the whole call: the first argument is a dict literal
    (`{**state, **out}`) whose internal comma defeats the obvious `[^,]*` form -- which it did, and
    the test then reported an empty wired set and blamed the call site instead of the pattern. Take
    the first quoted string after each call instead, which is the node name by the signature.
    """
    return {m.group(1)
            for call in re.finditer(r"_eval_node\(", src)
            for m in [re.search(r'"([a-z_]+)"', src[call.end():call.end() + 400])] if m}


def test_every_eval_node_has_a_call_site():
    """A node named in EVAL_NODES with no `_eval_node(...)` call site is SILENTLY inert — the exact
    shape of the defect D5 closes. Asserted against the source, not against a list."""
    src = Path(nodes.__file__).read_text()
    wired = _wired_nodes(src)
    assert "coder" in wired, f"the coder call site went missing; wired={wired}"
    missing = [n for n in config.EVAL_NODES if n not in wired]
    assert missing == [], (
        f"EVAL_NODES names {missing}, which have no _eval_node call site and can never be scored. "
        f"Wired today: {sorted(wired)}")


def test_the_rubric_nodes_that_are_not_wired_are_known_and_stated():
    """node-evaluator.md defines rubrics for 8 nodes; only `coder` is wired. That is a real, stated
    limit rather than an oversight — this test fails if the set changes without the docstring being
    updated, so the gap can never quietly widen or be quietly forgotten."""
    spec = config.OCEAN_WORKERS_DIR / "node-evaluator.md"
    if not spec.exists():
        pytest.skip(f"fk-aideveloper checkout has no {spec.name} (branch without the ocean skills)")
    rubrics = set(re.findall(r"^- \*\*([a-z_]+)\*\* —", spec.read_text(), re.M))
    assert {"coder", "harsh_reviewer", "sit_triage"} <= rubrics, f"spec rubrics changed: {rubrics}"
    src = Path(nodes.__file__).read_text()
    wired = _wired_nodes(src)
    assert wired == {"coder"}, (
        f"the wired set changed to {sorted(wired)} — update this test and the D5 commit note, which "
        f"both state that only `coder` is wired today")


def test_state_declares_every_key_the_driver_returns():
    """LangGraph silently DROPS any key a node returns that OceanState does not declare. This repo
    has been bitten by that four times, and it would make the whole evaluator inert while every unit
    test on the node itself still passed."""
    from ocean_pipeline.state import OceanState
    declared = set(OceanState.__annotations__)
    for key in ("node_evaluations", "eval_gap", "eval_unverified", "eval_attempts", "eval_stopped"):
        assert key in declared, f"{key} is undeclared — LangGraph will drop it"


def test_the_monitor_guard_matches_the_label_the_driver_actually_emits():
    """monitor/app.py skips headers starting `eval_`. That guard was written before the driver
    existed; this asserts they agree, since a mismatch resurrects the phantom-step bug the monitor's
    own comment describes."""
    assert 'f"eval_{node}"' in Path(agents.__file__).read_text(), \
        "evaluate_node no longer labels itself eval_<node>"
    assert 'header_label.startswith("eval_")' in Path(
        Path(nodes.__file__).parents[2] / "monitor" / "app.py").read_text()
    # And it must NOT be a real pipeline stage.
    assert "eval_coder" not in ui._LABELS, "eval is advisory; it must not become a labelled station"


def test_the_station_number_is_registered_and_terminal():
    """Station 4.6 emits telemetry; an unregistered number degrades to a `station_<n>` fallback and
    a non-terminal status makes its rows vanish from the ClickHouse completed-station aggregate.
    The suite's own invariant tests caught exactly this during implementation."""
    assert telemetry._STATION_NAMES[4.6] == "node_eval"
    assert telemetry._STATUS["eval"] in telemetry._TERMINAL_STATUSES
    assert telemetry._STATUS["eval_could_not_run"] in telemetry._TERMINAL_STATUSES


def test_a_stop_is_labelled_in_all_three_cascades():
    """`stop_run` composes final_outcome from three independent ladders. Asserting `reason` alone
    passes while the human-facing string still says "no diff for the reviewer to approve"."""
    src = Path(nodes.__file__).read_text()
    assert src.count('state.get("eval_stopped")') >= 3, (
        "the eval stop is not labelled in all three of stop_run's cascades (reason / pr_note / "
        "outcome_prefix)")
    assert "eval_accuracy_failed" in src and "eval_accuracy_stopped" in src
    # Keyed on `eval_stopped`, never on `eval_gap`: a gap from a BOUNCED pass persists in state and
    # would mislabel a later, unrelated stop.
    stop_run = src[src.index("async def stop_run"):]
    assert 'eval_gap"' not in stop_run.split("return {")[0], \
        "stop_run keys a label on eval_gap — a stale gap will mislabel an unrelated later stop"


def test_prep_rework_resets_the_budget_but_not_the_infra_fault():
    """Matches the quality_gate discipline exactly: a code_fault is a fresh coding attempt and
    deserves a fresh accuracy budget, but an infra fault that survived a rework stays visible."""
    out = asyncio.run(nodes.prep_rework(_st(coding_attempts=1, eval_attempts=2,
                                            eval_gap="x", eval_stopped=True,
                                            eval_unverified="judge was down")))
    assert out["eval_attempts"] == 0 and out["eval_gap"] == "" and out["eval_stopped"] is False
    assert "eval_unverified" not in out, "an unresolved infra fault must survive the rework"


def test_the_evaluation_reaches_the_run_report(tmp_path):
    """"ADVISORY (logged + surfaced in the run-report)" is the spec's own policy. `report.finish`
    selects explicit keys, so an unlisted one is invisible however correctly it was computed."""
    report._meta.clear()
    report._rows.clear()
    report.start("MM-1", "EXE-eval-report")
    report.record("coder", 1.0, {})
    final = {"final_status": "completed",
             "node_evaluations": [{"node": "coder", "accuracy": 72, "verdict": "WARN"}],
             "eval_unverified": ""}
    report.finish(final, tmp_path)
    doc = json.loads((tmp_path / "run-report.json").read_text())
    assert doc["node_evaluations"][0]["verdict"] == "WARN"
    assert doc["node_evaluations"][0]["accuracy"] == 72
    assert "eval_unverified" in doc


def test_the_coder_prompt_carries_the_judges_issues_on_a_bounce():
    """Without this the bounce is a blind re-run that reproduces the same diff and burns the attempt
    budget — the failure the quality-gate injection block one line up already documents."""
    src = inspect.getsource(nodes.coder)
    assert 'state.get("eval_gap")' in src, "the coder never reads the eval gap"
    assert "INDEPENDENT ACCURACY EVALUATION" in src
    assert "node_evaluations" in src, "the judge's own issues[] are not injected"
