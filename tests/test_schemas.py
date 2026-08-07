"""Unit tests for schemas.py's model-level self-consistency validators — specifically
AutomationVerdict's two fidelity_rung-capping validators (Finding 2a/2d, architecture review of PR
Aquaman#4/fk-aideveloper#294). These run at Pydantic construction time, before nodes.sit_triage ever
reads the field, so graph.after_sit_triage's gate sees the already-capped value automatically; these
tests exercise the validators directly rather than through a full graph run (test_graph.py covers the
routing consequences of an already-gated state dict, not the gating arithmetic itself).

Run: `pip install -e '.[test]' && pytest tests/test_schemas.py`
"""
from __future__ import annotations

from ocean_pipeline import schemas


def _verdict(**overrides) -> schemas.AutomationVerdict:
    base = {"ticket_id": "MM-1", "automation_result": "passed"}
    base.update(overrides)
    return schemas.AutomationVerdict(**base)


def test_coerce_fidelity_rung_clamps_out_of_range():
    assert _verdict(fidelity_rung=2).fidelity_rung == 2
    assert _verdict(fidelity_rung=5).fidelity_rung == 0     # out of {0,1,2} -> safest default
    assert _verdict(fidelity_rung=-1).fidelity_rung == 0
    assert _verdict(fidelity_rung="not a number").fidelity_rung == 0
    assert _verdict().fidelity_rung == 0                    # absent entirely -> default 0


def test_cap_rung_on_unmocked_hit():
    # Non-empty unmocked_paths_hit caps rung 2 down to 1, never raises it, never touches rung <= 1.
    assert _verdict(fidelity_rung=2, unmocked_paths_hit=["/api/v1/foo"]).fidelity_rung == 1
    assert _verdict(fidelity_rung=2, unmocked_paths_hit=[]).fidelity_rung == 2
    assert _verdict(fidelity_rung=1, unmocked_paths_hit=["/api/v1/foo"]).fidelity_rung == 1
    assert _verdict(fidelity_rung=0, unmocked_paths_hit=["/api/v1/foo"]).fidelity_rung == 0


def test_cap_rung_on_missing_ref_load():
    # The self-contradiction: claims deep-engine (needs_ref_load) + full fidelity (rung 2) + never
    # actually used real-load replay -> downgraded to rung 1.
    assert _verdict(fidelity_rung=2, needs_ref_load=True, ref_load_used=False).fidelity_rung == 1
    # Genuinely full fidelity: claimed deep-engine AND used ref-load -> stays 2.
    assert _verdict(fidelity_rung=2, needs_ref_load=True, ref_load_used=True).fidelity_rung == 2
    # Never claimed to need ref-load (the common case) -> untouched regardless of ref_load_used.
    assert _verdict(fidelity_rung=2, needs_ref_load=False, ref_load_used=False).fidelity_rung == 2
    # Already below rung 2 -> the validator's guard doesn't fire (nothing to downgrade further).
    assert _verdict(fidelity_rung=1, needs_ref_load=True, ref_load_used=False).fidelity_rung == 1


def test_both_rung_capping_validators_combine_correctly():
    # Both triggers true at once: order between the two model_validators must not matter (both only
    # ever cap rung 2 down to 1 -- see each validator's own docstring) and must not double-decrement
    # to 0 or leave 2 uncapped.
    v = _verdict(fidelity_rung=2, unmocked_paths_hit=["/x"], needs_ref_load=True, ref_load_used=False)
    assert v.fidelity_rung == 1


def test_coerce_execution_mode_normalizes_and_flags_unknown():
    """Finding 2d: an unrecognized mode must become "unknown" (which trips the real-service gate),
    never silently read as one of the two trusted values."""
    assert _verdict(execution_mode="local-mock-first").execution_mode == "local-mock-first"
    assert _verdict(execution_mode="QAT_FALLBACK").execution_mode == "qat-fallback"   # case/underscore
    assert _verdict(execution_mode="nonsense").execution_mode == "unknown"
    assert _verdict(execution_mode=None).execution_mode == "unknown"
    assert _verdict().execution_mode == "local-mock-first"                            # default


def test_test_fault_latches_test_edited_before_it_is_erased():
    """Finding 2e: `failure_class: "test_fault"` is coerced to "" (it's never terminal), which used to
    erase the only trace that a test was rewritten mid-run. The mode="before" model validator must
    latch test_edited=True from that raw value first."""
    v = _verdict(failure_class="test_fault")
    assert v.failure_class == ""        # still coerced, unchanged behavior
    assert v.test_edited is True        # but the signal survives
    # An explicitly-reported edit is preserved, and a clean run stays False.
    assert _verdict(test_edited=True).test_edited is True
    assert _verdict().test_edited is False
    assert _verdict(failure_class="code_fault").test_edited is False


def test_ac_coverage_and_unmocked_paths_default_empty_and_dont_crash_on_absence():
    v = _verdict()
    assert v.unmocked_paths_hit == []
    assert v.ac_coverage == []
    assert v.ref_load_used is False
    assert v.needs_ref_load is False
