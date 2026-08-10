"""qat-handoff Phase 2.1 — the pre-registered definitions behind a GENUINE Step-9b pass.

Phase 2's *measurement* is a multi-week passive wait for runs. Its INSTRUMENT is not, and this is
that instrument, built now so the definitions are fixed in writing BEFORE the first run and cannot
be tuned to fit results.

    genuine PASSED = verdict == PASSED
                   AND judge_rounds_observed >= 1        (current-pass, sentinel-matched, Phase 1.2)
                   AND zero CANNOT-VERIFY among assertion-bearing methods, CROSS-CHECKED FROM THE FILE
                   AND skip_guarded <= 1/3 of all test methods

Every conjunct after the first exists because the first is agent-written. The recurring rule in this
codebase — deterministic disk evidence outranks self-report — is applied here for the fifth time:
an agent that simply OMITS entries from `cannot_verify_methods` makes that conjunct true, so the
file's own `@pytest.mark.needs_env` count may only ever RAISE the JSON's, never lower it.

The 1/3 floor is a PRE-REGISTERED judgment call, not derived from data. Motivating case: MM-14475's
file had 14 of 17 scenarios skip-guarded (82%). Without a floor it could have scored PASSED on its 3
remaining methods and sent a human to QAT to verify 3/17 of the ticket.
"""
from __future__ import annotations

import inspect

import pytest

from ocean_pipeline import nodes, quality


def _src(methods: str) -> str:
    return "import pytest\n\n\nclass TestX:\n" + methods


# --------------------------------------------------------------- the census
def test_the_assert_clause_is_what_discriminates():
    """Validated against the real corpus (414 files, 1,577 methods): 226 methods (14%) have no bare
    assert, while leading-unconditional pytest.skip = 0 and @pytest.mark.skip = 7 (0.4%). `ast.Assert`
    is the right primitive — INV-16 mandates bare `assert` over `pytest.fail()`."""
    c = quality.sit_method_census(_src(
        "    def test_has_assert(self):\n        assert 1\n"
        "    def test_no_assert(self):\n        pass\n"))
    assert c["total"] == 2
    assert c["assertion_bearing"] == 1
    assert c["skip_guarded"] == 1


@pytest.mark.parametrize("body,bearing", [
    ("    def test_a(self):\n        assert 1\n", 1),
    ("    @pytest.mark.skip\n    def test_a(self):\n        assert 1\n", 0),
    ("    def test_a(self):\n        pytest.skip('no')\n        assert 1\n", 0),
    ("    def test_a(self):\n        pass\n", 0),
])
def test_each_skip_guard_shape(body, bearing):
    assert quality.sit_method_census(_src(body))["assertion_bearing"] == bearing


def test_needs_env_is_counted_only_on_assertion_bearing_methods():
    """`@pytest.mark.needs_env` is the DISK ANALOGUE of `cannot_verify_methods` — SKILL.md mandates
    the tag precisely so the classification survives past the skill's own run."""
    c = quality.sit_method_census(_src(
        "    @pytest.mark.needs_env\n    def test_a(self):\n        assert 1\n"
        "    @pytest.mark.needs_env\n    def test_b(self):\n        pass\n"))
    assert c["needs_env_on_assertion_bearing"] == 1, "a skip-guarded method must not inflate the count"


def test_an_unparsed_file_is_not_a_clean_census():
    """"Could not count" and "counted zero" are opposite facts."""
    c = quality.sit_method_census("def broken(:\n")
    assert c["parsed"] is False
    assert quality.coverage_floor_met(c) is False


def test_a_file_with_no_tests_does_not_pass_the_floor_vacuously():
    assert quality.coverage_floor_met(quality.sit_method_census("x = 1\n")) is False


# --------------------------------------------------------------- the floor
def test_the_motivating_case_fails_the_floor():
    """MM-14475: 14 of 17 scenarios skip-guarded (82%). Any floor below 82% excludes it; 1/3 is the
    pre-registered bar. Without a floor it scores PASSED on 3 of 17 methods."""
    body = "".join(f"    def test_ok{i}(self):\n        assert 1\n" for i in range(3))
    body += "".join(f"    def test_guard{i}(self):\n        pass\n" for i in range(14))
    c = quality.sit_method_census(_src(body))
    assert (c["total"], c["skip_guarded"]) == (17, 14)
    assert quality.coverage_floor_met(c) is False


def test_the_floor_is_one_third_and_is_pre_registered():
    assert quality.SKIP_GUARD_FLOOR == pytest.approx(1 / 3)
    # exactly 1/3 passes, one more fails
    ok = quality.sit_method_census(_src(
        "".join(f"    def test_a{i}(self):\n        assert 1\n" for i in range(2))
        + "    def test_g(self):\n        pass\n"))
    assert (ok["total"], ok["skip_guarded"]) == (3, 1)
    assert quality.coverage_floor_met(ok) is True
    bad = quality.sit_method_census(_src(
        "    def test_a(self):\n        assert 1\n"
        "    def test_g1(self):\n        pass\n"
        "    def test_g2(self):\n        pass\n"))
    assert quality.coverage_floor_met(bad) is False


# --------------------------------------------------------------- the composite
_CLEAN = {"parsed": True, "total": 3, "assertion_bearing": 3, "skip_guarded": 0,
          "needs_env_on_assertion_bearing": 0}


def test_a_genuine_pass_needs_every_conjunct():
    ok, why = nodes._genuine_passed({"qa_authoring_review_verdict": "PASSED"}, 1, _CLEAN)
    assert ok is True and why == ""


@pytest.mark.parametrize("review,rounds,census,expect_in_why", [
    ({"qa_authoring_review_verdict": "NEEDS_WORK"}, 1, _CLEAN, "verdict is"),
    ({"qa_authoring_review_verdict": "PASSED"}, 0, _CLEAN, "no Step-9b judge dispatch"),
    ({"qa_authoring_review_verdict": "PASSED"}, 1, {"parsed": False}, "could not be parsed"),
    ({"qa_authoring_review_verdict": "PASSED", "cannot_verify_methods": ["m"]}, 1, _CLEAN,
     "CANNOT-VERIFY"),
    ({"qa_authoring_review_verdict": "PASSED"}, 1,
     {**_CLEAN, "needs_env_on_assertion_bearing": 2}, "CANNOT-VERIFY"),
    ({"qa_authoring_review_verdict": "PASSED"}, 1,
     {"parsed": True, "total": 3, "skip_guarded": 3, "needs_env_on_assertion_bearing": 0},
     "coverage floor"),
    ({}, 1, _CLEAN, "verdict is"),
    ("not a dict", 1, _CLEAN, "no review verdict"),
])
def test_each_conjunct_can_refuse(review, rounds, census, expect_in_why):
    ok, why = nodes._genuine_passed(review, rounds, census)
    assert ok is False
    assert expect_in_why in why, why


def test_the_file_may_raise_the_agents_cannot_verify_count_never_lower_it():
    """The fifth instance of the standing rule. An agent that OMITS entries makes the conjunct true —
    a self-report RAISING trust, which Phase 1.2 exists to reject."""
    # agent claims zero, the FILE shows two -> refused
    ok, why = nodes._genuine_passed({"qa_authoring_review_verdict": "PASSED",
                                     "cannot_verify_methods": []}, 1,
                                    {**_CLEAN, "needs_env_on_assertion_bearing": 2})
    assert ok is False and "observed 2" in why
    # agent claims two, the FILE shows zero -> still refused (max, not the file alone)
    ok, why = nodes._genuine_passed({"qa_authoring_review_verdict": "PASSED",
                                     "cannot_verify_methods": ["a", "b"]}, 1, _CLEAN)
    assert ok is False and "claimed 2" in why


def test_the_station_computes_the_census_from_the_file_not_the_json():
    """Both the counts AND `test_file` in the review JSON are agent-written; pointing the latter at a
    cleaner file would pass the floor on something that is not the authored test."""
    src = inspect.getsource(nodes.sit_author)
    assert "quality.sit_method_census" in src
    # The CALL ITSELF, not a window: a neighbouring line also mentions `authored_path`, so a window
    # check passes even when the census argument has been swapped for the JSON's own path.
    i = src.index("quality.sit_method_census")
    call = src[i:src.index("\n", src.index(")", i))]
    assert "authored_path" in call, f"the census does not read the path Aquaman recorded: {call}"
    # Quote-agnostic: the mutant that exposed this used single quotes and slipped past a
    # double-quoted substring check.
    flat = call.replace("'", '"')
    assert "test_file" not in flat, f"the census path comes from the agent-written JSON: {call}"


def test_the_state_declares_the_phase2_keys():
    from ocean_pipeline.state import OceanState
    for k in ("qa_judge_rounds_observed", "qa_genuine_passed", "qa_genuine_passed_why",
              "qa_method_census"):
        assert k in OceanState.__annotations__, f"{k} would be dropped by LangGraph"
