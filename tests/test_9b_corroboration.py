"""qat-handoff Phase 1 — make the Step 9b verdict mean something.

Step 9b is the deepest check the pipeline has, and it was untrustworthy for a reason that turned out
to be one sentence: the `sit_author` prompt ended "a human reviews this draft next", and the skill
took it LITERALLY. Verified in the real artifacts — MM-14475: "No fresh-agent judge panel spawned …
a human reviews this draft next per the invocation"; MM-14457: "Deep Step-9b fresh-adversarial-judge
pass was intentionally deferred to the human review gate". Only 1 of 3 actually ran it.

That is also the correction to this repo's own C1 retrospective, which measured 3/3 NEEDS_WORK and
concluded the predictor was constant. The measurement was right; the explanation was not. A constant
verdict from a check that never fired is a different fact from one that did.

Rewording alone is NECESSARY BUT NOT SUFFICIENT — the graph really does edge sit_author ->
qa_review_gate, so an agent inferring "a human reviews next" is reasoning correctly about the world.
The weight is carried by corroboration at READ time, which is what most of this file tests.

The test cases below are the plan's own verification list, not invented ones.
"""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from ocean_pipeline import config, nodes

_EXE = "EXE-9b"


@pytest.fixture
def logdir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    d = config.artifacts_dir(_EXE)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write(logdir: Path, text: str) -> Path:
    p = logdir / "sit_author.log"
    with p.open("a", encoding="utf-8") as f:
        f.write(text)
    return p


def _started(desc: str, tid: str) -> str:
    """The exact line agents.py renders for a task_started event."""
    return f"⚡ sub-agent started: {desc} (task {tid})\n"


# --------------------------------------------------------------- the happy path
def test_a_real_judge_dispatch_is_counted(logdir):
    _write(logdir, _started("STEP9B-JUDGE round 1 — strict review", "t1"))
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 1


def test_one_judge_emitting_three_event_kinds_counts_as_one(logdir):
    """A single judge emits `⚙ Task`, then task_started, then N × task_progress. Counting any of the
    others turns one judge into N+2."""
    _write(logdir,
           "⚙ Task dispatching sub-agent: STEP9B-JUDGE round 1\n"
           + _started("STEP9B-JUDGE round 1", "t1")
           + "⚡ sub-agent progress: STEP9B-JUDGE round 1 (last tool: Read)\n"
             "⚡ sub-agent progress: STEP9B-JUDGE round 1 (last tool: Grep)\n"
             "⚡ sub-agent progress: STEP9B-JUDGE round 1 (last tool: Read)\n")
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 1


# --------------------------------------------------------------- the failure modes it must catch
def test_narration_without_a_dispatch_scores_zero(logdir):
    """THE case this exists for: an agent that PRINTS the round banner and defers the actual judge."""
    _write(logdir,
           "⟳ [Step 9b/round 1] Strict sibling-grounded review — 0 finding(s), fixing...\n"
           "✓ [Step 9b] Review passed — 0 blocking findings after 1 round(s).\n")
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 0


def test_step5d_gan_dispatches_are_not_misattributed(logdir):
    """Step 5d dispatches up to 4 sub-agents per round. A 12-dispatch 5d log must yield 0, not 12 —
    the sentinel is the only thing that discriminates."""
    for i in range(12):
        _write(logdir, _started(f"GAN adversary round {i // 4 + 1}", f"g{i}"))
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 0


def test_events_before_the_offset_belong_to_a_previous_pass(logdir):
    """Staleness: a prior pass's judges must not corroborate the current one."""
    _write(logdir, _started("STEP9B-JUDGE round 1", "old1"))
    offset = (logdir / "sit_author.log").stat().st_size
    _write(logdir, "===== sit_author @ pass start =====\n")
    assert nodes._judge_rounds_observed(_EXE, "sit_author", offset) == 0


def test_an_internal_redrive_does_not_zero_a_genuine_count(logdir):
    """`_drive_with_retry` re-drives internally and appends a fresh separator. Scanning from the LAST
    separator would erase a real count recorded before the retry; scanning from the pre-run_skill
    offset preserves it. This is the `verdict_path is None` retry case."""
    offset = nodes._log_offset(_EXE, "sit_author")
    _write(logdir, _started("STEP9B-JUDGE round 1", "t1"))
    _write(logdir, "===== sit_author @ pass start =====\n")   # the retry's separator, no new events
    assert nodes._judge_rounds_observed(_EXE, "sit_author", offset) == 1


def test_a_missing_or_unreadable_log_is_zero_never_none(logdir):
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 0
    assert nodes._judge_rounds_observed("EXE-does-not-exist", "sit_author", 0) == 0
    _write(logdir, "no separator, no events, just prose\n")
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 0


def test_the_offset_helper_never_raises_on_a_missing_log(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    assert nodes._log_offset("EXE-nope", "sit_author") == 0


# --------------------------------------------------------------- the collision detector
def test_a_count_above_the_ceiling_fails_closed(logdir):
    """Two rounds is the documented cap. Four sentinel-matched dispatches means the sentinel is
    colliding with something else — it must NOT present as a healthy 2."""
    for i in range(4):
        _write(logdir, _started(f"STEP9B-JUDGE round {i}", f"t{i}"))
    raw = nodes._judge_rounds_observed(_EXE, "sit_author", 0)
    assert raw == 4, "the raw count must be stored UNCLAMPED so the collision is visible"
    clamped = raw if 0 <= raw <= nodes._MAX_9B_ROUNDS else 0
    assert clamped == 0, "a colliding count must fail closed, not clamp to a plausible 2"


@pytest.mark.parametrize("raw,expect", [(0, 0), (1, 1), (2, 2), (3, 0), (4, 0), (12, 0), (-1, 0)])
def test_the_clamp_fails_closed_rather_than_capping(raw, expect):
    """Tests the PRODUCTION rule, not a copy of it in the test. `min(raw, 2)` would render a
    collision as a perfectly healthy 2 — indistinguishable from a real two-round pass."""
    assert nodes._corroborated_judge_rounds(raw) == expect


def test_a_reconnecting_judge_is_not_double_counted(logdir):
    """The same task id emitting `sub-agent started` twice (reconnect / re-narration) is ONE judge."""
    _write(logdir, _started("STEP9B-JUDGE round 1", "t1"))
    _write(logdir, _started("STEP9B-JUDGE round 1", "t1"))
    _write(logdir, _started("STEP9B-JUDGE round 2", "t2"))
    assert nodes._judge_rounds_observed(_EXE, "sit_author", 0) == 2


def test_the_station_applies_that_clamp():
    src = inspect.getsource(nodes.sit_author)
    assert "_corroborated_judge_rounds" in src, "sit_author never applies the ceiling"
    assert "observed_raw" in src, "the unclamped count is not recorded, so a collision is invisible"


# --------------------------------------------------------------- wiring
def _code_only(src: str) -> str:
    """Source with comment lines stripped.

    Needed because the fix's own comment QUOTES the sentence it removed, and a naive substring check
    then fails on the note documenting the change — the fourth time that trap has bitten in this
    codebase. Assert on what the agent actually receives (the prompt string), not on the prose
    explaining why."""
    return "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))


def _flat(text: str) -> str:
    """Whitespace-normalized, so an assertion does not depend on where a line happens to wrap."""
    return " ".join(text.split())


def test_the_prompt_no_longer_tells_the_skill_a_human_reviews_next():
    """1.1. The exact sentence the artifacts quote back as their reason for deferring."""
    src = _code_only(inspect.getsource(nodes.sit_author))
    assert "a human reviews this draft" not in src, (
        "the deferral-inviting wording is back — two of three real artifacts cite it verbatim")
    assert "MANDATORY on this pass" in src
    assert "STEP9B-JUDGE" in src, "the skill is never told the sentinel the corroboration requires"


def test_a_self_reported_claim_is_downgraded_without_disk_evidence():
    src = inspect.getsource(nodes.sit_author)
    assert "claimed" in src and "judge_rounds" in src
    assert "NOT spawned" in src, "a false judge_spawned claim is never surfaced"


def test_the_stale_review_json_is_unlinked_before_re_authoring():
    """1.5. A 9b verdict grades the test against the PRODUCT CODE, which the code_fault loop changes,
    so it is stale by construction. Fail-closed covers "missing", not "stale"."""
    src = inspect.getsource(nodes.sit_author)
    assert "qa-authoring-review.json" in src
    i_unlink = src.index(".unlink()")
    i_invoke = src.index("run_skill")
    assert i_unlink < i_invoke, "the stale verdict is deleted AFTER the skill runs, or not at all"


# --------------------------------------------------------------- the skill half
def _skill() -> str:
    p = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-qa-agent" / "SKILL.md"
    if not p.exists():
        pytest.skip("fk-aideveloper checkout has no ocean-qa-agent SKILL.md")
    return p.read_text()


def test_the_skill_mandates_the_sentinel_and_forbids_it_in_step_5d():
    text = _flat(_skill())
    assert "MUST carry the literal token `STEP9B-JUDGE`" in text, (
        "the sentinel is no longer MANDATED — the token appearing elsewhere is not the rule")
    assert "NEVER contain" in text or "never contain" in text, (
        "Step 5d is not forbidden the sentinel, so a GAN log would corroborate as 9b judges")


def test_passed_now_requires_reachability_not_merely_silence():
    """1.3. CANNOT-VERIFY is not in the blocking set, so an all-CANNOT-VERIFY round used to record
    PASSED — greenest exactly when the environment is most broken."""
    text = _skill()
    assert "UNVERIFIED" in text
    assert "CANNOT-VERIFY` on assertion-bearing methods" in _flat(text)
    assert "never `PASSED`" in text


def test_py_compile_is_round_zero_and_pytest_collect_is_not():
    """1.6. `pytest --collect-only` is PROVEN to create ~7 real QAT customers per invocation."""
    text = _skill()
    assert "python -m py_compile" in text
    assert "FILE_INVALID" in text
    assert "SKIP both judge rounds" in _flat(text)
    assert "collect-only" in text, "the reason py_compile is used instead is not recorded"


def test_the_verdict_json_has_a_documented_schema():
    """1.4. Three artifacts, three shapes; only `qa_authoring_review_verdict` was stable."""
    text = _skill()
    # Inside the schema TABLE only: these names also appear in surrounding prose, so a bare
    # substring check passes even after the table row is gone.
    table = text.split("Required keys in", 1)[1].split("Emit every key", 1)[0]
    for key in ("judge_spawned", "judge_rounds_run", "blocking_findings", "cannot_verify_methods",
                "skip_guarded_methods", "assertion_bearing_methods", "py_compile_ok",
                "py_compile_error", "test_file"):
        assert f"`{key}`" in table, f"{key} is not a row in the documented schema table"
    assert "an absent key and an empty one must not be the same signal" in _flat(text)
