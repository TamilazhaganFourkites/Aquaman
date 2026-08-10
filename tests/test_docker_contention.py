"""ocean-pipeline-issues-tracker #18 — Docker contention across concurrent runs.

**#18 was already fixed; the tracker's status was never flipped.** `_docker_used_gb` and the
`docker_over_committed` branch of `_docker_preflight_reason` both exist, and the helper's own
docstring cites the motivating run ("manual-findings #18: EXE-caa5c082 died at Step-0 because two
other tickets' full stacks were up"). Option (b) from the same item — a concurrency cap on the SIT
stage — is `MAX_CONCURRENT_SIT` plus the sit slots.

What was missing was a TEST. These lock the behaviour in place, including the two properties that
are easy to break by accident:

  * failure returns **None**, not 0.0 — the callers are written `if used_gb is not None`, so a
    variant returning 0.0 would silently make every "could not measure" read as "machine is idle"
    while leaving that guard as dead code;
  * subtraction happens **once**. The over-commit check does `mem_gb - used_gb` itself, so a caller
    that also pre-subtracts would double-count and reject runs that fit.

I learned the second one by writing exactly that bug: a duplicate `_docker_used_gb` shadowing this
one, plus a pre-subtraction in the preflight. Reverted.
"""
from __future__ import annotations

import inspect
import subprocess

import pytest

from ocean_pipeline import config, nodes


class _Proc:
    def __init__(self, stdout, returncode=0):
        self.stdout, self.returncode = stdout, returncode


def _stats(monkeypatch, lines, returncode=0):
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **k: _Proc("\n".join(lines) + "\n", returncode))


def test_there_is_exactly_one_definition_of_the_helper():
    """The duplication guard. Two definitions silently shadow, and the survivor's contract may
    differ from the one every caller was written against."""
    src = (nodes.__file__ and open(nodes.__file__).read()) or ""
    assert src.count("def _docker_used_gb(") == 1, "the helper is defined more than once"


def test_usage_across_containers_is_summed(monkeypatch):
    _stats(monkeypatch, ["a\t1.0GiB / 9.7GiB", "b\t2.5GiB / 9.7GiB"])
    assert nodes._docker_used_gb() == pytest.approx(3.5, abs=0.05)


def test_this_runs_own_containers_are_excluded(monkeypatch):
    """Their memory is what the budget is being computed FOR. Counting it makes a run compete with
    itself on a retry, when its own warm SIT stack is still up."""
    _stats(monkeypatch, ["EXE-mine-tracking\t3.0GiB / 9.7GiB", "EXE-other-kafka\t1.0GiB / 9.7GiB"])
    assert nodes._docker_used_gb("EXE-mine") == pytest.approx(1.0, abs=0.05)
    assert nodes._docker_used_gb("") == pytest.approx(4.0, abs=0.05)


def test_unmeasurable_usage_returns_None_not_zero(monkeypatch):
    """THE contract. Callers are written `if used_gb is not None` — returning 0.0 would read as
    "the machine is idle" and turn that guard into dead code."""
    def _boom(*a, **k):
        raise OSError("docker went away")
    monkeypatch.setattr(subprocess, "run", _boom)
    assert nodes._docker_used_gb() is None

    _stats(monkeypatch, [""], returncode=1)
    assert nodes._docker_used_gb() is None


def test_both_callers_guard_on_None():
    """A caller that dropped the `is not None` check would treat "could not measure" as zero usage
    and pass a preflight on a machine it never looked at.

    NOTE the missing `continue`: a version that skips any caller whose source does not
    mention `_docker_used_gb` — which is precisely the mutation this test exists to catch. Delete
    the call entirely and the old loop skipped that function and passed; the test could only ever
    fail on a caller that still called the helper AND still guarded it, i.e. on correct code."""
    for fn in (nodes._docker_preflight_reason, nodes._try_acquire_build_slot):
        src = inspect.getsource(fn)
        # The CALL, not the name. Both functions' DOCSTRINGS mention `_docker_used_gb` while
        # explaining the None contract, so a bare `"_docker_used_gb" in src` stayed true after the
        # call itself was replaced with a literal — a mutant that makes the headroom check inert
        # survived it. Prose about a mechanism is not the mechanism.
        assert "_docker_used_gb(" in src, (
            f"{fn.__name__} no longer CALLS the helper — the headroom check is inert, which reads "
            f"identically to a machine with plenty of room")
        assert "is not None" in src, f"{fn.__name__} uses the helper without the None guard"


def test_the_over_commit_check_subtracts_exactly_once():
    """`mem_gb - used_gb` lives in the over-commit branch. A caller that ALSO pre-subtracted would
    double-count and reject runs that fit — which is the bug this test was written after making."""
    src = inspect.getsource(nodes._docker_preflight_reason)
    assert src.count("_docker_used_gb(") == 1, "the helper is called more than once in one pass"
    assert "mem_gb - used_gb" in src
    assert "mem_gb = max(0.0, mem_gb - " not in src, "the budget is pre-subtracted AND subtracted"


def test_the_over_commit_reason_is_transient_and_says_so():
    """Distinct from insufficient total capacity: more memory does not appear between attempts, but
    a concurrent run DOES finish. The operator needs to know which one they hit."""
    src = inspect.getsource(nodes._docker_preflight_reason)
    assert "docker_over_committed" in src
    assert "insufficient_docker_resources" in src
    assert "manual-findings #18" in src, "the motivating run is no longer cited"


def test_the_sit_concurrency_cap_actually_bounds_concurrency():
    """Option (b) of the same tracker item. Both halves matter: the cap bounds how many stacks can
    exist, the subtraction handles the ones that already do.

    Asserting `MAX_CONCURRENT_SIT >= 1` and `callable(_acquire_sit_slot)` would be vacuous —
    neither can fail for any code state a human would write. `>= 1` is true of every legal value of
    the constant, and `callable()` of a module-level `def` is true by construction. So it drove the
    REAL slot machinery instead: fill the cap with `flock`-backed slots, then prove the next
    request is refused, then prove a release frees one.
    """
    held = [f"EXE-cap-{i}" for i in range(config.MAX_CONCURRENT_SIT)]
    try:
        for exec_id in held:
            assert nodes._try_acquire_sit_slot(exec_id), f"{exec_id} could not take a free slot"
        # One past the cap. A cap that does not refuse is not a cap.
        assert not nodes._try_acquire_sit_slot("EXE-cap-overflow"), (
            f"a {config.MAX_CONCURRENT_SIT + 1}th concurrent SIT was granted a slot — "
            f"MAX_CONCURRENT_SIT bounds nothing")
        # And it is a cap, not a permanent wall: releasing one must free exactly one.
        nodes._release_sit_slot(held[0])
        assert nodes._try_acquire_sit_slot("EXE-cap-overflow"), "a released slot was never reusable"
        held[0] = "EXE-cap-overflow"
    finally:
        for exec_id in held:
            nodes._release_sit_slot(exec_id)


def test_the_build_slot_also_checks_headroom():
    """A third caller, and the one that runs earliest — an image build on a saturated VM is the same
    failure one station sooner."""
    src = inspect.getsource(nodes._try_acquire_build_slot)
    assert "_docker_used_gb" in src
    assert "docker_budget_for_build" in src
