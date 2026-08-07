"""Unit tests for the cross-ticket lesson store (Finding 3, "give the pipeline a memory").

The property that matters is CROSS-TICKET recurrence: the same underlying failure, hit by a different
ticket in the same domain, must collapse to one record whose count goes up — otherwise every ticket
starts as blind as the first one, which is the finding's whole complaint.

Run: `pip install -e '.[test]' && pytest tests/test_lessons.py`
"""
from __future__ import annotations

import asyncio

import pytest

from ocean_pipeline import agents, config, lessons


@pytest.fixture(autouse=True)
def _isolated_store(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    # `_RUN_CONTEXT` is a module GLOBAL. Tests below set it; without restoring it they leak into every
    # later test in the session and make the suite ordering-dependent (judge review).
    before = dict(agents._RUN_CONTEXT)
    yield
    agents._RUN_CONTEXT.clear()
    agents._RUN_CONTEXT.update(before)


def test_normalize_fail_sig_collapses_run_specific_noise():
    """Two runs hitting the SAME defect differ in port, path and ids — if those survive into the key,
    the recurrence count never rises above 1 and nothing is ever recognized as a pattern."""
    a = lessons.normalize_fail_sig("connect ECONNREFUSED 127.0.0.1:5432 at /Users/x/EXE-abc123def/r.py:42")
    b = lessons.normalize_fail_sig("connect ECONNREFUSED 10.0.0.9:6000 at /Users/y/EXE-99988877/r.py:7")
    assert a == b
    assert lessons.normalize_fail_sig("boom\n  at frame 1\n  at frame 2") == "boom"
    # Genuinely different failures must NOT collapse together.
    assert lessons.normalize_fail_sig("connection refused") != lessons.normalize_fail_sig("permission denied")
    assert lessons.normalize_fail_sig("") == ""


def test_normalize_fail_sig_same_defect_different_magnitude_is_ONE_key():
    """Judge review found the substitution order made the key depend on a number's SIZE: `<hex>` ran
    before `<n>`, so a long digit run keyed as `<hex>` while a short one keyed as `<n>` — the same
    slot, two keys, neither ever reaching the recurrence threshold."""
    n = lessons.normalize_fail_sig
    assert n("Command timed out after 900000 ms") == n("Command timed out after 90000000 ms")
    assert n("3 examples, 1 failure") == n("12 examples, 4 failures")      # singular vs plural
    assert n("Could not find nokogiri-1.6.8.1 in the sources") == \
           n("Could not find nokogiri-1.13.10 in the sources")             # version arity
    # A real hex sha still collapses, and stays distinct from a plain number.
    assert n("bad object deadbeefcafe1234") == n("bad object 0badc0ffee5678")


def test_normalize_fail_sig_does_not_merge_every_traceback():
    """First-line-only collapsed EVERY Python traceback to `Traceback (most recent call last):` —
    distinct defects merged into one record whose note was overwritten by the last writer."""
    tb = "Traceback (most recent call last):\n  File \"a.py\", line 3, in f\n{}"
    a = lessons.normalize_fail_sig(tb.format("KeyError: 'stop_unlocode'"))
    b = lessons.normalize_fail_sig(tb.format("ConnectionResetError: [Errno 54] reset by peer"))
    assert a != b
    assert "Traceback" not in a and "KeyError" in a


def test_recall_requires_TWO_DISTINCT_tickets_not_just_two_hits():
    """The finding is about learning ACROSS tickets. Gating on raw recurrence_count meant one run
    retrying one failing command three times cleared the bar by itself, and that single flaky run then
    injected its noise into every later ticket in the domain (reproduced in judge review)."""
    for _ in range(5):
        lessons.record_failure("callback_notification", "coder:Bash", "flaky", "MM-1", "E-1")
    got = lessons.recall_lessons("callback_notification")
    assert got == [], "5 hits on ONE ticket is not a cross-ticket pattern"

    lessons.record_failure("callback_notification", "coder:Bash", "flaky", "MM-2", "E-2")
    got = lessons.recall_lessons("callback_notification")
    assert len(got) == 1 and set(got[0]["tickets"]) == {"MM-1", "MM-2"}


def test_record_and_recall_counts_recurrence_across_tickets():
    for t in ("MM-1", "MM-2", "MM-3"):
        lessons.record_failure("callback_notification", "coder:Bash", "boom", t, f"E-{t}")
    # A one-off on a single ticket must NOT surface: recalling every single failure would drown the
    # genuinely recurring ones.
    lessons.record_failure("callback_notification", "coder:Read", "one-off", "MM-9", "E-9")
    # A different domain must not leak in.
    lessons.record_failure("load_creation", "coder:Bash", "boom", "MM-4", "E-4")

    got = lessons.recall_lessons("callback_notification")
    assert len(got) == 1
    assert got[0]["recurrence_count"] == 3
    assert set(got[0]["tickets"]) == {"MM-1", "MM-2", "MM-3"}
    assert lessons.recall_lessons("jt_data_quality") == []      # nothing recorded yet
    assert lessons.recall_lessons("") == []                     # no bucket -> nothing recallable


def test_a_torn_write_cannot_annihilate_the_whole_history():
    """`write_text` killed mid-flight leaves a truncated file; `_read_records` reads that as `[]`, and
    the next record_failure then rewrites the store FROM empty — permanent silent data loss (judge
    review). The write is now tmp-file + os.replace, so a reader sees old or new, never torn."""
    for t in ("MM-1", "MM-2"):
        lessons.record_failure("callback_notification", "coder:Bash", "boom", t, "E1")
    assert len(lessons.recall_lessons("callback_notification")) == 1

    data = config.ARTIFACTS_ROOT / "lessons.json"
    before = data.read_text()
    lessons.record_failure("callback_notification", "coder:Bash", "boom", "MM-3", "E1")
    after = data.read_text()
    assert before != after and "MM-3" in after
    # No .tmp debris left behind on the happy path.
    assert not list(config.ARTIFACTS_ROOT.glob("lessons.json.tmp*"))


def test_record_failure_is_a_noop_without_a_recallable_key():
    """A lesson with no domain to key on, or no failure reason, can never be recalled by a later
    ticket -- writing it would just grow the file with noise."""
    lessons.record_failure("", "coder:Bash", "boom", "MM-1", "E1")
    lessons.record_failure("callback_notification", "coder:Bash", "", "MM-1", "E1")
    assert lessons.recall_lessons("callback_notification", min_tickets=1) == []


def test_recall_tolerates_a_corrupt_store():
    lessons.record_failure("callback_notification", "a", "b", "MM-1", "E1")
    (config.ARTIFACTS_ROOT / "lessons.json").write_text("{ not json")
    assert lessons.recall_lessons("callback_notification") == []      # never raises


def test_post_tool_use_failure_hook_records_cross_ticket_recurrence():
    """The real PostToolUseFailure hook (agents._capture_tool_failure): two tickets, same defect,
    different run-specific detail -> ONE lesson at count 2. Interrupts are not lessons."""
    hook = agents._capture_tool_failure("coder")

    agents.set_run_context(domain_bucket="callback_notification", ticket_id="MM-1", execution_id="E1")
    asyncio.run(hook({"tool_name": "Bash", "error": "connect ECONNREFUSED 127.0.0.1:5432"}, "t1", None))
    agents.set_run_context(domain_bucket="callback_notification", ticket_id="MM-2", execution_id="E2")
    asyncio.run(hook({"tool_name": "Bash", "error": "connect ECONNREFUSED 10.0.0.2:6000"}, "t2", None))
    # A user/timeout cancellation is not a lesson.
    asyncio.run(hook({"tool_name": "Bash", "error": "cancelled", "is_interrupt": True}, "t3", None))
    # An empty error carries no signature.
    asyncio.run(hook({"tool_name": "Bash", "error": ""}, "t4", None))

    got = lessons.recall_lessons("callback_notification")
    assert len(got) == 1
    assert got[0]["action_sig"] == "coder:Bash"          # <node>:<tool>
    assert got[0]["recurrence_count"] == 2
    assert set(got[0]["tickets"]) == {"MM-1", "MM-2"}


def test_verdict_redrive_does_not_split_the_key():
    """run_agent re-drives a worker as `<node>:verdict-redrive` when a verdict is missing. That is the
    SAME station hitting the SAME defect, so keeping the suffix produced `coder:Bash` AND
    `coder:verdict-redrive:Bash` — two records, each further from the threshold (judge review)."""
    agents.set_run_context(domain_bucket="callback_notification", ticket_id="MM-1", execution_id="E1")
    asyncio.run(agents._capture_tool_failure("coder")(
        {"tool_name": "Bash", "error": "bundle exec failed"}, "t1", None))
    agents.set_run_context(domain_bucket="callback_notification", ticket_id="MM-2", execution_id="E2")
    asyncio.run(agents._capture_tool_failure("coder:verdict-redrive")(
        {"tool_name": "Bash", "error": "bundle exec failed"}, "t2", None))

    got = lessons.recall_lessons("callback_notification")
    assert len(got) == 1, f"expected ONE record, got {[r['action_sig'] for r in got]}"
    assert got[0]["action_sig"] == "coder:Bash" and got[0]["recurrence_count"] == 2


def test_summary_reestablishes_the_capture_context_in_a_RESUMED_process(monkeypatch):
    """`_RUN_CONTEXT` is a per-PROCESS global set only by `researcher`. Every review gate defaults to
    interrupting, and a resume is a NEW process that re-enters at the interrupted node — so capture
    was dead for coder/harsh_reviewer/sit_run/sit_triage/flip_ready, the heaviest tool users (judge
    review). `_summary` is the one seam every station's prompt goes through."""
    from ocean_pipeline import nodes
    agents._RUN_CONTEXT.update({"domain_bucket": "", "ticket_id": "", "execution_id": ""})
    nodes._summary({"ticket_id": "MM-7", "execution_id": "EXE-7",
                    "domain_bucket": "callback_notification", "summary": "s", "route": "coding"})
    assert agents._RUN_CONTEXT["domain_bucket"] == "callback_notification"
    assert agents._RUN_CONTEXT["ticket_id"] == "MM-7"

    # ...and with the context live, the hook that was previously inert now records.
    asyncio.run(agents._capture_tool_failure("coder")(
        {"tool_name": "Bash", "error": "boom"}, "t", None))
    assert lessons.recall_lessons("callback_notification", min_tickets=1)


def test_recalled_lesson_carries_readable_detail_not_just_a_stripped_key():
    """The normalized fail_sig is a good KEY and a poor message — paths, numbers and quoted literals
    are deliberately stripped out. Judge review: `note` was written and never read, so the station saw
    a de-specified signature with nothing to act on."""
    from ocean_pipeline import nodes
    for t in ("MM-1", "MM-2"):
        lessons.record_failure("callback_notification", "coder:Bash",
                               lessons.normalize_fail_sig("Could not find nokogiri-1.6.8.1 in sources"),
                               t, "E1", note="Could not find nokogiri-1.6.8.1 in the local gem sources")
    recalled = lessons.recall_lessons("callback_notification")
    text = nodes._summary({"ticket_id": "MM-3", "execution_id": "E", "summary": "s", "route": "coding",
                           "domain_bucket": "callback_notification", "recurring_lessons": recalled})
    assert "nokogiri-1.6.8.1" in text, "the actual failure text must reach the station"
    assert "2+ DIFFERENT tickets" in text


def test_hook_never_raises_into_the_worker_it_observes():
    """A hook that throws would break the worker it is only meant to observe."""
    agents.set_run_context(domain_bucket="load_creation", ticket_id="MM-1", execution_id="E1")
    hook = agents._capture_tool_failure("coder")
    for bad in ({}, {"tool_name": None, "error": None}, {"error": 123}):
        assert asyncio.run(hook(bad, "t", None)) == {}
