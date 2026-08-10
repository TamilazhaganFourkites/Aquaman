"""Tests for scripts/eval_routing.py — the routing-accuracy harness.

WHY THIS FILE EXISTS: this script is the measurement instrument every severity claim in the defect
queue is supposed to be checked against, and it had no tests at all. Its scorer counted an
INFRASTRUCTURE failure as a wrong answer, so a machine with no `JIRA_API_TOKEN` reported
`route_accuracy: 0.0` — a number indistinguishable from a router that gets every ticket wrong.

The script imports the Claude Agent SDK path at module load, so it is loaded by file location and
only its pure functions (`_score`, `_provenance`) are exercised; `_predict` drives a real worker and
is out of scope here by design.
"""
from __future__ import annotations

import json
from pathlib import Path

from conftest import load_module_by_path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "eval_routing.py"
ev = load_module_by_path(_SCRIPT, "eval_routing")


def _row(ticket_id: str, gt: str, pred: str, bucket_ok=None) -> dict:
    return {"ticket_id": ticket_id, "gt_route": gt, "pred_route": pred, "route_ok": pred == gt,
            "gt_bucket": "", "pred_bucket": "", "bucket_ok": bucket_ok}


def test_an_unmeasurable_ticket_is_not_a_wrong_answer():
    """The headline defect. `_predict` returns `__error__` for ANY failure — unset token, network
    blip, malformed verdict. Counting those as misses meant "could not measure" and "measured, and
    it failed" shared one number, on the instrument used to check everything else."""
    results = [_row(f"MM-{i}", "coding", ev.ERROR_ROUTE) for i in range(9)]
    s = ev._score(results)
    assert s["route_accuracy"] is None, "an all-errors run must not report an accuracy at all"
    assert s["scored"] == 0 and s["errored"] == 9
    assert s["error_ticket_ids"] == [f"MM-{i}" for i in range(9)], "the errored tickets must be named"

    # Mixed: errors leave the denominator entirely, they do not drag it down.
    mixed = ([_row(f"E-{i}", "coding", ev.ERROR_ROUTE) for i in range(5)]
             + [_row(f"OK-{i}", "coding", "coding") for i in range(20)])
    s = ev._score(mixed)
    assert (s["n"], s["scored"], s["errored"]) == (25, 20, 5)
    assert s["route_accuracy"] == 1.0, "20 of 20 measured tickets were correct"


def test_no_accuracy_is_published_below_the_row_floor():
    """Nine rows is the real corpus size: one ticket moves the number 11 points. The eval still runs
    and still prints per-ticket results — it just refuses a headline it cannot support."""
    s = ev._score([_row(f"MM-{i}", "coding", "coding") for i in range(9)])
    assert s["underpowered"] is True
    assert s["route_accuracy"] is None and s["domain_bucket_accuracy"] is None
    assert s["route_correct"] == 9, "the raw counts are still reported — only the ratio is withheld"

    s = ev._score([_row(f"MM-{i}", "coding", "coding" if i < 20 else "rca")
                   for i in range(ev.MIN_SCORABLE_ROWS + 5)])
    assert s["underpowered"] is False and s["route_accuracy"] == 0.8


def test_the_floor_is_pinned_absolutely():
    """A relative assertion follows the constant anywhere; this is the value the argument rests on."""
    assert ev.MIN_SCORABLE_ROWS == 20


def test_a_bucket_is_never_scored_off_an_errored_route():
    """If the route could not be predicted, the bucket beside it is not evidence either."""
    s = ev._score([_row("MM-1", "coding", ev.ERROR_ROUTE, bucket_ok=False)]
                  + [_row(f"MM-{i}", "coding", "coding", bucket_ok=True) for i in range(2, 24)])
    assert s["domain_bucket_scored"] == 22, "the errored row must not appear in the bucket denominator"
    assert s["domain_bucket_accuracy"] == 1.0


def test_the_report_can_be_re_derived_not_merely_believed():
    """A bare percentage with no run id, corpus or code version is an assertion. Provenance is what
    lets a later run be compared to this one."""
    p = ev._provenance(str(_SCRIPT))
    assert p["run_id"].startswith("EVAL-") and len(p["run_id"]) > 8
    assert p["started_at"].endswith("+00:00"), "timestamps must be unambiguous (UTC)"
    assert p["corpus_path"].endswith("eval_routing.py") and Path(p["corpus_path"]).is_absolute()
    assert len(p["corpus_sha256_16"]) == 16, "the corpus must be identified by content, not just path"
    assert isinstance(p["git_dirty"], bool)

    # Never raises on a corpus that does not exist — provenance must not be able to fail the eval.
    missing = ev._provenance("/nonexistent/corpus.jsonl")
    assert missing["corpus_sha256_16"] == "" and missing["run_id"]

    # And it survives a JSON round-trip, since it ships inside the report.
    assert json.loads(json.dumps(p))["run_id"] == p["run_id"]
