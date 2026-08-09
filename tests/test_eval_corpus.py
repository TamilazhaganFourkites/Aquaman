"""The routing eval corpus must stay parseable, in-vocabulary, and BLIND.

`scripts/eval_corpus.jsonl` is the ground truth behind the only accuracy figure this pipeline has
produced that survived contamination. Keeping it in the repo is what makes that figure auditable —
but a ground truth nobody re-validates is worse than none. That is not hypothetical: **Z1 in this
same ticket was a stale 17-invariant snapshot enforced as current for months.**

So this file guards the corpus against the ways it can rot silently:

  * the eval's own parse (a stray comment after the JSON breaks the run, not the test);
  * vocabulary drift against `schemas` — if `ResearchVerdict.route`'s Literal or `DOMAIN_BUCKETS`
    change, the corpus is caught here rather than producing an unscorable run;
  * falling under the harness's own scorability floor;
  * degenerating into one class, which would make the base rate meaningless;
  * **leakage — a corpus ticket named in the worker's own prompt is not a blind row.**

That last one is the reason this file exists. MM-14457 sat in the corpus while `research.md`'s G19
rule cited its PR by number, so the worker was told about a ticket it was being graded on. I found
that by reading; this finds it by running.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ocean_pipeline import schemas

_ROOT = Path(__file__).resolve().parents[1]
_CORPUS = _ROOT / "scripts" / "eval_corpus.jsonl"
_HARNESS = _ROOT / "scripts" / "eval_routing.py"
# Every prompt file the graded worker actually receives.
_WORKER_PROMPTS = ("research.md",)


def _rows() -> list[dict]:
    """Parsed exactly the way scripts/eval_routing.py parses it — same skip rule, same json.loads."""
    out = []
    for i, ln in enumerate(_CORPUS.read_text().splitlines(), 1):
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError as e:
            pytest.fail(f"{_CORPUS.name}:{i} does not parse as JSON — the eval will crash here, "
                        f"not fail gracefully: {e}\n  {ln[:120]}")
    return out


def test_the_corpus_exists_and_parses():
    assert _CORPUS.exists(), "the ground truth is gone; every accuracy figure becomes unverifiable"
    assert _rows(), "the corpus parsed to zero rows"


def test_every_route_is_in_the_schema_vocabulary():
    """Coupled to `ResearchVerdict.route` deliberately: a label the worker can never emit is a row
    that can never be scored, and would sit there looking like coverage."""
    allowed = set(schemas.ResearchVerdict.model_fields["route"].annotation.__args__)
    for r in _rows():
        assert r.get("route") in allowed, (
            f"{r.get('ticket_id')}: route {r.get('route')!r} is not in {sorted(allowed)}")


def test_every_bucket_is_in_the_canonical_set_or_deliberately_empty():
    """"" means UNVERIFIED and route-only scoring — the corpus's own rule that an unscored row beats
    a wrong one. Anything else must be a real bucket."""
    for r in _rows():
        b = r.get("domain_bucket", "")
        assert b == "" or b in schemas.DOMAIN_BUCKETS, (
            f"{r.get('ticket_id')}: domain_bucket {b!r} is not a canonical bucket")


def test_no_duplicate_tickets():
    ids = [r["ticket_id"] for r in _rows()]
    dupes = sorted({t for t in ids if ids.count(t) > 1})
    assert not dupes, f"duplicated rows silently double-weight a ticket: {dupes}"


def test_the_corpus_still_clears_the_harness_scorability_floor():
    """The harness refuses to report below its own floor. A corpus that drifts under it produces
    runs that look executed and measure nothing."""
    floor = int(next(l.split("=")[1] for l in _HARNESS.read_text().splitlines()
                     if l.startswith("MIN_SCORABLE_ROWS")).strip())
    rows = _rows()
    assert len(rows) >= floor, (
        f"{len(rows)} rows is below the harness's MIN_SCORABLE_ROWS = {floor}; no accuracy can be "
        f"reported at this size")


def test_the_corpus_has_not_degenerated_into_one_class():
    """A lopsided corpus makes the headline meaningless — always answering the majority class would
    score well. The base rate must stay far enough below a useful result to be worth beating."""
    rows = _rows()
    counts = {r["route"]: sum(1 for x in rows if x["route"] == r["route"]) for r in rows}
    biggest = max(counts.values()) / len(rows)
    assert biggest <= 0.75, (
        f"one route class is {biggest:.0%} of the corpus ({counts}); the base rate is now so high "
        f"that the accuracy figure says little")


def test_no_corpus_TICKET_is_named_in_the_worker_prompt():
    """THE leakage guard. The eval's whole claim is that the worker classifies BLIND from title and
    description. A ticket the prompt names is not blind — the worker has been told about a ticket it
    is graded on, however incidentally.

    Found by hand once: MM-14457 was in the corpus while `research.md`'s G19 rule cited its PR
    number as a worked example. This catches the next one by running.
    """
    ids = {r["ticket_id"] for r in _rows()}
    workers = schemas.__file__ and (Path(__file__).resolve().parents[2] / "fk-aideveloper"
                                    / "skills" / "ocean-coding-agent" / "workers")
    if not workers.exists():
        pytest.skip("fk-aideveloper checkout not present")
    leaked = {}
    for name in _WORKER_PROMPTS:
        p = workers / name
        if not p.exists():
            continue
        text = p.read_text()
        for t in sorted(ids):
            if t in text:
                leaked.setdefault(t, []).append(name)
    assert not leaked, (
        f"these corpus tickets are named in the worker's own prompt, so those rows are NOT blind: "
        f"{leaked}. Either drop the row from the corpus or de-identify the rule that cites it.")


def test_the_labelling_rule_is_still_documented_in_the_header():
    """The labels are judgement calls. They are only auditable while the rule that produced them —
    and the evidence per row — travels with the file."""
    header = "\n".join(l for l in _CORPUS.read_text().splitlines() if l.lstrip().startswith("#"))
    assert "LABELLING RULE" in header, "the rule that produced these labels is gone"
    assert "RESOLUTION" in header, "the resolution-over-issue_type lesson is gone from the header"
    assert "PER-ROW EVIDENCE" in header, "the per-row evidence trail is gone"
