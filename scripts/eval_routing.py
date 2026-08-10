#!/usr/bin/env python3
"""F5 (Aquaman architecture review): routing / domain-bucket eval harness.

Scores the `researcher` station's route + domain_bucket against a ground-truth corpus, replaying
each ticket from its TITLE AND DESCRIPTION ONLY. This is the measurement the architecture review
flags as the prerequisite for trusting F1-F7 (or ANY prompt/model change): without a routing-accuracy
baseline you cannot tell whether a change improved outcomes or just moved them around. (ocean-rca did
exactly this — 69.4% -> 98.4% — once it had the harness, and it scored on title+description too.)

WHAT "BLIND" MEANS HERE, and what it did not mean before. This header used to claim it "replays
CLOSED tickets BLIND". Both halves were false, and a review demonstrated it:
  * NOT closed — only 2 of the 9 corpus tickets are closed, and they are exactly the two `rca` rows;
    every `coding` row is Open or In-Dev.
  * NOT blind — it drove the full live researcher against a bare ticket id, and research.md MANDATES
    reading the comment thread (G8), `gh pr list --search "<TICKET>"` per repo (G19), and linked
    tickets (G21). 7/7 coding rows have open PRs naming repo and domain; 0/2 rca rows do — so PR
    existence alone was a near-perfect route oracle. Several of those PRs were authored by THIS
    pipeline, so it was grading a researcher on tickets the system had already solved.
Blindness is now enforced structurally, not requested: `jira.issue_text` fetches only
`fields=summary,description` (server-side), and the worker runs with `allowed_tools=["Write"]` so the
mandated lookups are not available to perform. See `_predict` for the full reasoning.

The researcher classifies and never codes, opens PRs, or mutates state, so replaying it has no side
effects beyond its own artifacts dir.

Corpus format — JSONL, one object per line (``#`` lines and blanks ignored). The illustration uses
a FICTITIOUS ticket id on purpose: the standing rule from this eval's own contamination incident is
that illustrations must come from outside the corpus, and the previous example was a real corpus row
with its answer key attached. A docstring is not sent to the graded worker, so this was not live
contamination — but "the leak is harmless here" is the reasoning that produced the 31/31 run, and a
rule with an exception is one nobody applies.
    {"ticket_id": "MM-00000", "route": "coding", "domain_bucket": "ocean_tracking_milestones"}
`route` is required and always scored. `domain_bucket` is scored ONLY for coding tickets that
supply a non-empty ground-truth bucket (rca/sop/etc. have no bucket; unverified buckets are
left "" and skipped rather than scored against a guess).

Run:
    python scripts/eval_routing.py --corpus scripts/eval_corpus.jsonl [--limit N] \
        [--concurrency 3] [--out report.json]

Each ticket runs under a fresh EVAL-<ticket> execution_id. Requires the claude Agent SDK/CLI
(same as a real run) since it drives the actual researcher worker — this is a deliberate
end-to-end replay, not a mock.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import datetime as dt
import hashlib
import json
import secrets
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from ocean_pipeline import agents, jira, schemas  # noqa: E402

ERROR_ROUTE = "__error__"


async def _predict(ticket_id: str) -> tuple[str, str]:
    """Classify ONE ticket from its title + description ONLY; return (route, domain_bucket).
    Any failure is surfaced as route='__error__' so one bad ticket never sinks the whole eval.

    BLINDNESS — the reason this does not just pass a ticket id. A review established that the earlier
    version was not blind at all: it drove the full live researcher against a bare id, and
    research.md MANDATES reading the ticket's comment thread (G8), running
    `gh pr list --search "<TICKET>"` on every target repo (G19), and reading linked tickets (G21).
    Measured over this corpus, 7/7 `coding` rows have open PRs whose titles name the repo and the
    domain; 0/2 `rca` rows have any. "Does a PR exist for this ticket?" was therefore a near-perfect
    route oracle, one MANDATORY command away — and several of those PRs were authored by this very
    pipeline, so the eval was grading a researcher on tickets the same system had already solved.

    Two mechanisms make it blind, and neither is a request to behave:
      1. the INPUT is the scrubbed title+description fetched by `jira.issue_text` (server-side
         `fields=summary,description`), not a live id to look up;
      2. the TOOLS are reduced to Write only, so the mandated `gh`/comment/linked-ticket lookups are
         not available to perform. A prompt asking the worker to skip them would be
         prompt-versus-prompt against its own MANDATORY rules; removing the tools actually holds.
    This also restores the precedent this script's own header cites — ocean-rca's 69.4% -> 98.4%
    benchmark was explicitly title-plus-description-only."""
    summary, description = jira.issue_text(ticket_id)
    if not summary and not description:
        # No text = nothing to classify blind. Report it rather than silently falling back to a live
        # lookup, which is exactly the leak this function exists to close.
        return ERROR_ROUTE, "could not fetch title/description (JIRA_API_TOKEN set?)"
    try:
        v: schemas.ResearchVerdict = await agents.run_agent(
            agent_md="research.md",
            node="researcher",
            ticket_id=ticket_id,
            execution_id=f"EVAL-{ticket_id}",
            task_prompt=(
                f"Classify the route + domain_bucket for the ocean ticket below.\n\n"
                f"This is a BLIND routing-accuracy evaluation. The ticket's title and description are "
                f"the ONLY inputs — you have no tools for looking anything else up, by design. Do not "
                f"attempt to read its comments, its linked tickets, or any repo/PR/branch: a real "
                f"run's resolution context would tell you the answer instead of testing whether you "
                f"can derive it. Classify from the text, then write the ResearchVerdict "
                f"(route, domain_bucket, target_repos) and stop. Leave target_repos empty if the text "
                f"does not name a repo; do not guess one to look complete.\n\n"
                f"--- TITLE ---\n{summary}\n\n--- DESCRIPTION ---\n{description[:12000]}"
            ),
            verdict_model=schemas.ResearchVerdict,
            allowed_tools=["Write"],
        )
        return v.route, (v.domain_bucket or "")
    except Exception as e:  # noqa: BLE001 — record the error as a miss, keep going
        return ERROR_ROUTE, f"{type(e).__name__}: {e}"[:140]


async def _run(rows: list[dict], concurrency: int) -> list[dict]:
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(r: dict) -> dict:
        async with sem:
            pred_route, pred_bucket = await _predict(r["ticket_id"])
        gt_bucket = r.get("domain_bucket", "") or ""
        # bucket is scored only for coding tickets that carry a verified ground-truth bucket
        bucket_ok = (pred_bucket == gt_bucket) if (r["route"] == "coding" and gt_bucket) else None
        return {
            "ticket_id": r["ticket_id"],
            "gt_route": r["route"], "pred_route": pred_route, "route_ok": pred_route == r["route"],
            "gt_bucket": gt_bucket, "pred_bucket": pred_bucket, "bucket_ok": bucket_ok,
        }

    return await asyncio.gather(*(one(r) for r in rows))


# Below this many scored rows, an accuracy figure is theatre. The corpus is NINE rows today, so one
# ticket moves the number 11 points — wider than most differences anyone would act on. The eval still
# runs and still prints per-ticket results; it just refuses to publish a headline percentage it
# cannot support. See MM-14816-accuracy-plan.md.
MIN_SCORABLE_ROWS = 20


def _score(results: list[dict]) -> dict:
    """Accuracy over the rows that were actually MEASURED — errors are excluded, not counted wrong.

    `_predict` returns `__error__` for any failure: an unset JIRA_API_TOKEN, a network blip, a
    malformed verdict. Scoring that as a wrong answer meant a machine with no Jira credentials
    reported `route_accuracy: 0.0` — indistinguishable from a router that gets every ticket wrong,
    from the one instrument every severity claim in the queue is supposed to be checked against.
    "Could not measure" and "measured, and it failed" are different facts and must not share a
    number. Same distinction the pipeline draws between could_not_verify and a real failure."""
    n = len(results)
    errored = [x for x in results if x["pred_route"] == ERROR_ROUTE]
    scored = [x for x in results if x["pred_route"] != ERROR_ROUTE]
    route_ok = sum(1 for x in scored if x["route_ok"])
    # A bucket is scorable only if its route prediction was, too.
    bucket_scored = [x for x in scored if x["bucket_ok"] is not None]
    bucket_ok = sum(1 for x in bucket_scored if x["bucket_ok"])
    route_misses = collections.Counter(
        (x["gt_route"], x["pred_route"]) for x in scored if not x["route_ok"])

    enough = len(scored) >= MIN_SCORABLE_ROWS
    return {
        "n": n,
        "scored": len(scored),
        "errored": len(errored),
        "error_ticket_ids": [x["ticket_id"] for x in errored],
        # None, never 0.0, when there is nothing to stand on — an absent number is honest, a wrong
        # one is not. `underpowered` says WHY it is absent when rows were scored but too few.
        "route_accuracy": (round(route_ok / len(scored), 4) if scored and enough else None),
        "route_correct": route_ok,
        "underpowered": (not enough),
        "min_scorable_rows": MIN_SCORABLE_ROWS,
        "domain_bucket_scored": len(bucket_scored),
        "domain_bucket_accuracy": (round(bucket_ok / len(bucket_scored), 4)
                                   if bucket_scored and enough else None),
        "domain_bucket_correct": bucket_ok,
        "route_confusion": [{"gt": gt, "pred": pr, "count": c} for (gt, pr), c in route_misses.items()],
    }


def _provenance(corpus: str) -> dict:
    """What this number is OF — so a report can be re-derived instead of merely believed.

    A bare accuracy with no run id, timestamp, corpus or code version cannot be reproduced or
    compared against a later run, which makes it an assertion rather than a measurement."""
    def _git(*args: str) -> str:
        try:
            p = subprocess.run(["git", *args], capture_output=True, text=True, timeout=10,
                               cwd=Path(__file__).resolve().parent)
            return p.stdout.strip() if p.returncode == 0 else ""
        except Exception:  # noqa: BLE001 — provenance is best-effort; never fail the eval for it
            return ""

    corpus_path = Path(corpus)
    try:
        digest = hashlib.sha256(corpus_path.read_bytes()).hexdigest()[:16]
    except OSError:
        digest = ""
    return {
        "run_id": f"EVAL-{secrets.token_hex(4)}",
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "corpus_path": str(corpus_path.resolve()) if corpus_path.exists() else corpus,
        "corpus_sha256_16": digest,
        "git_sha": _git("rev-parse", "--short", "HEAD"),
        "git_dirty": bool(_git("status", "--porcelain")),
    }


async def main() -> int:
    ap = argparse.ArgumentParser(description="F5 routing/domain-bucket eval harness")
    ap.add_argument("--corpus", required=True, help="JSONL ground-truth corpus")
    ap.add_argument("--limit", type=int, default=0, help="score only the first N tickets (0 = all)")
    ap.add_argument("--concurrency", type=int, default=3, help="parallel researcher replays")
    ap.add_argument("--out", default="", help="write the full JSON report (summary + per-ticket) here")
    args = ap.parse_args()

    # Per-row parse guard. `_predict` is carefully hardened so one bad TICKET never sinks the eval,
    # but this loader was a bare comprehension 47 lines below it — one malformed JSONL row raised
    # JSONDecodeError and killed the whole run before a single ticket was scored (reproduced in
    # review). A bad row is skipped loudly and counted, never silently dropped.
    rows, bad_rows = [], []
    for n, ln in enumerate(Path(args.corpus).read_text().splitlines(), start=1):
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        try:
            row = json.loads(ln)
        except json.JSONDecodeError as e:
            bad_rows.append((n, str(e)))
            continue
        if not isinstance(row, dict) or not row.get("ticket_id"):
            bad_rows.append((n, "row is not an object with a ticket_id"))
            continue
        rows.append(row)
    for n, why in bad_rows:
        print(f"corpus line {n}: SKIPPED — {why}", file=sys.stderr)
    if bad_rows:
        print(f"corpus: {len(bad_rows)} unparseable row(s) skipped, {len(rows)} scored — the accuracy "
              f"below is over the {len(rows)} rows that parsed, NOT the whole corpus", file=sys.stderr)
    if args.limit:
        rows = rows[:args.limit]
    if not rows:
        print("corpus is empty", file=sys.stderr)
        return 2

    provenance = _provenance(args.corpus)
    results = await _run(rows, args.concurrency)
    summary = {**_score(results), "provenance": provenance}

    print(json.dumps(summary, indent=2))
    # Report over what was MEASURED, and say so when there is no headline to report.
    print(f"\n  route: {summary['route_correct']}/{summary['scored']} scored"
          f"   domain_bucket: {summary['domain_bucket_correct']}/{summary['domain_bucket_scored']}")
    if summary["errored"]:
        print(f"  !! {summary['errored']} ticket(s) COULD NOT BE MEASURED and are excluded from the "
              f"accuracy (not counted wrong): {', '.join(summary['error_ticket_ids'][:6])}"
              + ("…" if summary["errored"] > 6 else ""))
    if summary["underpowered"]:
        print(f"  !! NO ACCURACY REPORTED — {summary['scored']} scored row(s) is below the "
              f"{summary['min_scorable_rows']}-row floor; one ticket would move it "
              f"{round(100 / max(summary['scored'], 1))} points. Per-ticket results below still stand.")
    print()
    for x in sorted(results, key=lambda r: (r["route_ok"], r["ticket_id"])):
        flag = "OK" if x["route_ok"] else "XX"
        bflag = {True: "OK", False: "XX", None: "--"}[x["bucket_ok"]]
        print(f"  [{flag}] {x['ticket_id']:12} route {x['gt_route']:12} -> {x['pred_route']:12}"
              f"  [{bflag}] bucket {x['gt_bucket']:26} -> {x['pred_bucket']}")

    if args.out:
        Path(args.out).write_text(json.dumps({"summary": summary, "results": results}, indent=2))
        print(f"\n  wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
