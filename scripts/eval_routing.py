#!/usr/bin/env python3
"""F5 (Aquaman architecture review): routing / domain-bucket eval harness.

Replays closed tickets BLIND through the `researcher` station and scores its route +
domain_bucket against a ground-truth corpus. This is the measurement the architecture
review flags as the prerequisite for trusting F1-F7 (or ANY prompt/model change): without
a routing-accuracy baseline you cannot tell whether a change improved outcomes or just
moved them around. (ocean-rca did exactly this — 69.4% -> 98.4% — once it had the harness.)

The researcher is READ-ONLY (it queries Jira/GitHub and classifies; it never codes, opens
PRs, or mutates state), so replaying it has no side effects beyond its own artifacts dir.

Corpus format — JSONL, one object per line (``#`` lines and blanks ignored):
    {"ticket_id": "MM-14312", "route": "coding", "domain_bucket": "ocean_tracking_milestones"}
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
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from ocean_pipeline import agents, schemas  # noqa: E402


async def _predict(ticket_id: str) -> tuple[str, str]:
    """Drive ONLY the researcher for one ticket; return (route, domain_bucket).
    Any failure is surfaced as route='__error__' so one bad ticket never sinks the whole eval."""
    try:
        v: schemas.ResearchVerdict = await agents.run_agent(
            agent_md="research.md",
            node="researcher",
            ticket_id=ticket_id,
            execution_id=f"EVAL-{ticket_id}",
            task_prompt=(
                f"Research {ticket_id} and classify its route + domain_bucket ONLY. This is a "
                f"routing-accuracy EVALUATION replay — do NOT code, open PRs, or take any action; "
                f"produce the ResearchVerdict (route, domain_bucket, target_repos) and stop."
            ),
            verdict_model=schemas.ResearchVerdict,
        )
        return v.route, (v.domain_bucket or "")
    except Exception as e:  # noqa: BLE001 — record the error as a miss, keep going
        return "__error__", f"{type(e).__name__}: {e}"[:140]


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


def _score(results: list[dict]) -> dict:
    n = len(results)
    route_ok = sum(1 for x in results if x["route_ok"])
    bucket_scored = [x for x in results if x["bucket_ok"] is not None]
    bucket_ok = sum(1 for x in bucket_scored if x["bucket_ok"])
    route_misses = collections.Counter(
        (x["gt_route"], x["pred_route"]) for x in results if not x["route_ok"])
    return {
        "n": n,
        "route_accuracy": round(route_ok / n, 4) if n else None,
        "route_correct": route_ok,
        "domain_bucket_scored": len(bucket_scored),
        "domain_bucket_accuracy": round(bucket_ok / len(bucket_scored), 4) if bucket_scored else None,
        "domain_bucket_correct": bucket_ok,
        "route_confusion": [{"gt": gt, "pred": pr, "count": c} for (gt, pr), c in route_misses.items()],
    }


async def main() -> int:
    ap = argparse.ArgumentParser(description="F5 routing/domain-bucket eval harness")
    ap.add_argument("--corpus", required=True, help="JSONL ground-truth corpus")
    ap.add_argument("--limit", type=int, default=0, help="score only the first N tickets (0 = all)")
    ap.add_argument("--concurrency", type=int, default=3, help="parallel researcher replays")
    ap.add_argument("--out", default="", help="write the full JSON report (summary + per-ticket) here")
    args = ap.parse_args()

    rows = [json.loads(ln) for ln in Path(args.corpus).read_text().splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")]
    if args.limit:
        rows = rows[:args.limit]
    if not rows:
        print("corpus is empty", file=sys.stderr)
        return 2

    results = await _run(rows, args.concurrency)
    summary = _score(results)

    print(json.dumps(summary, indent=2))
    print(f"\n  route: {summary['route_correct']}/{summary['n']}"
          f"   domain_bucket: {summary['domain_bucket_correct']}/{summary['domain_bucket_scored']}\n")
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
