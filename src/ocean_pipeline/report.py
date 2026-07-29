"""Consolidated start-to-end run report, written to the artifacts dir at run end.

Independent of --verbose: whatever scrolled past on the console, the run always
leaves a shareable `run-report.md` (+ `run-report.json`) with the full timeline,
result, and spend — even if the run failed partway.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from . import config, metrics, telemetry, ui

_rows: list[dict] = []
_meta: dict = {}


def start(ticket: str, execution_id: str) -> None:
    _rows.clear()
    _meta.clear()
    now = datetime.now()
    _meta.update(ticket=ticket, execution_id=execution_id,
                 started=now.strftime("%Y-%m-%d %H:%M:%S"), _start=now)
    telemetry.reset_timings(execution_id)


def record(node: str, duration: float, update: dict) -> None:
    """One completed node: label, duration, and its outcome/facts (same data the log shows)."""
    _rows.append({
        "node": node,
        "label": ui.node_label(node),
        "seconds": round(duration, 1),
        "outcome": ui.outcome_line(node, update),
    })


def _fmt(sec: float) -> str:
    m, s = divmod(int(sec), 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


def finish(final: dict, out_dir: Path) -> Path | None:
    if not _meta:
        return None
    start_dt = _meta.get("_start") or datetime.now()
    duration = (datetime.now() - start_dt).total_seconds()
    t = metrics.totals()
    doc = {
        "ticket": _meta.get("ticket"),
        "execution_id": _meta.get("execution_id"),
        "started": _meta.get("started"),
        "finished": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": round(duration, 1),
        "final_status": final.get("final_status"),
        "final_outcome": final.get("final_outcome"),
        "pr_number": final.get("pr_number"),
        "test_automation_pr_url": final.get("test_automation_pr_url"),
        "ready_flipped": final.get("ready_flipped", False),
        "usage": {
            "input_tokens": t["input"], "output_tokens": t["output"],
            "tool_calls": t["tools"], "station_runs": t["stations"],
        },
        "timeline": list(_rows),
        # Measured per-station wall-clock (accurate even when stations run in parallel — sourced from
        # each station's own start/end, not the sequential stream-gap). name -> seconds.
        "station_seconds": telemetry.station_durations(_meta.get("execution_id") or ""),
        # which latency levers were active — so a timings.jsonl line is attributable to the right one
        # in a before/after comparison (all three toggle independently).
        "parallel_analysis": config.PARALLEL_ANALYSIS,
        "persistent_container": config.PERSISTENT_CONTAINER,
        "warm_sit_infra": config.WARM_SIT_INFRA,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "run-report.json").write_text(json.dumps(doc, indent=2), encoding="utf-8")
    md = out_dir / "run-report.md"
    md.write_text(_markdown(doc), encoding="utf-8")
    _append_timings_log(doc)
    return md


def _append_timings_log(doc: dict) -> None:
    """Persist one JSON line per run to a DURABLE path (TIMINGS_LOG), so before/after latency
    comparisons survive the /tmp artifacts cleanup. Best-effort — never breaks the run."""
    try:
        rec = {
            "ticket": doc.get("ticket"), "execution_id": doc.get("execution_id"),
            "finished": doc.get("finished"), "final_status": doc.get("final_status"),
            "parallel_analysis": doc.get("parallel_analysis"),
            "persistent_container": doc.get("persistent_container"),
            "warm_sit_infra": doc.get("warm_sit_infra"),
            "total_seconds": doc.get("duration_seconds"),
            "station_seconds": doc.get("station_seconds") or {},
        }
        config.TIMINGS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with config.TIMINGS_LOG.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")
    except OSError:
        pass


def _markdown(doc: dict) -> str:
    s = doc["usage"]
    tok = s["input_tokens"] + s["output_tokens"]
    out = [
        "# FK Ocean Pipeline — Run Report",
        "",
        f"- **Ticket:** {doc['ticket']}",
        f"- **Run:** `{doc['execution_id']}`",
        f"- **Started:** {doc['started']}  •  **Finished:** {doc['finished']}  •  "
        f"**Duration:** {_fmt(doc['duration_seconds'])}",
        # A null final_status means the run did not reach a terminal node -- almost always a PAUSE at
        # an approval gate (e.g. qa_review_gate awaiting `--qa`), not a completed run. Don't mislabel it
        # "UNKNOWN" (reads like a completed-but-unclassified run); say it's paused/incomplete and how to
        # resume. (O1: a real pause report showed "Result: UNKNOWN".)
        f"- **Result:** {str(doc['final_status']).upper() if doc.get('final_status') else 'PAUSED / INCOMPLETE — awaiting the next gate (resume with `--resume ' + str(doc.get('execution_id') or '<EXE>') + '`)'}",
    ]
    if doc.get("final_outcome"):
        out.append(f"- **Outcome:** {doc['final_outcome']}")
    if doc.get("pr_number"):
        out.append(f"- **Service PR:** #{doc['pr_number']}"
                   + (" (ready-for-review)" if doc.get("ready_flipped") else " (draft)"))
    if doc.get("test_automation_pr_url"):
        out.append(f"- **Test-automation PR:** {doc['test_automation_pr_url']}")
    out += [
        f"- **Usage:** {tok} tokens · {s['tool_calls']} tool calls · {s['station_runs']} station runs",
        "",
        "## Timeline",
        "",
        "| # | Step (node) | Duration | Outcome |",
        "|---|-------------|----------|---------|",
    ]
    for i, r in enumerate(doc["timeline"], 1):
        out.append(f"| {i} | {r['label']} (`{r['node']}`) | {_fmt(r['seconds'])} | {r['outcome'] or ''} |")
    out.append("")

    # Measured per-station timing (slowest first) — the accurate, parallelism-safe breakdown for
    # before/after latency comparisons. The Timeline above shows observed ORDER; this shows each
    # station's OWN runtime (which the sequential stream-gap can mis-attribute once stations overlap).
    stationsec = doc.get("station_seconds") or {}
    if stationsec:
        levers = (f"parallel_analysis={'on' if doc.get('parallel_analysis') else 'off'} · "
                  f"persistent_container={'on' if doc.get('persistent_container') else 'off'} · "
                  f"warm_sit_infra={'on' if doc.get('warm_sit_infra') else 'off'}")
        out += [
            f"## Station timings (measured · {levers})",
            "",
            "| Station | Own runtime |",
            "|---------|-------------|",
        ]
        for name, sec in sorted(stationsec.items(), key=lambda kv: kv[1], reverse=True):
            out.append(f"| {name} | {_fmt(sec)} |")
        # NB: with parallel_analysis on, the sum EXCEEDS wall-clock (overlapping stations) — the run's
        # real elapsed is Duration above; this sum is a per-station total, not the wall-clock.
        out += [f"| _sum of stations (not wall-clock)_ | {_fmt(sum(stationsec.values()))} |", ""]
    return "\n".join(out)
