"""Consolidated start-to-end run report, written to the artifacts dir at run end.

Independent of --verbose: whatever scrolled past on the console, the run always
leaves a shareable `run-report.md` (+ `run-report.json`) with the full timeline,
result, and spend — even if the run failed partway.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from . import metrics, ui

_rows: list[dict] = []
_meta: dict = {}


def start(ticket: str, execution_id: str) -> None:
    _rows.clear()
    _meta.clear()
    now = datetime.now()
    _meta.update(ticket=ticket, execution_id=execution_id,
                 started=now.strftime("%Y-%m-%d %H:%M:%S"), _start=now)


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
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "run-report.json").write_text(json.dumps(doc, indent=2), encoding="utf-8")
    md = out_dir / "run-report.md"
    md.write_text(_markdown(doc), encoding="utf-8")
    return md


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
    return "\n".join(out)
