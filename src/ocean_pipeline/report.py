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
        # The SIT verdict itself. gan_effect.py pre-registers `automation_result == "passed"`
        # as its PRIMARY dependent variable; it was absent here, so the analyser silently
        # substituted `final_status == "completed"` — a DIFFERENT variable — without amending
        # the pre-registration. Writing the real field is the honest fix; rewriting the
        # pre-registration to match whatever was convenient is how a pre-registration stops
        # meaning anything.
        "automation_result": final.get("automation_result", ""),
        "failure_class": final.get("failure_class", ""),
        "coding_attempts": final.get("coding_attempts", 0),
        "final_outcome": final.get("final_outcome"),
        "pr_number": final.get("pr_number"),
        "test_automation_pr_url": final.get("test_automation_pr_url"),
        "ready_flipped": final.get("ready_flipped", False),
        # D5: node-evaluator.md's policy is "ADVISORY by default (logged + surfaced in the
        # run-report)" — this is the surfacing half, so an advisory evaluation is not write-only.
        # `finish` selects explicit keys rather than dumping state, so an undeclared key here is
        # invisible no matter how correctly the node computed it.
        # B1: the disk-derived instrument and both sides of the comparison. `review_branch_repos`
        # is recorded as its own key rather than reconstructed from covered+gap, which is
        # contaminated by service_repo and cannot answer "what did the disk actually show?".
        "service_repo": final.get("service_repo", ""),
        "pr_numbers": final.get("pr_numbers") or {},
        "review_branch_repos": final.get("review_branch_repos") or [],
        "review_repos_covered": final.get("review_repos_covered") or [],
        "review_coverage_gap": final.get("review_coverage_gap") or [],
        "review_coverage_unverified": final.get("review_coverage_unverified", ""),
        # gan_effect.py's ARM ASSIGNMENT and its SECONDARY variable. `qa_gan_residual_gaps` splits
        # runs into the `gaps` / `clean` arms; absent, every run landed in `unknown` and the
        # comparison had zero runs on either side. `review_findings` is the secondary outcome.
        # NB `qa_gan_residual_gaps` has no `or []`: None (never recorded) must stay distinct from
        # [] (recorded, clean).
        "qa_gan_residual_gaps": final.get("qa_gan_residual_gaps"),
        "review_findings": final.get("review_findings") or [],
        # The credential gate's own could-not-measure signal. `flip_ready` fails OPEN when gitleaks
        # is missing and says the reason is "carried to the terminal so it can never read as
        # 'scanned and clean'" — but the terminal scrolls away and this file is what survives. Absent
        # here, a run that scanned NOTHING and a run that scanned clean produced byte-identical
        # reports, which is the same collapse `review_coverage_unverified` and `eval_unverified`
        # two lines up exist to prevent.
        "secret_scan_unverified": final.get("secret_scan_unverified", ""),
        "secret_findings": final.get("secret_findings") or [],
        "node_evaluations": final.get("node_evaluations") or [],
        # Kept separate from the list above so "the judge could not be run / returned nothing
        # readable" can never be read as "no evaluation found anything wrong".
        "eval_unverified": final.get("eval_unverified", ""),
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
