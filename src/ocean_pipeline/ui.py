"""Runner log — a clean, management-legible view of a pipeline run.

This is the DEFAULT console surface: one line per station with a plain-English
name, elapsed time, and outcome — readable by someone who isn't an engineer.
Engineers add --verbose for the raw per-agent tool activity underneath.
"""
from __future__ import annotations

from datetime import datetime

from . import metrics

WIDTH = 64


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")

# internal node id -> plain-English station label
_LABELS = {
    "researcher":         "Research & routing",
    "rca_agent":          "Root-cause analysis (RCA)",
    "rca_done":           "RCA report delivered",
    "unsupported_route":  "Unsupported ticket — stopped",
    "dep_resolver":       "Dependency resolution",
    "reachability_gate":  "Reachability verification",
    "coder":              "Coding",
    "harsh_reviewer":     "Adversarial code review",
    "open_pr":            "Open draft PR",
    "graph_augment":      "Code-graph augmentation",
    "release_intel":      "Release intelligence",
    "automation_testing": "Local SIT (automation testing)",
    "prep_rework":        "Rework — SIT found a defect",
    "flip_ready":         "Flip PR to ready-for-review",
    "stop_run":           "Stopped — needs an engineer",
}
_STOP_NODES = {"stop_run", "unsupported_route"}

# station-string (as passed to the agent runner) -> plain-English label, for the
# live "▶ started" header that brackets the curated milestones streamed underneath.
_STATION_LABELS = {
    "station0-researcher":        "Research & routing",
    "rca-agent":                  "Root-cause analysis (RCA)",
    "rca":                        "RCA report",
    "station1-deps":              "Dependency resolution",
    "station1_5-reachability":    "Reachability verification",
    "station4-coder":             "Coding",
    "station5-review":            "Adversarial code review",
    "station3_87-open-pr":        "Open draft PR",
    "station4_5b-graph-augment":  "Code-graph augmentation",
    "station4_6-release-intel":   "Release intelligence",
    "station6-author":            "SIT: author/locate test",
    "station6-run":               "SIT: run locally",
    "station6-report":            "SIT: verdict",
    "station6-automation-testing":"Local SIT (automation testing)",
    "station6_5-ready-flip":      "Flip PR to ready-for-review",
}


def station_label(station: str) -> str:
    return _STATION_LABELS.get(station, station)


def station_start(station: str) -> None:
    """Header printed when a station's agent begins; milestones stream under it."""
    print(f"\n▶  {_now()}  {station_label(station)}", flush=True)


def milestone(text: str) -> None:
    """One curated, human-readable action inside a running station."""
    print(f"     · {text}", flush=True)


def _fmt_elapsed(sec: float) -> str:
    m, s = divmod(int(sec), 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


def _details(node: str, upd: dict) -> list[str]:
    """A few important sub-steps per station, from the structured data it returned.
    Kept short (a handful of lines) — informative, not the --verbose firehose."""
    if not isinstance(upd, dict):
        return []
    d: list[str] = []
    if node == "researcher":
        pkt = upd.get("research_packet") or {}
        if isinstance(pkt, dict) and pkt.get("domain_bucket"):
            d.append(f"domain: {pkt['domain_bucket']}")
        for r in (upd.get("target_repos") or [])[:4]:
            if isinstance(r, dict) and r.get("repo"):
                env = f", {r['build_env']}" if r.get("build_env") else ""
                d.append(f"repo: {r['repo']} ({r.get('language', '?')}{env})")
    elif node == "rca_agent":
        n = len(upd.get("rca_findings") or [])
        if n:
            d.append(f"{n} fix item(s) identified for the coder")
    elif node == "coder":
        fc = upd.get("files_changed")
        d.append(f"{fc} file(s) changed" if fc else "changes committed")
        if upd.get("branch"):
            d.append(f"pushed branch {upd['branch']}")
    elif node == "harsh_reviewer":
        findings = upd.get("review_findings") or []
        if findings:
            counts: dict[str, int] = {}
            for f in findings:
                sev = (f.get("severity") if isinstance(f, dict) else None) or "OTHER"
                counts[sev] = counts.get(sev, 0) + 1
            d.append("findings: " + ", ".join(f"{v} {k}" for k, v in counts.items()))
            for f in findings[:3]:
                if isinstance(f, dict):
                    where = f.get("file") or ""
                    what = f.get("summary") or f.get("short_summary") or ""
                    d.append(f"  {f.get('severity', '?')}: {what}{f' ({where})' if where else ''}".rstrip())
            if len(findings) > 3:
                d.append(f"  … +{len(findings) - 3} more")
        else:
            d.append("no CRITICAL/MAJOR findings")
    elif node == "automation_testing":
        rep = upd.get("sit_report") or {}
        tests = rep.get("tests") or []
        if tests:
            passed = sum(1 for t in tests if isinstance(t, dict) and t.get("result") == "passed")
            d.append(f"{passed}/{len(tests)} tests passed")
        cr = rep.get("changed_repo") or {}
        if isinstance(cr, dict) and cr.get("repo"):
            d.append(f"ran {cr['repo']} {cr.get('ran_on', '')}".strip())
        for t in tests[:5]:
            if isinstance(t, dict) and t.get("name"):
                d.append(f"  {t.get('result', '?')}: {t['name']}")
        mocked = [x.get("repo") for x in (rep.get("dependencies") or [])
                  if isinstance(x, dict) and x.get("ran_on") == "mocked" and x.get("repo")]
        if mocked:
            d.append(f"mocked: {', '.join(mocked[:4])}")
        for f in (upd.get("sit_findings") or [])[:3]:
            if isinstance(f, dict):
                d.append(f"defect: {f.get('cause') or f.get('test') or f}")
    elif node == "flip_ready":
        if upd.get("test_automation_pr_url"):
            d.append(f"test PR: {upd['test_automation_pr_url']}")
    return d


def _highlight(node: str, upd: dict) -> str:
    if not isinstance(upd, dict):
        return ""
    if node == "researcher":
        return f"→ routed to {upd.get('route', '')}"
    if node == "dep_resolver":
        notes = (upd.get("dependency_report") or {}).get("notes") or ""
        return (notes[:48] + "…") if len(notes) > 49 else (notes or "no blockers")
    if node == "reachability_gate":
        return "BLOCKING claim found" if upd.get("reachability_blocking") else "all claims verified"
    if node == "rca_agent":
        return "code fix needed" if upd.get("rca_fix_needed") else "no code fix"
    if node == "coder":
        return "branch pushed"
    if node == "harsh_reviewer":
        return str(upd.get("review_verdict") or "")
    if node == "open_pr" and upd.get("pr_number"):
        return f"PR #{upd['pr_number']}"
    if node == "automation_testing" and upd.get("automation_result"):
        fc = upd.get("failure_class")
        return f"SIT {upd['automation_result']}" + (f" ({fc})" if fc else "")
    if node == "flip_ready" and upd.get("ready_flipped"):
        return "ready-for-review"
    if node == "prep_rework":
        return "looping back to Coding"
    return ""


def banner(ticket: str, execution_id: str) -> None:
    print("\n" + "═" * WIDTH)
    print(f"  FK Ocean Pipeline   ·   {ticket}")
    print(f"  run {execution_id}   ·   started {_now()}")
    print("═" * WIDTH, flush=True)


def step(node: str, upd: dict, elapsed: float) -> None:
    icon = "✗" if node in _STOP_NODES else "✓"
    label = _LABELS.get(node, node)
    line = f"  {icon}  {label:<38}{_fmt_elapsed(elapsed):>7}"
    hi = _highlight(node, upd)
    if hi:
        line += f"   {hi}"
    print(line, flush=True)
    for det in _details(node, upd):
        print(f"        └ {det}", flush=True)


def summary(final: dict, total: float) -> None:
    status = (final.get("final_status") or "unknown").upper()
    print("─" * WIDTH)
    print(f"  RESULT: {status}   ·   took {_fmt_elapsed(total)}   ·   finished {_now()}")
    if final.get("final_outcome"):
        print(f"  {final['final_outcome']}")
    t = metrics.totals()
    spend = metrics.fmt(t["cost"], t["input"], t["output"], t["tools"])
    if spend:
        print(f"  spend: {spend}   across {t['stations']} station runs")
    print("═" * WIDTH + "\n", flush=True)
