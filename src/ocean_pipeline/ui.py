"""Runner log — three audiences, one run (see config.LOG_LEVEL):

  management — top station headers + one-line outcome only.
  team       — (default) headers + outcome + curated milestones/detail lines.
  developer  — team, plus the raw per-agent tool activity (agents._drive's firehose).
"""
from __future__ import annotations

from datetime import datetime

from . import config, metrics

WIDTH = 64
_LEVEL_ORDER = {"management": 0, "team": 1, "developer": 2}


def _at_least(min_level: str) -> bool:
    return _LEVEL_ORDER.get(config.LOG_LEVEL, 1) >= _LEVEL_ORDER[min_level]


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")

# internal node id -> plain-English station label
_LABELS = {
    "researcher":         "Research & routing",
    "sme_consult":        "Ocean SME consult",
    "rca_agent":          "Root-cause analysis (RCA)",
    "rca_review_gate":    "RCA review — awaiting approval",
    "rca_done":           "RCA report delivered",
    "unsupported_route":  "Unsupported ticket — stopped",
    "dep_resolver":       "Dependency resolution",
    "reachability_gate":  "Reachability verification",
    "coder":              "Coding",
    "harsh_reviewer":     "Adversarial code review",
    "open_pr":            "Open draft PR",
    "graph_augment":      "Code-graph augmentation",
    "sit_resolve":        "Local SIT — resolve & gate",
    "sit_author":         "Local SIT — draft test",
    "qa_review_gate":     "QA review — awaiting approval",
    "sit_run":            "Local SIT — run",
    "sit_testrail":       "TestRail — writing cases",
    "sit_triage":         "Local SIT — triage & verdict",
    "learn_repo":         "Onboarding an unsupported repo",
    "prep_rework":        "Rework — SIT found a defect",
    "human_gate":         "Awaiting human approval",
    "flip_ready":         "Flip PR to ready-for-review",
    "stop_run":           "Stopped — needs an engineer",
}
_STOP_NODES = {"stop_run", "unsupported_route"}


def station_start(node: str) -> None:
    """Header printed when a node's agent begins; milestones stream under it.
    Keyed on the LangGraph node name — the same label map as the completion line."""
    print(f"\n▶  {_now()}  {_LABELS.get(node, node)}", flush=True)


def milestone(text: str) -> None:
    """One curated, human-readable action inside a running node.
    Suppressed at management level — that tier gets headers + outcome only."""
    if not _at_least("team"):
        return
    print(f"     · {text}", flush=True)


def node_label(node: str) -> str:
    return _LABELS.get(node, node)


def outcome_line(node: str, update: dict) -> str:
    """Highlight + detail facts for a node, flattened to one line (for the report table)."""
    parts = []
    hi = _highlight(node, update)
    if hi:
        parts.append(hi)
    parts += [d.strip() for d in _details(node, update)]
    return "; ".join(p for p in parts if p)


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
    elif node == "sit_triage":
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
        body = (notes[:48] + "…") if len(notes) > 49 else (notes or "no blockers")
        return f"BLOCKING: {body}" if upd.get("dependency_blocking") else body
    if node == "reachability_gate":
        return "BLOCKING claim found" if upd.get("reachability_blocking") else "all claims verified"
    if node == "rca_agent":
        return "code fix needed" if upd.get("rca_fix_needed") else "no code fix"
    if node == "rca_review_gate" and upd.get("rca_approval_decision"):
        return str(upd["rca_approval_decision"])
    if node == "coder":
        return "branch pushed"
    if node == "harsh_reviewer":
        return str(upd.get("review_verdict") or "")
    if node == "open_pr" and upd.get("pr_number"):
        return f"PR #{upd['pr_number']}"
    if node == "sit_resolve":
        return f"needs onboarding: {upd.get('onboard_repo', '')}" if upd.get("needs_onboarding") else "repo resolved"
    if node == "sit_author":
        return "SIT drafted — awaiting review"
    if node == "qa_review_gate" and upd.get("qa_decision"):
        return upd["qa_decision"].replace("_", " ")
    if node == "sit_testrail":
        return f"TestRail run {upd['testrail_run_id']}" if upd.get("testrail_run_id") else "TestRail cases written"
    if node == "sit_triage" and upd.get("automation_result"):
        fc = upd.get("failure_class")
        return f"SIT {upd['automation_result']}" + (f" ({fc})" if fc else "")
    if node == "learn_repo":
        return f"onboarded {upd.get('repo_onboarded', '')}".strip()
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
    """The station's completion line (sub-heading): icon, label, elapsed, one-line
    outcome. Always printed, at every log level. Detail bullets underneath are the
    team-level-and-up "additional lines" — management stops at this one line."""
    icon = "✗" if node in _STOP_NODES else "✓"
    label = _LABELS.get(node, node)
    line = f"  {icon}  {label:<38}{_fmt_elapsed(elapsed):>7}"
    hi = _highlight(node, upd)
    if hi:
        line += f"   {hi}"
    print(line, flush=True)
    if _at_least("team"):
        for det in _details(node, upd):
            print(f"        └ {det}", flush=True)


def summary(final: dict, total: float) -> None:
    status = (final.get("final_status") or "unknown").upper()
    print("─" * WIDTH)
    print(f"  RESULT: {status}   ·   took {_fmt_elapsed(total)}   ·   finished {_now()}")
    if final.get("final_outcome"):
        print(f"  {final['final_outcome']}")
    t = metrics.totals()
    usage = metrics.fmt(t["input"], t["output"], t["tools"])
    if usage:
        print(f"  usage: {usage}   across {t['stations']} station runs")
    print("═" * WIDTH + "\n", flush=True)
