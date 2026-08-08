"""Runner log — three audiences, one run (see config.LOG_LEVEL), each a strict superset
of the one below it:

  management — the run banner + each station's "▶ header" line only. No outcome, no
               milestones, no detail bullets — just "what's running right now."
  team       — (default) management, PLUS one outcome line per station (icon, elapsed,
               highlight) — exactly one line per process, nothing streamed underneath.
  developer  — team, PLUS the curated milestones streamed during each station, its
               detail bullets, and the raw per-agent tool activity (agents._drive's
               firehose) — this is the "give me everything" tier.
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
    "blocked_review_gate": "Open questions — awaiting approval",
    "qa_scenarios":       "GAN-hardened test scenarios",
    "coder":              "Coding",
    "quality_gate":       "Static quality gate",
    "harsh_reviewer":     "Adversarial code review",
    "open_pr":            "Open draft PR",
    "sit_resolve":        "Local SIT — resolve & gate",
    "sit_author":         "Local SIT — draft test",
    "qa_review_gate":     "QA review — awaiting approval",
    "sit_run":            "Local SIT — run",
    "sit_testrail":       "TestRail — writing cases",
    "prep_image":         "Pre-warming the Ruby image",
    "prep_container":     "Starting the shared test container",
    "teardown_container": "Removing the shared test container",
    "sit_triage":         "Local SIT — triage & verdict",
    "learn_repo":         "Onboarding an unsupported repo",
    "prep_rework":        "Rework — SIT found a defect",
    "prep_env_retry":     "Retry — SIT hit an environment issue",
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
    """One curated, human-readable action inside a running node. Developer-only: team
    level is capped at exactly one line per process (the step() outcome line) — the
    streamed "what's happening right now" detail belongs to the "give me everything" tier."""
    if not _at_least("developer"):
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
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _safe_rung(upd: dict) -> int:
    """This display layer must never crash a run over a malformed value -- unlike
    schemas.AutomationVerdict's own `_coerce_fidelity_rung` (which every real verdict goes through),
    _details/_highlight read the raw state dict directly and can't assume it always passed through
    that validation (a future code path, a hand-built state, or test code could hand this something
    else). Judge review reproduced a live crash (`'<' not supported between instances of 'str' and
    'int'`) on a non-int fidelity_rung before this guard existed. Falls back to 0 (the safe/most-
    suspicious default) on anything that doesn't cleanly coerce, mirroring the schema validator's own
    fail-safe direction."""
    try:
        return int(upd.get("fidelity_rung", 0))
    except (TypeError, ValueError):
        return 0


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
    elif node == "qa_scenarios":
        # D1 Step 3 (bullet 3). This station had NO branch here at all, so the runner printed a bare
        # "GAN-hardened test scenarios  12m" and the report's Outcome cell was empty -- ~29 minutes
        # of adversarial work with no visible result anywhere. Cheapest real surface there is.
        verdict = upd.get("qa_gan_verdict") or ""
        gaps = upd.get("qa_gan_residual_gaps") or []
        if verdict:
            d.append(f"GAN verdict: {verdict}")
        if upd.get("qa_gan_stop_reason"):
            d.append(f"stopped: {upd['qa_gan_stop_reason']}")
        for g in gaps[:3]:
            if isinstance(g, dict):
                d.append(f"  HIGH: {str(g.get('summary', ''))[:110]}")
        if len(gaps) > 3:
            d.append(f"  … +{len(gaps) - 3} more")
    elif node == "coder":
        fc = upd.get("files_changed")
        d.append(f"{fc} file(s) changed" if fc else "changes committed")
        if upd.get("branch"):
            d.append(f"pushed branch {upd['branch']}")
        # D5: the accuracy evaluation rides on the coder's own update, because `_eval_node` runs
        # inside that station rather than as a node of its own (monitor/app.py:411-425 depends on it
        # NOT being a node). Advisory: shown, never styled as a failure unless it actually gated.
        for e in (upd.get("node_evaluations") or []):
            if not isinstance(e, dict) or e.get("node") != "coder":
                continue
            acc = e.get("accuracy")
            d.append(f"  accuracy eval: {e.get('verdict') or 'UNREADABLE'} "
                     f"({'not reported' if acc is None else acc}/100)")
            for issue in (e.get("issues") or [])[:3]:
                d.append(f"    - {str(issue)[:120]}")
        if upd.get("eval_unverified"):
            d.append(f"  accuracy NOT measured: {str(upd['eval_unverified'])[:120]}")
    elif node == "quality_gate":
        checked = upd.get("quality_gate_checked_files")
        if checked is not None:
            d.append(f"{checked} changed file(s) checked")
        for f in (upd.get("quality_gate_findings") or [])[:3]:
            if isinstance(f, dict):
                where = f.get("file") or ""
                line = f":{f['line']}" if f.get("line") else ""
                d.append(f"  {f.get('severity', '?')}: {f.get('summary', '')}"
                         f"{f' ({where}{line})' if where else ''}".rstrip())
        extra = len(upd.get("quality_gate_findings") or []) - 3
        if extra > 0:
            d.append(f"  … +{extra} more")
        if upd.get("quality_gate_unverified"):
            d.append(f"  NOT fully checked: {str(upd['quality_gate_unverified'])[:120]}")
    elif node == "harsh_reviewer":
        # B1 first: a coverage gap outranks the finding counts, because it means a repo was never
        # read AT ALL — "0 findings" on an unreviewed repo is not good news.
        if upd.get("review_coverage_gap"):
            d.append(f"COVERAGE GAP: reviewed {upd.get('review_repos_covered')}, "
                     f"but {upd['review_coverage_gap']} also carry this branch")
        elif upd.get("review_branch_repos"):
            d.append(f"{len(upd['review_branch_repos'])} repo(s) carry this branch; "
                     f"reviewed {upd.get('review_repos_covered')}")
        if upd.get("review_coverage_unverified"):
            d.append(f"  coverage NOT derived: {str(upd['review_coverage_unverified'])[:120]}")
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
        if upd.get("automation_result") == "passed":
            rung = _safe_rung(upd)
            if rung < 2:
                d.append(f"fidelity: Rung {rung}" + (" — UNVERIFIED, no real signal" if rung == 0 else " — partial signal"))
        unmocked = rep.get("unmocked_paths_hit") or []
        if unmocked:
            d.append(f"unmocked: {', '.join(unmocked[:4])}" + (f" (+{len(unmocked) - 4} more)" if len(unmocked) > 4 else ""))
        crs = rep.get("changed_repos") or []
        ran = [f"{c['repo']} {c.get('ran_on', '')}".strip()
               for c in crs if isinstance(c, dict) and c.get("repo")]
        if ran:
            d.append("ran " + ", ".join(ran))
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
    if node == "quality_gate":
        from . import schemas as _s
        blocking = [f for f in (upd.get("quality_gate_findings") or []) if _s.is_blocking_finding(f)]
        checked = upd.get("quality_gate_checked_files") or 0
        if blocking:
            return f"{len(blocking)} blocking issue(s) — back to Coding"
        if upd.get("quality_gate_unverified"):
            return f"DID NOT fully run — {checked} file(s) checked"
        return f"{checked} file(s) clean"
    if node == "qa_scenarios":
        gaps = upd.get("qa_gan_residual_gaps") or []
        verdict = upd.get("qa_gan_verdict") or "?"
        # Gated on the GAPS, not the verdict -- same reason as the PR-body section in open_pr.
        if gaps:
            return f"{verdict} — {len(gaps)} residual HIGH gap(s)"
        return f"{verdict} — converged" if verdict else "scenarios hardened"
    if node == "coder":
        # Only ever mention the evaluation when it actually ran -- with NODE_EVAL off (the default)
        # this is unchanged from before D5.
        ev = next((e for e in (upd.get("node_evaluations") or [])
                   if isinstance(e, dict) and e.get("node") == "coder"), None)
        if upd.get("eval_stopped"):
            return "accuracy eval FAILED — stopping"
        if upd.get("eval_gap"):
            return "accuracy eval FAILED — back to Coding"
        if ev:
            acc = ev.get("accuracy")
            return (f"branch pushed; accuracy {ev.get('verdict') or 'UNREADABLE'}"
                    f"{'' if acc is None else f' ({acc}/100)'}")
        return "branch pushed"
    if node == "harsh_reviewer" and upd.get("review_coverage_stopped"):
        return "review coverage gap — stopping"
    if node == "harsh_reviewer" and upd.get("review_coverage_gap"):
        return f"{len(upd['review_coverage_gap'])} repo(s) unreviewed — back to Coding"
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
        cm = upd.get("qa_testrail_case_map")
        if isinstance(cm, dict) and cm:
            ids = ", ".join(f"TC-{v}" for v in cm.values())
            return f"TestRail: {len(cm)} case(s) — {ids}"
        return "TestRail cases written"
    if node == "sit_triage" and upd.get("automation_result"):
        fc = upd.get("failure_class")
        line = f"SIT {upd['automation_result']}" + (f" ({fc})" if fc else "")
        # Finding 2c (whole-diff judge review): a "passed" alone reads identically whether the SIT
        # ran at full fidelity or never really got exercised (Rung 0) -- surface the rung right in
        # the console/run-report line, the cheapest, most-visible place a human actually looks,
        # rather than requiring them to dig into sit_report for something this consequential.
        if upd["automation_result"] == "passed":
            rung = _safe_rung(upd)
            if rung < 2:
                line += " — Rung " + str(rung) + (" (UNVERIFIED, no real signal)" if rung == 0 else " (partial signal)")
        unmocked = (upd.get("sit_report") or {}).get("unmocked_paths_hit") or []
        if unmocked:
            line += f" — {len(unmocked)} unmocked path(s)"
        return line
    if node == "learn_repo":
        return f"onboarded {upd.get('repo_onboarded', '')}".strip()
    if node == "flip_ready" and upd.get("ready_flipped"):
        return "ready-for-review"
    if node == "prep_rework":
        return "looping back to Coding"
    if node == "prep_env_retry" and upd.get("env_retry_attempts"):
        return f"retry {upd['env_retry_attempts']}, looping back to Local SIT — run"
    return ""


def banner(ticket: str, execution_id: str) -> None:
    print("\n" + "═" * WIDTH)
    print(f"  FK Ocean Pipeline   ·   {ticket}")
    print(f"  run {execution_id}   ·   started {_now()}")
    print("═" * WIDTH, flush=True)


def step(node: str, upd: dict, elapsed: float) -> None:
    """The station's one-line outcome: icon, label, elapsed, highlight. This is team
    level's entire "one line per process" — management doesn't get this line at all
    (it stops at the station_start header); developer additionally gets the detail
    bullets underneath (milestones and the raw per-agent dump are separate, streamed
    during the station rather than printed here at completion)."""
    if not _at_least("team"):
        return
    icon = "✗" if node in _STOP_NODES else "✓"
    label = _LABELS.get(node, node)
    line = f"  {icon}  {label:<38}{_fmt_elapsed(elapsed):>7}"
    hi = _highlight(node, upd)
    if hi:
        line += f"   {hi}"
    print(line, flush=True)
    if _at_least("developer"):
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
