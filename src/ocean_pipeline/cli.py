"""CLI entry point — the LangGraph replacement for /fk-execute (isbu boards).

Usage:
    ocean-pipeline MM-14615
    ocean-pipeline MM-14615 --context "Bug only affects SCAC=ABCD loads"
    ocean-pipeline MM-14615 --log-level management     # top headers + one-line outcome only
    ocean-pipeline MM-14615 --log-level developer       # == -v/--verbose, full raw agent activity
    ocean-pipeline --resume EXE-1a2b3c4d          # continue a crashed run from its last checkpoint
    ocean-pipeline --print-graph                  # print the mermaid diagram from the live graph

The pipeline runs end-to-end: research -> deps -> reachability -> code ->
adversarial review loop -> local SIT (ocean-automation-testing). On SIT PASS it
flips the service PR to ready-for-review automatically (the human boundary is
merge/deploy, which the pipeline never performs).

Console log detail has three tiers (config.LOG_LEVEL) — management (top headers +
one-line outcome only), team (default: headers + outcome + curated milestones/details),
developer (team, plus the full raw per-agent activity). The run-report.md written to
the artifacts dir at the end always has full detail, regardless of console level.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from . import config, metrics, report, telemetry, tracing, ui
from .graph import build_graph, compile_app
from .nodes import _release_build_slot, _release_sit_slot
from .state import OceanState

# Loops (review x code_fault) can chain well past LangGraph's default of 25 node
# transitions, so raise the ceiling; the review/coding budgets are the real caps.
RECURSION_LIMIT = 100


def _project(ticket_id: str) -> str:
    return ticket_id.split("-", 1)[0].upper() if "-" in ticket_id else ""


def _ticket_key(ticket_id: str) -> str:
    """The bare Jira key (e.g. 'MM-14475') from a possibly free-form ticket arg."""
    m = re.search(r"[A-Z]+-\d+", ticket_id or "")
    return m.group(0) if m else ""


def _preflight() -> None:
    """Fail fast with a clear message instead of dying mid-run."""
    problems = []
    if not config.FK_AIDEVELOPER_DIR.exists():
        problems.append(f"FK_AIDEVELOPER_DIR not found: {config.FK_AIDEVELOPER_DIR}")
    elif not config.AGENTS_DIR.exists():
        problems.append(f"station agents dir not found: {config.AGENTS_DIR}")
    else:
        # Version-pin guard (README "Checkout state, not just presence"): the ocean SME files are the
        # fk-aideveloper artifacts the graph reaches into at run time, and origin/main does NOT carry
        # them yet — so a checkout on the wrong branch fails deep at sme_consult with an opaque error.
        # Check them upfront and surface the same #fk-aideveloper Slack-branch hint the README gives.
        missing_smes = [f for f in ("sme-callback-notification.md", "sme-load-creation.md",
                                    "sme-ocean-milestones.md", "sme-ocean-data-quality.md")
                        if not (config.OCEAN_AGENTS_DIR / f).exists()]
        if missing_smes:
            problems.append(
                f"ocean SME file(s) missing from {config.OCEAN_AGENTS_DIR}: {', '.join(missing_smes)} — "
                f"FK_AIDEVELOPER_DIR is likely on a branch that doesn't carry the ocean-coding-agent "
                f"skill. Ask in #fk-aideveloper which branch currently carries this work.")
        # Same version-pin guard for the ocean coding WORKERS — they live in fk-aideveloper's
        # ocean-coding-agent (MM-14620); a checkout without it fails deep at the first agent node.
        missing_workers = [f for f in ("research.md", "dep-resolve.md", "reachability.md",
                                       "code.md", "review.md", "rca-research.md")
                           if not (config.OCEAN_WORKERS_DIR / f).exists()]
        if missing_workers:
            problems.append(
                f"ocean-coding-agent worker(s) missing from {config.OCEAN_WORKERS_DIR}: "
                f"{', '.join(missing_workers)} — FK_AIDEVELOPER_DIR is likely on a branch without the "
                f"ocean-coding-agent skill. Ask in #fk-aideveloper which branch currently carries it.")
    if shutil.which("claude") is None:
        problems.append("`claude` CLI not on PATH — the Claude Agent SDK spawns it (install Claude Code)")
    # gitops.py's own docstring says "the caller is responsible for `gh auth` (preflight checks
    # it)" — this is that check. Without it, a missing/unauthenticated `gh` surfaces as a raw
    # GitOpError deep inside open_pr/flip_ready instead of a clean upfront message.
    if shutil.which("gh") is None:
        problems.append("`gh` CLI not on PATH — gitops.py shells out to it for PR create/list/ready")
    else:
        try:
            proc = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True,
                                  timeout=config.GH_TIMEOUT_SECONDS)
            if proc.returncode != 0:
                problems.append(f"`gh auth status` failed — run `gh auth login`: "
                                 f"{proc.stderr.strip() or proc.stdout.strip()}")
        except subprocess.TimeoutExpired:
            problems.append(f"`gh auth status` timed out after {config.GH_TIMEOUT_SECONDS}s")
    if problems:
        raise SystemExit("preflight failed:\n  - " + "\n  - ".join(problems))


def _report(execution_id: str, final: dict, total: float) -> None:
    telemetry.execution_end(execution_id, final.get("ticket_id", ""),
                            final.get("final_status", "failed"), final.get("route", "coding"),
                            final_outcome=final.get("final_outcome", ""))
    ui.summary(final, total)


async def _drive_stream(app, initial, thread) -> tuple[dict, float, tuple[str, ...]]:
    """Stream station completions as a clean runner log, then return (state, elapsed, paused_on).

    One line per station (plain-English name, elapsed, outcome). Per-node elapsed is
    the wall-clock between consecutive completions — the pipeline runs sequentially,
    so that is the station's own runtime. Everything above each line (milestones, and
    at developer level the raw agent activity) is gated by config.LOG_LEVEL — see ui.py.
    `paused_on` is `snapshot.next` — the node name(s) the graph is sitting in front of when
    it stopped at an interrupt(), e.g. `("qa_review_gate",)` — empty when the run reached END
    instead of pausing. There are three distinct interrupt() gates (rca_review_gate,
    qa_review_gate, human_gate), not all with the same resume flags, so the caller needs to
    know which one this is, not just whether the run paused at all — see _pause_message."""
    start = time.monotonic()
    last = start
    async for chunk in app.astream(initial, config=thread, stream_mode="updates"):
        now = time.monotonic()
        for node, update in chunk.items():
            if node == "__interrupt__":   # human-approval gate paused the run — not a real node
                continue
            ui.step(node, update, now - last)
            report.record(node, now - last, update)
        last = now
    snapshot = await app.aget_state(thread)
    return snapshot.values, time.monotonic() - start, snapshot.next


def _pause_message(execution_id: str, paused_on: tuple[str, ...]) -> str:
    """The resume hint for whichever interrupt() gate the graph is actually sitting in front
    of. Three gates exist, not all with the same resume flags — a generic "awaiting approval,
    use --approve/--reject" message is wrong whenever the pause is at qa_review_gate (pauses by
    default, uses --qa) rather than rca_review_gate or human_gate (both also plain approve/reject
    choices, and share --approve/--reject — safe because they can never both be the pending
    interrupt at once: rca_review_gate resolves before dep_resolver/coder even start, long before
    the run could reach human_gate)."""
    if "rca_review_gate" in paused_on:
        return (f"awaiting review of the RCA report already posted as a Jira comment.\n"
                f"  approve: ocean-pipeline --resume {execution_id} --approve\n"
                f"  reject:  ocean-pipeline --resume {execution_id} --reject")
    if "qa_review_gate" in paused_on:
        return (f"awaiting QA review of the drafted SIT.\n"
                f"  approve + TestRail:    ocean-pipeline --resume {execution_id} --qa approve-testrail\n"
                f"  approve, no TestRail:  ocean-pipeline --resume {execution_id} --qa approve-no-testrail\n"
                f"  request changes:       ocean-pipeline --resume {execution_id} --qa changes --note '<feedback>'")
    if "human_gate" in paused_on:
        return (f"awaiting human approval before flipping the service PR ready.\n"
                f"  approve: ocean-pipeline --resume {execution_id} --approve\n"
                f"  reject:  ocean-pipeline --resume {execution_id} --reject")
    # Any future interrupt() gate that lands here without an entry above — surface the raw
    # node name rather than silently reusing the wrong gate's flags.
    return (f"paused at {', '.join(paused_on)!r} — no known resume flags for this gate yet; "
            f"resume with `ocean-pipeline --resume {execution_id}` and check nodes.py for what it expects.")


def _run_repos(execution_id: str, final: dict) -> list[str]:
    """Repo names this run targeted — from the final graph state, else the researcher verdict / research
    packet on disk (present even on a crash, since research runs first). Used to know which
    `<repo>-cached` base image to purge."""
    repos: list[str] = []
    def _collect(target_repos):
        for r in (target_repos or []):
            n = (r.get("repo") or "").split("/")[-1] if isinstance(r, dict) else ""
            if n:
                repos.append(n)
    _collect(final.get("target_repos"))
    if not repos:
        adir = config.artifacts_dir(execution_id)
        for f in list(adir.glob("research*packet*.json")) + list(adir.glob("researcher.verdict.json")):
            try:
                _collect(json.loads(f.read_text()).get("target_repos"))
            except Exception:  # noqa: BLE001 — best-effort disk fallback
                pass
            if repos:
                break
    return list(dict.fromkeys(repos))   # dedup, preserve order


def _cleanup_run_docker(execution_id: str, ticket_id: str, repos: list[str]) -> list[str]:
    """Remove the Docker resources THIS run created — called from _execute's `finally`, so it fires on
    success, failure, AND crash (unlike the graph's teardown node, which only runs on a terminal node,
    so a crashed run — e.g. a StationError at reachability — leaked its container/image). Removes every
    container named for this run (the orchestrator's `ocean-<repo>-<exec_id>` and the worker-recipe
    `ocean-<repo>-<TICKET>` / `ocean-ow-<ticket>-coder`) and the run's ticket-scoped `*-mm<ticket>-*`
    images. The SHARED lock-scoped `<repo>-cached:<hash>` base image is removed too UNLESS
    config.KEEP_CACHED_IMAGE (default: delete — one ticket at a time leaves no residue; set the flag to
    keep it for cross-run reuse). Best-effort: never raises — a cleanup hiccup must not mask the run's
    outcome. Ticket is matched with a delimiter (`-mm14235-`/`-mm14235:`/ends-with) so MM-1423 never
    nukes MM-14235's resources."""
    if not shutil.which("docker"):
        return []
    exl = (execution_id or "").lower()
    tkl = (ticket_id or "").lower()            # mm-14235
    tkey = tkl.replace("-", "")                # mm14235

    def _d(args):
        try:
            return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.SubprocessError):
            return None

    def _owns(name: str) -> bool:
        # EVERY ticket clause is guarded on a non-empty key: `"x".endswith("")` is True, so an empty
        # ticket_id (e.g. `--resume` after a lost checkpoint, which falls back to "") would otherwise
        # match EVERY container and `rm -f` the whole host. With ticket empty we fall back to exec-id
        # matching only. Both dash-less (`ocean-ow-mm14235-coder`) and dashed (`ocean-…-mm-14235`)
        # ticket forms are covered.
        n = name.lower()
        return bool((exl and exl in n)
                    or (tkl and (n.endswith(tkl) or f"-{tkl}-" in n or n.endswith("-" + tkl)))
                    or (tkey and (f"-{tkey}-" in n or n.endswith("-" + tkey))))

    removed: list[str] = []
    out = _d(["ps", "-a", "--format", "{{.Names}}"])                   # containers this run created
    for name in (out.stdout.split() if out and out.returncode == 0 else []):
        if _owns(name) and (_d(["rm", "-f", name]) or _Bad()).returncode == 0:
            removed.append(f"container:{name}")
    out = _d(["images", "--format", "{{.Repository}}:{{.Tag}}"])       # ticket-scoped images, NOT -cached
    for img in (out.stdout.splitlines() if out and out.returncode == 0 else []):
        il = img.lower()
        if "-cached:" in il:
            continue
        if tkey and (f"-{tkey}-" in il or f"-{tkey}:" in il) and (_d(["rmi", "-f", img]) or _Bad()).returncode == 0:
            removed.append(f"image:{img}")
    # shared <repo>-cached base image — delete by default (single-ticket workflow), keep only if flagged.
    # NB: this removes ALL tags of <repo>-cached, not just this run's lock-hash — so two CONCURRENT
    # same-repo runs must set KEEP_CACHED_IMAGE=1, else the first to finish pulls the image out from the
    # second (it just rebuilds; not destructive). The single-ticket-at-a-time default is safe.
    if not config.KEEP_CACHED_IMAGE:
        for repo in repos:
            out = _d(["images", f"{repo}-cached", "--format", "{{.Repository}}:{{.Tag}}"])
            for img in (out.stdout.splitlines() if out and out.returncode == 0 else []):
                if img.strip() and (_d(["rmi", "-f", img.strip()]) or _Bad()).returncode == 0:
                    removed.append(f"image:{img.strip()}")
    return removed


class _Bad:                       # tiny stand-in so `(_d(...) or _Bad()).returncode` is always safe
    returncode = 1


async def _execute(execution_id: str, ticket_id: str, initial, thread) -> None:
    """Run (or resume) the graph, guaranteeing a telemetry END row + report even on failure."""
    Path(config.CHECKPOINT_DB).parent.mkdir(parents=True, exist_ok=True)
    metrics.reset()
    # Expose the exec-id so each worker's stream is captured to artifacts_dir/<station>.log — a stall
    # (EXE-0417bc97's whole-disk grep) was only diagnosable by external forensics because nothing was
    # logged; a per-station log makes the worker's last action visible in ~1s.
    os.environ["OCEAN_PIPELINE_EXEC_ID"] = execution_id
    report.start(ticket_id, execution_id)
    out_dir = config.artifacts_dir(execution_id)
    handler = tracing.callback_handler()   # self-hosted Langfuse, or None if unconfigured
    if handler is not None:
        tkey = _ticket_key(ticket_id)
        thread = {
            **thread,
            "callbacks": [handler],
            "run_name": tkey or "aquaman-run",   # names the Langfuse trace by the ticket
            "metadata": {
                **thread.get("metadata", {}),
                "langfuse_session_id": execution_id,
                "langfuse_tags": ["aquaman"] + ([tkey] if tkey else []),
            },
        }
        print(f"[ocean-pipeline] Langfuse tracing → {tracing.host()}"
              f"  (trace: {tkey or 'aquaman-run'}, tags: aquaman{',' + tkey if tkey else ''})")
    final: dict = {}
    paused_on: tuple = ()   # stays () on a crash before _drive_stream returns → finally treats it as terminal
    try:
        async with AsyncSqliteSaver.from_conn_string(config.CHECKPOINT_DB) as saver:
            app = compile_app(saver)
            final, total, paused_on = await _drive_stream(app, initial, thread)
        if paused_on:
            # Leave the run 'running' (no END row) — the resume finalizes it. Three DIFFERENT
            # interrupt() gates exist (rca_review_gate, qa_review_gate, human_gate), not all
            # with the same resume flags — printing the wrong pair here silently misroutes the
            # resume (a bare `--approve` on a paused qa_review_gate falls through
            # after_qa_review's default branch instead of erroring) rather than failing loudly,
            # so getting this right matters more than it looks.
            print(f"\n[PAUSED] {_pause_message(execution_id, paused_on)}")
        else:
            _report(execution_id, final, total)
            # Machine-readable completion line for headless runners (oas-autodev's
            # reconcile matches `pr=#<n>` / a PR URL in the log tail). Emitting the PR
            # number on a clean finish means a green run is tracked as ready-for-merge
            # instead of being mis-classified as blocked (exit 0 + no PR string).
            pr = final.get("pr_number")
            print(f"[DONE] {ticket_id} status={final.get('final_status') or 'completed'}"
                  + (f" pr=#{pr}" if pr else ""))
    except Exception as e:  # noqa: BLE001 — never leave a `running` row orphaned (AP-223)
        telemetry.execution_end(execution_id, ticket_id, "failed", "unknown",
                                final_outcome=f"{type(e).__name__}: {e}")
        print(f"\n[FAILED] {type(e).__name__}: {e}")
        final = {**final, "final_status": final.get("final_status") or "failed",
                 "final_outcome": final.get("final_outcome") or f"{type(e).__name__}: {e}"}
        raise
    finally:
        try:
            path = report.finish(final, out_dir)
            if path:
                print(f"  Full report: {path}")
        except Exception:
            pass
        try:
            # Remove THIS run's containers + ticket-scoped images on any TERMINAL exit (success / fail /
            # crash) so a batch of tickets doesn't accumulate them; the shared <repo>-cached base image
            # is kept. Skip on a PAUSE — the run is mid-flight and will resume + reuse the container.
            if not paused_on:
                cleaned = _cleanup_run_docker(execution_id, ticket_id, _run_repos(execution_id, final))
                if cleaned:
                    print(f"  Cleaned {len(cleaned)} run Docker resource(s): "
                          + ", ".join(cleaned[:8]) + (" …" if len(cleaned) > 8 else ""))
        except Exception:
            pass
        try:
            # Release the SIT concurrency slot (#18) on any TERMINAL exit — guaranteed here even when the
            # conditional teardown_container node didn't run (both container features off) or a StationError
            # aborted the graph before it. Kept on a PAUSE (the stack stays up; a resume re-holds it).
            if not paused_on:
                _release_sit_slot(execution_id)
                # Same backstop for the build-slot hold: a crash mid-coder/mid-harsh_reviewer/mid-
                # reachability_gate (before that node's own `finally` runs) or mid-prep_container (before
                # teardown_container runs) must still free it on process exit. Safe/no-op if this
                # execution never held one.
                _release_build_slot(execution_id)
        except Exception:
            pass
        if handler is not None:
            tracing.flush()
        telemetry.flush()   # drain best-effort aidev-db writes so the END row lands before exit


async def _run(ticket_id: str, context: str) -> None:
    _preflight()
    if _project(ticket_id) not in config.ISBU_PROJECTS:
        raise SystemExit(
            f"{ticket_id}: not an isbu board. This orchestrator runs the full Ocean/MM pipeline only."
        )
    execution_id = telemetry.new_execution_id()
    telemetry.execution_start(execution_id, ticket_id)

    thread = {"configurable": {"thread_id": execution_id}, "recursion_limit": RECURSION_LIMIT}
    initial: OceanState = {
        "ticket_id": ticket_id,
        "execution_id": execution_id,
        "profile": "isbu",
        "context": context,
        "review_iteration": 0,
        "review_findings": [],
        "coding_attempts": 0,
        "sit_findings": [],
    }
    ui.banner(ticket_id, execution_id)
    await _execute(execution_id, ticket_id, initial, thread)


async def _resume_ticket_id(execution_id: str, thread: dict) -> str:
    """Recover the real ticket_id from the checkpointed graph state before _execute runs.

    Without this, a resume hardcoded ticket_id="" — even though the checkpoint has held the
    real value all along (proven by _report()'s own use of final.get("ticket_id", "") on the
    clean-finish path). That blanked the Ticket field in run-report.md on every resume, and a
    failed resume would telemetrize execution_end with an empty ticket_id too. Best-effort: a
    missing/corrupt checkpoint falls back to "" rather than raising, so a resume of a truly
    unknown execution_id still surfaces _execute's own errors instead of a new one here."""
    try:
        async with AsyncSqliteSaver.from_conn_string(config.CHECKPOINT_DB) as saver:
            app = compile_app(saver)
            snapshot = await app.aget_state(thread)
            return snapshot.values.get("ticket_id", "")
    except Exception:  # noqa: BLE001 — best-effort recovery only, never block the resume
        return ""


async def _resume(execution_id: str, resume_value=None) -> None:
    _preflight()
    thread = {"configurable": {"thread_id": execution_id}, "recursion_limit": RECURSION_LIMIT}
    ticket_id = await _resume_ticket_id(execution_id, thread)
    # A plain crash-resume replays from the checkpoint (input None). Resuming a paused gate injects
    # the decision via Command(resume=...) so the pending interrupt() returns it — a plain string
    # ("approve"/"reject") for the rca_review_gate or ready-flip human_gate, or a {decision, note}
    # dict for the QA review gate.
    initial = None
    if resume_value is not None:
        from langgraph.types import Command
        initial = Command(resume=resume_value)
    print(f"[ocean-pipeline] resuming execution={execution_id}"
          + (f" ({resume_value})" if resume_value else ""))
    await _execute(execution_id, ticket_id, initial, thread)


def main() -> None:
    p = argparse.ArgumentParser(prog="ocean-pipeline")
    p.add_argument("ticket", nargs="?", help="Jira ticket id, e.g. MM-14615")
    p.add_argument("--context", default="", help="extra context for the run")
    p.add_argument("--resume", metavar="EXECUTION_ID", help="continue a crashed run from its last checkpoint")
    p.add_argument("--approve", action="store_true",
                   help="with --resume: approve a paused rca_review_gate (proceed to report/coding) "
                        "or ready-flip human_gate (proceed to ready-flip)")
    p.add_argument("--reject", action="store_true",
                   help="with --resume: reject a paused rca_review_gate (stop before any coding) "
                        "or ready-flip human_gate (leave the PR draft)")
    p.add_argument("--qa", choices=["approve-testrail", "approve-no-testrail", "changes"],
                   help="with --resume: answer a paused QA review gate")
    p.add_argument("--note", default="", help="with --resume --qa changes: feedback for the redraft")
    p.add_argument("--print-graph", action="store_true", help="print the mermaid diagram and exit")
    p.add_argument("--rca-only", action="store_true",
                   help="run research + ocean-rca report and STOP (no auto-coding, even if a fix is needed)")
    p.add_argument("--log-level", choices=["management", "team", "developer"], default=None,
                   help="console log detail: management (top headers + one-line outcome only), "
                        "team (default — headers + outcome + curated milestones/details), "
                        "developer (team, plus the full raw per-agent activity, i.e. --verbose)")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="shorthand for --log-level developer (stream each station agent's "
                        "live activity: tool calls, tool results, thinking, text)")
    args = p.parse_args()

    if args.log_level:
        config.LOG_LEVEL = args.log_level
    elif args.verbose:
        config.LOG_LEVEL = "developer"
    if args.rca_only:
        config.RCA_ONLY = True

    if args.print_graph:
        print(build_graph().compile().get_graph().draw_mermaid())
    elif args.resume:
        if args.qa:   # QA review gate: {decision, note}
            resume_value = {"decision": args.qa.replace("-", "_"), "note": args.note}
        elif args.approve:
            resume_value = "approve"
        elif args.reject:
            resume_value = "reject"
        else:
            resume_value = None
        asyncio.run(_resume(args.resume, resume_value))
    elif args.ticket:
        asyncio.run(_run(args.ticket, args.context))
    else:
        p.error("provide a ticket id, --resume EXECUTION_ID, or --print-graph")


if __name__ == "__main__":
    main()
