"""CLI entry point — the LangGraph replacement for /fk-execute (isbu boards).

Usage:
    ocean-pipeline MM-14615
    ocean-pipeline MM-14615 --context "Bug only affects SCAC=ABCD loads"
    ocean-pipeline --resume EXE-1a2b3c4d          # continue a crashed run from its last checkpoint
    ocean-pipeline --print-graph                  # print the mermaid diagram from the live graph

The pipeline runs end-to-end: research -> deps -> reachability -> code ->
adversarial review loop -> local SIT (ocean-automation-testing). On SIT PASS it
flips the service PR to ready-for-review automatically (the human boundary is
merge/deploy, which the pipeline never performs).
"""
from __future__ import annotations

import argparse
import asyncio
import re
import shutil
import time
from pathlib import Path

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from . import config, metrics, report, telemetry, tracing, ui
from .graph import build_graph, compile_app
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
    if shutil.which("claude") is None:
        problems.append("`claude` CLI not on PATH — the Claude Agent SDK spawns it (install Claude Code)")
    if problems:
        raise SystemExit("preflight failed:\n  - " + "\n  - ".join(problems))


def _report(execution_id: str, final: dict, total: float) -> None:
    telemetry.execution_end(execution_id, final.get("ticket_id", ""),
                            final.get("final_status", "failed"), final.get("route", "coding"),
                            final_outcome=final.get("final_outcome", ""))
    ui.summary(final, total)


async def _drive_stream(app, initial, thread) -> tuple[dict, float, bool]:
    """Stream station completions as a clean runner log, then return (state, elapsed, paused).

    One line per station (plain-English name, elapsed, outcome). Per-node elapsed is
    the wall-clock between consecutive completions — the pipeline runs sequentially,
    so that is the station's own runtime. --verbose adds the raw agent activity above
    each line. `paused` is True when the graph stopped at an interrupt() (the human-approval
    gate) rather than reaching an end — the run is resumable with --approve/--reject."""
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
    return snapshot.values, time.monotonic() - start, bool(snapshot.next)


async def _execute(execution_id: str, ticket_id: str, initial, thread) -> None:
    """Run (or resume) the graph, guaranteeing a telemetry END row + report even on failure."""
    Path(config.CHECKPOINT_DB).parent.mkdir(parents=True, exist_ok=True)
    metrics.reset()
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
    try:
        async with AsyncSqliteSaver.from_conn_string(config.CHECKPOINT_DB) as saver:
            app = compile_app(saver)
            final, total, paused = await _drive_stream(app, initial, thread)
        if paused:
            # Human-approval gate: leave the run 'running' (no END row) — the resume finalizes it.
            print(f"\n[PAUSED] awaiting human approval before flipping the service PR ready.\n"
                  f"  approve: ocean-pipeline --resume {execution_id} --approve\n"
                  f"  reject:  ocean-pipeline --resume {execution_id} --reject")
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


async def _resume(execution_id: str, resume_value=None) -> None:
    _preflight()
    thread = {"configurable": {"thread_id": execution_id}, "recursion_limit": RECURSION_LIMIT}
    # A plain crash-resume replays from the checkpoint (input None). Resuming a paused gate injects
    # the decision via Command(resume=...) so the pending interrupt() returns it — a string for the
    # ready-flip gate ("approve"/"reject"), or a {decision, note} dict for the QA review gate.
    initial = None
    if resume_value is not None:
        from langgraph.types import Command
        initial = Command(resume=resume_value)
    print(f"[ocean-pipeline] resuming execution={execution_id}"
          + (f" ({resume_value})" if resume_value else ""))
    await _execute(execution_id, "", initial, thread)


def main() -> None:
    p = argparse.ArgumentParser(prog="ocean-pipeline")
    p.add_argument("ticket", nargs="?", help="Jira ticket id, e.g. MM-14615")
    p.add_argument("--context", default="", help="extra context for the run")
    p.add_argument("--resume", metavar="EXECUTION_ID", help="continue a crashed run from its last checkpoint")
    p.add_argument("--approve", action="store_true",
                   help="with --resume: approve a paused human-approval gate (proceed to ready-flip)")
    p.add_argument("--reject", action="store_true",
                   help="with --resume: reject a paused human-approval gate (leave the PR draft)")
    p.add_argument("--qa", choices=["approve-testrail", "approve-no-testrail", "changes"],
                   help="with --resume: answer a paused QA review gate")
    p.add_argument("--note", default="", help="with --resume --qa changes: feedback for the redraft")
    p.add_argument("--print-graph", action="store_true", help="print the mermaid diagram and exit")
    p.add_argument("--rca-only", action="store_true",
                   help="run research + ocean-rca report and STOP (no auto-coding, even if a fix is needed)")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="stream each station agent's live activity (tool calls + text)")
    args = p.parse_args()

    if args.verbose:
        config.VERBOSE = True
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
