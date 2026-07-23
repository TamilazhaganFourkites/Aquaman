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
import shutil
import time
from pathlib import Path

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from . import config, metrics, telemetry, tracing, ui
from .graph import build_graph, compile_app
from .state import OceanState

# Loops (review x code_fault) can chain well past LangGraph's default of 25 node
# transitions, so raise the ceiling; the review/coding budgets are the real caps.
RECURSION_LIMIT = 100


def _project(ticket_id: str) -> str:
    return ticket_id.split("-", 1)[0].upper() if "-" in ticket_id else ""


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
                            final.get("final_status", "failed"), final.get("route", "coding"))
    ui.summary(final, total)


async def _drive_stream(app, initial, thread) -> tuple[dict, float]:
    """Stream station completions as a clean runner log, then return (state, elapsed).

    One line per station (plain-English name, elapsed, outcome). Per-node elapsed is
    the wall-clock between consecutive completions — the pipeline runs sequentially,
    so that is the station's own runtime. --verbose adds the raw agent activity above
    each line."""
    start = time.monotonic()
    last = start
    async for chunk in app.astream(initial, config=thread, stream_mode="updates"):
        now = time.monotonic()
        for node, update in chunk.items():
            ui.step(node, update, now - last)
        last = now
    snapshot = await app.aget_state(thread)
    return snapshot.values, time.monotonic() - start


async def _execute(execution_id: str, ticket_id: str, initial, thread) -> None:
    """Run (or resume) the graph, guaranteeing a telemetry END row even on failure."""
    Path(config.CHECKPOINT_DB).parent.mkdir(parents=True, exist_ok=True)
    handler = tracing.callback_handler()   # self-hosted Langfuse, or None if unconfigured
    if handler is not None:
        thread = {**thread, "callbacks": [handler]}
        print(f"[ocean-pipeline] Langfuse tracing → {tracing.host()}")
    try:
        async with AsyncSqliteSaver.from_conn_string(config.CHECKPOINT_DB) as saver:
            app = compile_app(saver)
            final, total = await _drive_stream(app, initial, thread)
        _report(execution_id, final, total)
    except Exception as e:  # noqa: BLE001 — never leave a `running` row orphaned (AP-223)
        telemetry.execution_end(execution_id, ticket_id, "failed", "unknown")
        print(f"\n[FAILED] {type(e).__name__}: {e}")
        raise
    finally:
        if handler is not None:
            tracing.flush()


async def _run(ticket_id: str, context: str) -> None:
    _preflight()
    if _project(ticket_id) not in config.ISBU_PROJECTS:
        raise SystemExit(
            f"{ticket_id}: not an isbu board. This orchestrator runs the full Ocean/MM pipeline only."
        )
    execution_id = telemetry.new_execution_id()
    telemetry.execution_start(execution_id, ticket_id)
    metrics.reset()

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


async def _resume(execution_id: str) -> None:
    _preflight()
    thread = {"configurable": {"thread_id": execution_id}, "recursion_limit": RECURSION_LIMIT}
    print(f"[ocean-pipeline] resuming execution={execution_id}")
    await _execute(execution_id, "", None, thread)  # None -> resume from checkpoint


def main() -> None:
    p = argparse.ArgumentParser(prog="ocean-pipeline")
    p.add_argument("ticket", nargs="?", help="Jira ticket id, e.g. MM-14615")
    p.add_argument("--context", default="", help="extra context for the run")
    p.add_argument("--resume", metavar="EXECUTION_ID", help="continue a crashed run from its last checkpoint")
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
        asyncio.run(_resume(args.resume))
    elif args.ticket:
        asyncio.run(_run(args.ticket, args.context))
    else:
        p.error("provide a ticket id, --resume EXECUTION_ID, or --print-graph")


if __name__ == "__main__":
    main()
