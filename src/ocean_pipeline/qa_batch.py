"""ocean-pipeline-qa-batch — unattended, sequential local-testing regression sweep over a LIST of
already-ticketed, already-PR'd tickets (ocean-qa-agent-ac-driven-plan.md Workstream 4).

This is deliberately NOT the full ocean-pipeline: it never researches, never codes, never reviews,
and — critically — it NEVER flips a service PR to ready. It exists purely to re-run local automation
testing (author-if-needed -> run -> report, Station 6 of the main graph) across N tickets overnight,
reusing the EXACT SAME node functions the main graph already tests (nodes.sit_resolve/sit_author/
qa_review_gate/sit_run/sit_testrail/sit_triage/learn_repo) — no parallel re-implementation of Station 6
logic, so every fix that graph carries (the resource pre-flight, the marker-based short-circuit,
ac_coverage passthrough, graph-owned repo onboarding) applies here automatically, for free.

A `code_fault` verdict terminates the ticket's run with that finding recorded — this mode CANNOT
self-heal via the coder (there is no coder node in this subgraph); a code_fault ticket needs an
engineer or a full `ocean-pipeline <ticket>` run, not another QA-batch pass.

Usage:
    ocean-pipeline-qa-batch MM-101 MM-102 MM-103
    ocean-pipeline-qa-batch -f tickets.txt        # one ticket id per line, # comments / blanks ok
    python -m ocean_pipeline.qa_batch MM-101      # works without a `pip install -e .` reinstall

Sequential, not parallel — Station 6 contends for Docker/ports/local repo checkouts across runs (the
same reason `run-batch.sh`'s full-pipeline batch is sequential). A single ticket's failure never aborts
the batch; a green/red summary prints at the end, and every ticket still gets its own Langfuse trace
(named by the ticket key, exactly like a normal `ocean-pipeline` run) if LANGFUSE_* is configured.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from langgraph.graph import END, StateGraph

from . import config, graph as graph_mod, nodes, telemetry, tracing, ui
from .state import OceanState


def _qa_batch_finish(state: OceanState) -> dict:
    """Terminal node for THIS subgraph only — batch mode never flips a PR, unlike the main graph's
    flip_ready. A 'passed' ticket here just means local testing is green; ready-for-review is a
    decision for a full `ocean-pipeline <ticket>` run or an engineer, not this unattended sweep."""
    result = state.get("automation_result", "failed")
    fc = state.get("failure_class", "")
    if result == "passed":
        outcome = "sit_passed (qa-batch mode: not flipped; re-run the full pipeline or flip by hand)"
    elif fc == "code_fault":
        outcome = "code_fault: needs a coder re-run (`ocean-pipeline <ticket>`) — not fixable in qa-batch mode"
    elif state.get("needs_onboarding"):
        outcome = "repo_onboarding_exhausted"
    else:
        outcome = fc or "sit_failed"
    return {"final_status": "completed" if result == "passed" else "failed", "final_outcome": outcome}


def build_qa_subgraph():
    """Station-6-only subgraph: sit_resolve -> (onboard | author->qa_review->run[+testrail]) -> triage.
    Reuses nodes.py's own tested functions verbatim — see module docstring for why that matters."""
    g = StateGraph(OceanState)
    g.add_node("sit_resolve", nodes.sit_resolve)
    g.add_node("sit_author", nodes.sit_author)
    g.add_node("qa_review_gate", nodes.qa_review_gate)
    g.add_node("sit_run", nodes.sit_run)
    g.add_node("sit_testrail", nodes.sit_testrail)
    g.add_node("sit_triage", nodes.sit_triage)
    g.add_node("learn_repo", nodes.learn_repo)
    g.add_node("qa_batch_finish", _qa_batch_finish)

    g.set_entry_point("sit_resolve")
    g.add_conditional_edges("sit_resolve", graph_mod.after_sit_resolve, {
        "onboard": "learn_repo", "run": "sit_author", "stop": "qa_batch_finish",
    })
    g.add_edge("sit_author", "qa_review_gate")
    g.add_conditional_edges("qa_review_gate", graph_mod.after_qa_review, {
        "sit_author": "sit_author", "sit_run": "sit_run", "sit_testrail": "sit_testrail",
    })
    g.add_edge("sit_run", "sit_triage")
    g.add_edge("sit_testrail", "sit_triage")
    # code_fault has nowhere to loop to in this subgraph (no coder) — always terminate and report,
    # regardless of coding_attempts budget (which stays 0 forever here).
    g.add_conditional_edges("sit_triage", lambda s: (
        "onboard" if (s.get("needs_onboarding") and s.get("onboard_attempts", 0) < config.MAX_ONBOARD_ATTEMPTS)
        else "finish"
    ), {"onboard": "learn_repo", "finish": "qa_batch_finish"})
    g.add_edge("learn_repo", "sit_resolve")
    g.add_edge("qa_batch_finish", END)
    return g


async def run_one(ticket_id: str) -> dict:
    """Run the QA-only subgraph for ONE ticket (no checkpointer needed — a batch item is one shot,
    never resumed mid-run; a crash just fails that ticket and the batch moves on).

    Sets config.QA_REVIEW_AUTO here, not just in main() — this is the actual function that builds
    and runs the graph, so it's the one place that can guarantee the invariant regardless of
    whether the caller went through main()/run_batch or imported run_one directly (this module's
    own docstring documents `python -m ocean_pipeline.qa_batch` as a supported usage, and a direct
    import is just as plausible). Without this, a caller that bypasses main() would hit
    qa_review_gate's interrupt() with nobody watching — the run never resumes (no checkpointer;
    "never resumed mid-run" above), silently defeating the entire unattended-sweep purpose."""
    config.QA_REVIEW_AUTO = True
    execution_id = telemetry.new_execution_id()
    telemetry.station_event(execution_id, 6, "qa_batch_start", ticket_id=ticket_id)
    app = build_qa_subgraph().compile()
    initial: OceanState = {
        "ticket_id": ticket_id, "execution_id": execution_id, "profile": "isbu", "context": "",
        "coding_attempts": 0, "sit_findings": [],
    }
    thread = {"configurable": {"thread_id": execution_id}, "recursion_limit": 50}
    handler = tracing.callback_handler()
    if handler is not None:
        thread = {**thread, "callbacks": [handler], "run_name": ticket_id,
                 "metadata": {"langfuse_session_id": execution_id,
                              "langfuse_tags": ["aquaman", "qa-batch", ticket_id]}}
    try:
        final = await app.ainvoke(initial, config=thread)
    except Exception as e:  # noqa: BLE001 — one ticket's crash must not abort the batch
        final = {"final_status": "failed", "final_outcome": f"{type(e).__name__}: {e}"}
    finally:
        if handler is not None:
            tracing.flush()
    telemetry.station_event(execution_id, 6, "qa_batch_end",
                            final_status=final.get("final_status"))
    return {"ticket_id": ticket_id, "execution_id": execution_id, **final}


async def run_batch(tickets: list[str]) -> list[dict]:
    """SEQUENTIAL — Station 6 contends for Docker/ports/local repo checkouts; a parallel batch would
    corrupt itself the same way two engineers running SIT at once would. One ticket's failure never
    stops the rest (run_one already swallows its own exceptions into a failed result)."""
    results = []
    for i, t in enumerate(tickets, 1):
        ui.milestone(f"[{i}/{len(tickets)}] {t}")
        results.append(await run_one(t))
    return results


def _print_summary(results: list[dict]) -> None:
    print("\n" + "=" * 64)
    print(f"  QA BATCH SUMMARY — {len(results)} ticket(s)")
    print("=" * 64)
    print(f"{'TICKET':<14} {'STATUS':<12} {'EXECUTION':<14} OUTCOME")
    for r in results:
        status = "✅ ok" if r.get("final_status") == "completed" else "❌ fail"
        print(f"{r['ticket_id']:<14} {status:<12} {r['execution_id']:<14} {r.get('final_outcome', '')}")


def _load_tickets(args) -> list[str]:
    if args.file:
        lines = Path(args.file).read_text().splitlines()
        return [ln.split("#", 1)[0].strip() for ln in lines if ln.split("#", 1)[0].strip()]
    return args.tickets


def main() -> None:
    p = argparse.ArgumentParser(prog="ocean-pipeline-qa-batch")
    p.add_argument("tickets", nargs="*", help="Jira ticket ids, e.g. MM-101 MM-102")
    p.add_argument("-f", "--file", help="path to a file with one ticket id per line")
    args = p.parse_args()
    tickets = _load_tickets(args)
    if not tickets:
        p.error("provide ticket ids, or -f tickets.txt")
    results = asyncio.run(run_batch(tickets))  # run_one sets config.QA_REVIEW_AUTO -- see its docstring
    _print_summary(results)
    sys.exit(0 if all(r.get("final_status") == "completed" for r in results) else 1)


if __name__ == "__main__":
    main()
