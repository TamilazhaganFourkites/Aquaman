"""Graph wiring. The control flow that the fk-execute prose checklist described
is now deterministic edges:

  - the RCA router,
  - the Station 5 <-> Station 4 review loop (capped at MAX_REVIEW_ITERATIONS),
  - the Station 6 outcomes: PASS -> flip service PR ready; code_fault -> FULL loop
    back through coder -> review -> Station 6 (capped at MAX_CODING_ATTEMPTS);
    could_not_verify / exhausted budget -> stop (service PR left draft).
"""
from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from . import config, nodes
from .state import OceanState


def route_after_research(state: OceanState) -> str:
    r = state.get("route")
    # RCA-only run (control-plane "In RCA" stage): always take the analysis path, even if
    # the researcher leaned "coding" — the fix is a separate later run ("RCA Done").
    if config.RCA_ONLY and r != "unsupported" and r in ("rca", "coding"):
        return "rca"
    if r in ("rca", "coding"):
        return r
    # sop / loft / ff_onboarding / unclassified are handled by other harnesses, not this
    # Ocean pipeline — stop cleanly rather than silently coding a non-coding ticket.
    return "unsupported"


def after_rca(state: OceanState) -> str:
    # RCA agent -> RCA Done (terminal report) | Fix needed -> deps+reachability -> coder
    # In RCA-only mode we always stop at the report; the fix is a separate "RCA Done" run.
    if config.RCA_ONLY:
        return "done"
    return "fix_needed" if state.get("rca_fix_needed") else "done"


def after_review(state: OceanState) -> str:
    if state.get("review_verdict") == "APPROVE":
        return "approve"
    if state.get("review_iteration", 0) >= config.MAX_REVIEW_ITERATIONS:
        # Review budget spent with findings outstanding: stop looping and open the PR;
        # Station 6 (SIT) is the next gate, and a code_fault there still triggers rework.
        return "approve"
    return "rework"


def after_automation(state: OceanState) -> str:
    if state.get("automation_result") == "passed":
        return "pass"
    # failed:
    if (state.get("failure_class") == "code_fault"
            and state.get("coding_attempts", 0) < config.MAX_CODING_ATTEMPTS):
        return "code_fault"        # re-enter the full coder -> review -> SIT loop
    return "stop"                  # could_not_verify, or code_fault budget exhausted


def build_graph():
    g = StateGraph(OceanState)

    g.add_node("researcher", nodes.researcher)
    g.add_node("rca_agent", nodes.rca_agent)
    g.add_node("rca_done", nodes.rca_done)
    g.add_node("unsupported_route", nodes.unsupported_route)
    g.add_node("dep_resolver", nodes.dep_resolver)
    g.add_node("reachability_gate", nodes.reachability_gate)
    g.add_node("coder", nodes.coder)
    g.add_node("harsh_reviewer", nodes.harsh_reviewer)
    g.add_node("open_pr", nodes.open_pr)
    g.add_node("graph_augment", nodes.graph_augment)
    g.add_node("release_intel", nodes.release_intel)
    g.add_node("automation_testing", nodes.automation_testing)   # ocean-automation-testing skill, end-to-end
    g.add_node("prep_rework", nodes.prep_rework)
    g.add_node("flip_ready", nodes.flip_ready)
    g.add_node("stop_run", nodes.stop_run)

    g.add_edge(START, "researcher")
    g.add_conditional_edges("researcher", route_after_research,
                            {"rca": "rca_agent", "coding": "dep_resolver",
                             "unsupported": "unsupported_route"})
    g.add_edge("unsupported_route", END)
    # RCA agent -> RCA Done (terminal) | Fix needed -> deps + reachability gate -> coder
    g.add_conditional_edges("rca_agent", after_rca,
                            {"done": "rca_done", "fix_needed": "dep_resolver"})
    g.add_edge("rca_done", END)

    g.add_edge("dep_resolver", "reachability_gate")
    g.add_edge("reachability_gate", "coder")
    g.add_edge("coder", "harsh_reviewer")
    g.add_conditional_edges("harsh_reviewer", after_review,
                            {"rework": "coder", "approve": "open_pr"})

    g.add_edge("open_pr", "graph_augment")
    g.add_edge("graph_augment", "release_intel")
    g.add_edge("release_intel", "automation_testing")

    # Station 6 outcomes (branch on the skill's verdict)
    g.add_conditional_edges("automation_testing", after_automation, {
        "pass": "flip_ready",
        "code_fault": "prep_rework",
        "stop": "stop_run",
    })
    g.add_edge("prep_rework", "coder")   # full loop: coder -> review -> (open_pr no-op) -> ... -> SIT
    g.add_edge("flip_ready", END)
    g.add_edge("stop_run", END)

    return g


def compile_app(checkpointer):
    """Compile the graph with a caller-supplied checkpointer.

    The checkpointer must be an ASYNC saver (the nodes are async / driven via
    ainvoke). The CLI opens an AsyncSqliteSaver as an `async with` context and
    passes it here — see cli.py. Do not create the saver here: AsyncSqliteSaver
    is an async context manager whose lifetime must span the ainvoke call.
    """
    return build_graph().compile(checkpointer=checkpointer)


# Uncompiled StateGraph exposed for LangGraph Studio / `langgraph dev` (referenced by
# langgraph.json). Studio supplies its own persistence, so hand it the builder — not a
# checkpointer-bound compile. Construction is side-effect-free (no agent calls).
graph = build_graph()
