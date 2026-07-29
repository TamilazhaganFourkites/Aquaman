"""Graph wiring. The control flow that the fk-execute prose checklist described
is now deterministic edges:

  - the RCA router,
  - the Station 5 <-> Station 4 review loop (capped at MAX_REVIEW_ITERATIONS),
  - the Station 6 outcomes: PASS -> flip service PR ready; code_fault -> FULL loop
    back through coder -> review -> Station 6 (capped at MAX_CODING_ATTEMPTS);
    unsupported ocean repo -> learn_repo (graph-owned onboarding) -> re-run Station 6
    (capped at MAX_ONBOARD_ATTEMPTS); could_not_verify / exhausted budget -> stop
    (service PR left draft).
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


def after_rca_review(state: OceanState) -> str:
    # Human reviewed the RCA report (already posted as a Jira comment) at rca_review_gate.
    # Reject -> stop cleanly, before any coding starts. Approve -> the same routing as before:
    # RCA Done (terminal report) | Fix needed -> deps+reachability -> coder. In RCA-only mode
    # we always stop at the report; the fix is a separate "RCA Done" run.
    if str(state.get("rca_approval_decision", "")).lower().startswith("reject"):
        return "reject"
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


def after_sit_resolve(state: OceanState) -> str:
    # Onboarding is detected at resolve, before any authoring/execution: branch to learn_repo
    # (capped), stop cleanly if the budget is spent, else run the SIT.
    if state.get("needs_onboarding"):
        return "onboard" if state.get("onboard_attempts", 0) < config.MAX_ONBOARD_ATTEMPTS else "stop"
    return "run"


def after_qa_review(state: OceanState):
    # Human 3-way review of the drafted SIT (or auto-approved). "changes" redrafts (capped);
    # approve-with-TestRail fans out to run + TestRail in parallel; approve-without-TestRail just runs.
    d = state.get("qa_decision")
    if d == "changes" and state.get("qa_review_iteration", 0) < config.MAX_QA_REVIEW_ITERATIONS:
        return "sit_author"
    if d == "approve_testrail":
        return ["sit_run", "sit_testrail"]
    return "sit_run"   # approve_no_testrail, or "changes" budget exhausted -> proceed without TestRail


def after_sit_triage(state: OceanState) -> str:
    if state.get("automation_result") == "passed":
        return "pass"
    # failed:
    # A late-surfaced unsupported repo the graph can onboard, then re-run Station 6 (capped).
    if (state.get("needs_onboarding")
            and state.get("onboard_attempts", 0) < config.MAX_ONBOARD_ATTEMPTS):
        return "onboard"
    if (state.get("failure_class") == "code_fault"
            and state.get("coding_attempts", 0) < config.MAX_CODING_ATTEMPTS):
        return "code_fault"        # re-enter the full coder -> review -> SIT loop
    return "stop"                  # could_not_verify, code_fault exhausted, or onboarding exhausted


def after_human_gate(state: OceanState) -> str:
    # Off (default) or approved -> flip; an explicit reject on resume -> stop (PR left draft).
    return "reject" if str(state.get("approval_decision", "")).lower().startswith("reject") else "approve"


def build_graph():
    g = StateGraph(OceanState)

    g.add_node("researcher", nodes.researcher)
    g.add_node("sme_consult", nodes.sme_consult)
    g.add_node("rca_agent", nodes.rca_agent)
    g.add_node("rca_report", nodes.rca_report)                    # plain-code: post the RCA report to Jira (one comment)
    g.add_node("rca_review_gate", nodes.rca_review_gate)          # human review of the posted RCA before acting on it
    g.add_node("rca_done", nodes.rca_done)
    g.add_node("unsupported_route", nodes.unsupported_route)
    g.add_node("dep_resolver", nodes.dep_resolver)
    g.add_node("reachability_gate", nodes.reachability_gate)
    g.add_node("coder", nodes.coder)
    g.add_node("harsh_reviewer", nodes.harsh_reviewer)
    g.add_node("open_pr", nodes.open_pr)
    # Station 6 decomposed into graph-owned phases (drives the skill one --only phase at a time).
    g.add_node("sit_resolve", nodes.sit_resolve)                 # resolve changed repo + onboarding check
    g.add_node("sit_author", nodes.sit_author)                   # draft SIT scenarios + sample test, then STOP
    g.add_node("qa_review_gate", nodes.qa_review_gate)           # human 3-way review of the drafted SIT
    g.add_node("sit_run", nodes.sit_run)                         # execute the approved SIT (local, mock-first)
    g.add_node("sit_testrail", nodes.sit_testrail)               # create TestRail cases (parallel with sit_run)
    g.add_node("sit_triage", nodes.sit_triage)                   # parse junit, triage, verdict, open test PR
    g.add_node("learn_repo", nodes.learn_repo)                   # graph-owned onboarding of an unsupported repo
    g.add_node("prep_rework", nodes.prep_rework)
    g.add_node("human_gate", nodes.human_gate)                   # optional approval before ready-flip
    g.add_node("flip_ready", nodes.flip_ready)
    g.add_node("stop_run", nodes.stop_run)

    g.add_edge(START, "researcher")
    # Coding route consults the ocean SME (graph-owned dispatch) before the gates.
    g.add_conditional_edges("researcher", route_after_research,
                            {"rca": "rca_agent", "coding": "sme_consult",
                             "unsupported": "unsupported_route"})
    g.add_edge("sme_consult", "dep_resolver")
    g.add_edge("unsupported_route", END)
    # RCA agent -> human review gate -> RCA Done (terminal) | Fix needed -> deps + reachability
    # gate -> coder. Reject at the gate -> stop, before any coding starts.
    g.add_edge("rca_agent", "rca_report")
    g.add_edge("rca_report", "rca_review_gate")
    g.add_conditional_edges("rca_review_gate", after_rca_review,
                            {"reject": "stop_run", "done": "rca_done", "fix_needed": "dep_resolver"})
    g.add_edge("rca_done", END)

    g.add_edge("dep_resolver", "reachability_gate")
    g.add_edge("reachability_gate", "coder")
    g.add_edge("coder", "harsh_reviewer")
    g.add_conditional_edges("harsh_reviewer", after_review,
                            {"rework": "coder", "approve": "open_pr"})

    g.add_edge("open_pr", "sit_resolve")

    # Station 6, decomposed: resolve -> (onboard? | author) -> QA review gate -> run [+ TestRail] -> triage.
    g.add_conditional_edges("sit_resolve", after_sit_resolve, {
        "onboard": "learn_repo",         # unsupported repo -> onboard it first (before authoring)
        "run": "sit_author",             # draft the SIT, then the human review gate
        "stop": "stop_run",              # unsupported + onboarding budget exhausted
    })
    g.add_edge("sit_author", "qa_review_gate")
    # Human 3-way review: changes -> redraft; approve -> run; approve+TestRail -> run ‖ TestRail (parallel).
    g.add_conditional_edges("qa_review_gate", after_qa_review, {
        "sit_author": "sit_author",
        "sit_run": "sit_run",
        "sit_testrail": "sit_testrail",
    })
    g.add_edge("sit_run", "sit_triage")
    g.add_edge("sit_testrail", "sit_triage")   # parallel branch joins here (fan-in)
    g.add_conditional_edges("sit_triage", after_sit_triage, {
        "pass": "human_gate",            # PASS -> optional human-approval gate -> ready-flip
        "onboard": "learn_repo",         # late-surfaced unsupported repo
        "code_fault": "prep_rework",
        "stop": "stop_run",
    })
    g.add_conditional_edges("human_gate", after_human_gate,
                            {"approve": "flip_ready", "reject": "stop_run"})
    g.add_edge("learn_repo", "sit_resolve")   # re-resolve now that the repo is (being) onboarded
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
