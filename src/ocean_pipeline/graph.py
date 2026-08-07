"""Graph wiring. The control flow that the fk-execute prose checklist described
is now deterministic edges:

  - the RCA router,
  - MM-14738: TDD-style test authoring -- `qa_scenarios` GAN-hardens the SIT test scenarios right
    after `reachability_gate`, BEFORE `coder` runs, so the coder builds against a fixed, adversarially
    -hardened spec. `sit_author` (Station 6b, still post-review) writes the actual pytest from that
    artifact instead of designing scenarios itself; a code_fault rework re-enters at `coder` and never
    re-runs `qa_scenarios`.
  - the Station 5 <-> Station 4 review loop (capped at MAX_REVIEW_ITERATIONS),
  - the Station 6 outcomes: PASS -> flip service PR ready; code_fault -> FULL loop
    back through coder -> review -> Station 6 (capped at MAX_CODING_ATTEMPTS);
    unsupported ocean repo -> learn_repo (graph-owned onboarding) -> re-run Station 6
    (capped at MAX_ONBOARD_ATTEMPTS); AGENT-DIAGNOSED environment_failure -> prep_env_retry
    -> re-run sit_run ONLY, not the full loop (capped at MAX_ENV_RETRY_ATTEMPTS) -- the
    deterministic resource-insufficient preflight short-circuit never retries here, since
    more Docker memory doesn't appear between attempts; could_not_verify / exhausted budget
    -> stop (service PR left draft).
"""
from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from . import config, nodes, schemas
from .state import OceanState


def route_after_research(state: OceanState):
    """Returns the next node NAME(s). On the coding route with PARALLEL_ANALYSIS on, returns a LIST
    so LangGraph fans out to sme_consult ∥ dep_resolver ∥ prep_image in one superstep (they are
    independent; reachability_gate joins them). Off -> the sequential chain via sme_consult."""
    r = state.get("route")
    # RCA-only run (control-plane "In RCA" stage): always take the analysis path, even if
    # the researcher leaned "coding" — the fix is a separate later run ("RCA Done").
    if config.RCA_ONLY and r != "unsupported" and r in ("rca", "coding"):
        return "rca_agent"
    if r == "rca":
        return "rca_agent"
    if r == "coding":
        return ["sme_consult", "dep_resolver", "prep_image"] if config.PARALLEL_ANALYSIS else "sme_consult"
    # sop / loft / ff_onboarding / unclassified are handled by other harnesses, not this
    # Ocean pipeline — stop cleanly rather than silently coding a non-coding ticket.
    return "unsupported_route"


def after_rca_review(state: OceanState) -> str:
    # Human reviewed the RCA report (already posted as a Jira comment) at rca_review_gate.
    # Reject -> stop cleanly, before any coding starts. Approve -> the same routing as before:
    # RCA Done (terminal report) | Fix needed -> deps+reachability -> coder. In RCA-only mode
    # we always stop at the report; the fix is a separate "RCA Done" run.
    if str(state.get("rca_approval_decision", "")).lower().startswith("reject"):
        return "reject"
    # Finding 3 (judge follow-up): never start autonomous CODING off an RCA that failed its own
    # quality gate. rca_report refused to post it (missing sections / no INDEPENDENT STATUS CHECK),
    # so its root cause is exactly the "reads as complete but skipped its own falsification steps"
    # artifact the gate exists to catch — routing that into dep_resolver -> coder would build on it.
    # "done" (rca_done) then reports the non-delivery honestly rather than claiming success.
    if state.get("rca_report_gate_problems"):
        return "done"
    # Same reasoning, one step further out (judge review): a report the gate could not CHECK is not a
    # checked report. When FK_AIDEVELOPER_DIR lacks the checker, rca_report fails OPEN — correct, a
    # broken checker must not block a legitimate RCA from reaching Jira — but that leaves
    # gate_problems empty, which used to read exactly like a clean pass here and routed an UNVERIFIED
    # root cause straight into autonomous coding whenever RCA_REVIEW_AUTO was on (headless: no human
    # ever sees it). Posting unverified is a defensible risk; CODING off unverified is not, so the
    # fail-open stops at the Jira comment. rca_done then says so explicitly.
    if state.get("rca_report_unverified"):
        return "done"
    if config.RCA_ONLY:
        return "done"
    return "fix_needed" if state.get("rca_fix_needed") else "done"


def _has_blocking_findings(state: OceanState) -> bool:
    """F3 (architecture review): any UNRESOLVED CRITICAL/MAJOR in the last review.

    Uses schemas.is_blocking_finding — the SAME predicate ReviewVerdict's F2 gate uses, imported
    rather than re-implemented. It used to be a second copy of the same inline string comparison, and
    a review proved that made the two gates fail together on one malformed severity string instead of
    backstopping each other (see the canonicalizer's own comment in schemas.py)."""
    return any(schemas.is_blocking_finding(f) for f in (state.get("review_findings") or []))


def after_review(state: OceanState) -> str:
    if state.get("review_verdict") == "APPROVE":
        return "approve"
    if state.get("review_iteration", 0) >= config.MAX_REVIEW_ITERATIONS:
        # Review budget spent. If the coder actually produced a diff, stop looping and open the
        # PR anyway -- Station 6 (SIT) is the next gate, and a code_fault there still triggers
        # rework. But if there's NO diff (coder refused/failed to write any code -- e.g. a
        # genuinely blocked ticket, branch left blank), "approving" is nonsensical: it would route
        # straight to open_pr, which has nothing to open a PR for and crashes with a GitOpError.
        # Stop cleanly instead (EXE-342a6243/MM-14475: two blocked coder attempts, two explicit
        # reviewer escalations to a human, but budget-exhaustion silently routed to open_pr anyway
        # and crashed). Checking just `branch`, not `files_changed`, mirrors open_pr's own guard
        # (`if not slugs or not branch: raise GitOpError`) -- the same signal the rest of the
        # graph already treats as authoritative for "is there something to act on."
        if state.get("branch"):
            # F3 (architecture review): budget exhausted WITH a diff. This used to ALWAYS open the PR
            # (Station 6 is the next gate). But if the FINAL review still holds UNRESOLVED CRITICAL/MAJOR
            # findings, "approve" would ship a PR the reviewer explicitly rejected — the "review-budget
            # exhaustion routes to approval" escape (the class that let MM-14457's issues through).
            # Escalate to a human (stop_run records it, no PR) when blocking findings remain; only open
            # the PR when the residual is at most MINOR. (Composes with F2: the last verdict is already
            # CHANGES_REQUIRED whenever CRITICAL/MAJOR exist, so this never contradicts an honest APPROVE.)
            if _has_blocking_findings(state):
                return "stop"
            # Mirror open_pr's FULL guard, not half of it. open_pr raises GitOpError on
            # `not slugs or not branch`, but this only checked `branch` — so a branch-set/slugs-empty
            # state routed to "approve" and crashed there instead of stopping cleanly. Near-unreachable
            # today (the coder reports repo and branch together), but the asymmetry is the kind that
            # becomes reachable the moment either side is edited (judge review).
            if not nodes._service_slugs(state):
                return "stop"
            return "approve"
        return "stop"
    return "rework"


def after_reachability(state: OceanState) -> str:
    # MM-14816 (G20): if the resolver surfaced an AC-blocking product/UX open question, route (BEFORE the
    # ~20-min GAN and the coder) to the blocked_review_gate — a HUMAN decides in the Monitor UI whether to
    # answer/post-to-Jira/reject (the outward Jira post is never automatic). reachability short-circuited
    # its own work at entry for this case. Match stop_run's stripped filter so an all-whitespace list
    # can't route to the gate. Otherwise proceed to the GAN as today.
    if any(str(q).strip() for q in (state.get("blocking_open_questions") or [])):
        return "blocked"
    return "proceed"


def after_blocked_review(state: OceanState) -> str:
    # MM-14816 (G20): the human's Monitor-UI decision on the open questions.
    #   post   → stop_run (blocked branch posts to Jira + cli emits the oas-autodev marker → AWAITING_INPUT)
    #   answer → loop back to reachability_gate (block cleared, answers carried) → runs fully → coding
    #   reject → loop back to reachability_gate (block cleared, questions carried as caveats) → coding
    return "post" if state.get("blocked_decision") == "post" else "continue"


def after_sit_resolve(state: OceanState) -> str:
    # Onboarding is detected at resolve, before any authoring/execution: branch to learn_repo
    # (capped), stop cleanly if the budget is spent, else run the SIT.
    if state.get("needs_onboarding"):
        return "onboard" if state.get("onboard_attempts", 0) < config.MAX_ONBOARD_ATTEMPTS else "stop"
    return "run"


def after_qa_review(state: OceanState):
    # Human 3-way review of the drafted SIT (or auto-approved). "changes" redrafts (capped);
    # approve-with-TestRail fans out to run + TestRail in parallel; approve-without-TestRail just runs.
    # FIX A2-1: the HEADLESS DEFAULT is approve_no_testrail (config QA_TESTRAIL off) -> sit_run ONLY, so
    # sit_testrail never runs. That path is now SAFE because sit_run itself materializes the authored
    # scenario payload templates BEFORE pytest (nodes.sit_run), so test-data no longer depends on the
    # TestRail branch running. No routing change is needed here — the fix is that sit_run owns test-data on
    # BOTH the testrail and no-testrail paths (making it racy-fan-out-proof: the template synthesis is
    # ordered before pytest inside sit_run, not concurrently in the parallel sit_testrail branch).
    d = state.get("qa_decision")
    if d == "changes" and state.get("qa_review_iteration", 0) < config.MAX_QA_REVIEW_ITERATIONS:
        return "sit_author"
    if d == "approve_testrail":
        return ["sit_run", "sit_testrail"]
    return "sit_run"   # approve_no_testrail, or "changes" budget exhausted -> proceed without TestRail


def after_sit_triage(state: OceanState) -> str:
    if state.get("automation_result") == "passed":
        # Finding 2c (architecture review of PR Aquaman#4/fk-aideveloper#294): a Rung-0 "trivial
        # green" pass (schemas.AutomationVerdict.fidelity_rung — the SUT errored before ever touching
        # the receiving service, so the assertion held by inaction, not by real signal) must not
        # silently reach flip_ready the same way a genuine pass does. Route it through stop_run
        # instead, same as could_not_verify: left in draft, needs a human, never looped/retried (a
        # fidelity problem isn't something retrying the SAME mock-first run would fix).
        if state.get("fidelity_rung", 0) == 0:
            return "stop"
        return "pass"
    # failed:
    # A late-surfaced unsupported repo the graph can onboard, then re-run Station 6 (capped).
    if (state.get("needs_onboarding")
            and state.get("onboard_attempts", 0) < config.MAX_ONBOARD_ATTEMPTS):
        return "onboard"
    if (state.get("failure_class") == "code_fault"
            and state.get("coding_attempts", 0) < config.MAX_CODING_ATTEMPTS):
        return "code_fault"        # re-enter the full coder -> review -> SIT loop
    # environment_failure: only an AGENT-DIAGNOSED harness/infra issue retries (image rebuild, fresh
    # infra bring-up may fix it) -- sit_run only, not the full coder loop, since this isn't a code
    # problem. The deterministic resource-insufficient preflight short-circuit (preflight_failed=True)
    # NEVER retries here: more Docker memory doesn't appear between attempts, so retrying that specific
    # case is pure waste, not a fix (see nodes.py::_docker_preflight_reason).
    if (state.get("failure_class") == "environment_failure"
            and not state.get("preflight_failed")
            and state.get("env_retry_attempts", 0) < config.MAX_ENV_RETRY_ATTEMPTS):
        return "environment_failure"
    return "stop"                  # could_not_verify, environment_failure (non-retriable or
                                    # exhausted), code_fault exhausted, or onboarding exhausted


def after_human_gate(state: OceanState) -> str:
    # Off (default) or approved -> flip; an explicit reject on resume -> stop (PR left draft).
    return "reject" if str(state.get("approval_decision", "")).lower().startswith("reject") else "approve"


def build_graph():
    g = StateGraph(OceanState)

    g.add_node("researcher", nodes.researcher)
    g.add_node("sme_consult", nodes.sme_consult)
    if config.PARALLEL_ANALYSIS:
        g.add_node("prep_image", nodes.prep_image)               # #2: pre-warm the Ruby image (parallel path only)
    g.add_node("rca_agent", nodes.rca_agent)
    g.add_node("rca_report", nodes.rca_report)                    # plain-code: post the RCA report to Jira (one comment)
    g.add_node("rca_review_gate", nodes.rca_review_gate)          # human review of the posted RCA before acting on it
    g.add_node("rca_done", nodes.rca_done)
    g.add_node("unsupported_route", nodes.unsupported_route)
    g.add_node("dep_resolver", nodes.dep_resolver)
    g.add_node("reachability_gate", nodes.reachability_gate)
    g.add_node("blocked_review_gate", nodes.blocked_review_gate)  # MM-14816 (G20): human Monitor-UI gate before any Jira post
    g.add_node("qa_scenarios", nodes.qa_scenarios)   # MM-14738: GAN-hardened test scenarios, pre-code
    if config.PERSISTENT_CONTAINER:
        g.add_node("prep_container", nodes.prep_container)        # #1: start ONE shared test container
    if config.PERSISTENT_CONTAINER or config.WARM_SIT_INFRA:
        g.add_node("teardown_container", nodes.teardown_container)  # #1/#6: remove run-scoped Docker at end
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
    g.add_node("prep_env_retry", nodes.prep_env_retry)
    g.add_node("human_gate", nodes.human_gate)                   # optional approval before ready-flip
    g.add_node("flip_ready", nodes.flip_ready)
    g.add_node("stop_run", nodes.stop_run)

    g.add_edge(START, "researcher")
    # Coding route consults the ocean SME (graph-owned dispatch) before the gates. route_after_research
    # returns node NAMES directly (a list on the parallel coding path); the list below is the set of
    # possible destinations for graph validation/visualization.
    _research_dests = ["rca_agent", "sme_consult", "unsupported_route"]
    if config.PARALLEL_ANALYSIS:
        _research_dests += ["dep_resolver", "prep_image"]
    g.add_conditional_edges("researcher", route_after_research, _research_dests)
    # With the persistent container ON, boot the shared container BEFORE reachability (not after) so
    # reachability's Docker probes reuse it too — not just coder/review/SIT. reachability_gate already
    # emits the reuse directive (nodes._container_directive), it just needs the container to exist by
    # then; otherwise it boots its OWN ticket-scoped container that then leaks and doubles the boot of
    # an infra-heavy Ruby repo (EXE-0417bc97: `ocean-mmcuw-MM-14457` ran the whole time alongside the
    # coder's `ocean-…-EXE…`). So the fan-out / dep_resolver join at prep_container, which feeds
    # reachability_gate; teardown_container then reaps the ONE shared container. OFF: join straight at
    # reachability_gate (unchanged).
    _analysis_join = "prep_container" if config.PERSISTENT_CONTAINER else "reachability_gate"
    if config.PARALLEL_ANALYSIS:
        # Fan-out sme ∥ dep ∥ prep_image (one superstep) -> join at _analysis_join. Same-superstep
        # fan-in is LangGraph's safe barrier: the join node runs ONCE, after all three complete, and
        # reachability_gate (downstream, single edge) therefore also runs exactly once.
        g.add_edge("sme_consult", _analysis_join)
        g.add_edge("prep_image", _analysis_join)
        # dep_resolver -> _analysis_join is added once below (shared with the RCA fix path).
    else:
        g.add_edge("sme_consult", "dep_resolver")   # original strictly-sequential baseline
    g.add_edge("unsupported_route", END)
    # RCA agent -> human review gate -> RCA Done (terminal) | Fix needed -> deps + reachability
    # gate -> coder. Reject at the gate -> stop, before any coding starts.
    g.add_edge("rca_agent", "rca_report")
    g.add_edge("rca_report", "rca_review_gate")
    g.add_conditional_edges("rca_review_gate", after_rca_review,
                            {"reject": "stop_run", "done": "rca_done", "fix_needed": "dep_resolver"})
    g.add_edge("rca_done", END)

    g.add_edge("dep_resolver", _analysis_join)
    if config.PERSISTENT_CONTAINER:
        g.add_edge("prep_container", "reachability_gate")   # prep_container (shared boot) -> reachability_gate
    # reachability_gate -> qa_scenarios -> coder, regardless of PERSISTENT_CONTAINER (the only thing
    # that toggle changes is whether prep_container sits in front of reachability_gate, above).
    # MM-14816 (G20): conditional — a blocking product/UX open question routes to the blocked_review_gate
    # (human decides in the Monitor UI, BEFORE the GAN/coder); otherwise proceed to qa_scenarios as today.
    g.add_conditional_edges("reachability_gate", after_reachability,
                            {"proceed": "qa_scenarios", "blocked": "blocked_review_gate"})
    # The human's 3-way decision: post → stop_run (posts to Jira + teardown below, no leak); answer/reject
    # → loop back to reachability_gate (block now cleared → runs fully → coding), answers/caveats carried.
    g.add_conditional_edges("blocked_review_gate", after_blocked_review,
                            {"post": "stop_run", "continue": "reachability_gate"})
    # MM-14738: GAN-hardened test scenarios are designed pre-code, right after reachability -- the
    # fixed target `coder` must satisfy. A code_fault rework re-enters at `coder` (below) and never
    # loops back through `qa_scenarios`, by construction (this is qa_scenarios' only outbound edge).
    g.add_edge("qa_scenarios", "coder")
    g.add_edge("coder", "harsh_reviewer")
    g.add_conditional_edges("harsh_reviewer", after_review,
                            {"rework": "coder", "approve": "open_pr", "stop": "stop_run"})

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
        "environment_failure": "prep_env_retry",  # agent-diagnosed harness/infra issue, sit_run only
        "stop": "stop_run",
    })
    g.add_conditional_edges("human_gate", after_human_gate,
                            {"approve": "flip_ready", "reject": "stop_run"})
    g.add_edge("learn_repo", "sit_resolve")   # re-resolve now that the repo is (being) onboarded
    g.add_edge("prep_rework", "coder")   # full loop: coder -> review -> (open_pr no-op) -> ... -> SIT
    # environment_failure retry re-enters at sit_run ONLY (not sit_resolve/sit_author/coder) --
    # authoring + human review already happened and aren't implicated; only re-execution is needed.
    # A single-source trigger into sit_run alone is already a proven pattern here (after_qa_review's
    # "approve_no_testrail" path does the same, skipping sit_testrail), so this doesn't depend on any
    # unverified fan-in/join behavior at sit_triage -- the existing sit_run -> sit_triage edge fires
    # exactly as it does on a first attempt.
    g.add_edge("prep_env_retry", "sit_run")
    if config.PERSISTENT_CONTAINER or config.WARM_SIT_INFRA:
        # #1/#6: both coding-route terminals route through teardown to remove the shared container
        # and/or the warm SIT infra (idempotent no-op for whatever wasn't created). RCA/unsupported
        # terminals never made either, so they go straight to END.
        g.add_edge("flip_ready", "teardown_container")
        g.add_edge("stop_run", "teardown_container")
        g.add_edge("teardown_container", END)
    else:
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
