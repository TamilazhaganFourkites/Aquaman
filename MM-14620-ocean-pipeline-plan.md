# MM-14620 — Ocean Pipeline: Re-Architecture, Gaps & Demo Plan

**Ticket:** MM-14620 (feature)
**Goal:** A LangGraph "ocean-pipeline" where **LangGraph is the sole orchestrator**, and each
graph node drives **one specific, narrow agent** to carry a ticket through its full development
cycle — reliable enough to **demo live to the team**.
**Repo:** `Aquaman` (this repo).

> **Senior's direction (the reason for this rewrite):** the pipeline must be *completely
> controlled by LangGraph*. The fk-aideveloper "station process" must **not** drive it. Today
> Aquaman is a thin wrapper that delegates the real process to fk-aideveloper's monolithic
> station agents — that is exactly what must change. This plan re-centers the design on
> **LangGraph owns the process; agents are narrow workers.**

---

## 1. The mental model (read this first)

Think of a **kitchen**:

- **LangGraph = the head chef / the recipe.** It decides *what happens, in what order, who does
  each task, when to repeat a step, and when to stop and ask a human.* All control lives here.
- **Agents = line cooks.** Each does **one narrow job** — "research this ticket," "write this
  code," "review this diff." A cook must **not** decide the order of the meal or call the next cook.

**What's wrong today:** the fk-aideveloper stations are line cooks that also try to run the
kitchen — they carry their own routing rules, decide when to call SMEs, and run their own git
commands. That "station process" means LangGraph isn't actually in charge.

**The fix:** strip each worker down to one job, and move every "who / when / repeat / stop"
decision into LangGraph nodes and edges.

---

## 2. Where the current code breaks the principle

| Current behavior | File | Why it violates "LangGraph owns the process" |
|---|---|---|
| Every node prepends `UNIVERSAL_PREFIX`: *"You are operating as part of the FK Lean Manufacturing Pipeline… Your station: X. Load context JIT… pull the ONE skill…"* | `agents.py` | That IS the fk-aideveloper station process, injected into every worker. The worker inherits process framing it shouldn't have. |
| `run_station(agent_md="fk-researcher.md")` hands a node's whole job to a monolithic station agent that self-routes, dispatches SMEs, creates feature files, writes JSON handoffs | `nodes.py`, `agents.py` | The graph doesn't control research/SME-dispatch/routing — the `.md` does. LangGraph is a passthrough. |
| `open_pr`, `graph_augment`, `release_intel`, `flip_ready` just **prompt `fk-coder.md`** to run `gh`/`git` | `nodes.py` | Deterministic git/PR operations are delegated to an LLM agent instead of being plain, testable code in the graph. |
| Station 6 is **one node driving the `ocean-automation-testing` skill end-to-end**; the skill owns its internal "Stations 0–3" (author→run→triage→PR) | `nodes.py` `automation_testing` | LangGraph can't see or control the SIT sub-steps or branch precisely — it's a black box. |
| Nodes hand off via JSON files written to `FK_AIDEVELOPER_DIR/memory/...` | throughout | Process state lives in fk-aideveloper's convention, not in LangGraph's typed state. |

**Note — what is already correct** and should be *kept*: the router edge, the review loop
(`review_iteration` ≤ 2), the code_fault loop, the checkpointer, and the runner-log/Langfuse
observability. These are genuine LangGraph-owned control — good. The problem is everything the
graph *delegates* rather than *controls*.

---

## 3. The three architecture decisions (made)

| Decision | Choice | Why (for a team new to LangGraph) |
|---|---|---|
| **Agent sourcing** | **Hybrid** — vendor slim, single-job worker prompts into `src/ocean_pipeline/agents/*.md` (research, code, review, QA, RCA); keep referencing the **ocean domain-SME knowledge** from fk-aideveloper | LangGraph fully owns the *process* and the workers (satisfies the senior, no station-process dependency) — but we don't throw away the hard-won SME domain facts (which repo owns callbacks / milestones). SMEs become *expert lookups*, not process. |
| **Git/PR ops** | **LangGraph code owns them** — `gh`/`git` run as plain Python in nodes | Deterministic, testable, no LLM variance. Easiest to debug and the clearest demonstration that LangGraph runs the pipeline. |
| **Station 6 (SIT)** | **Decompose into nodes** — `author-SIT → run-SIT → triage → open-test-PR` — phased (last) | LangGraph owns the control and branching; the mechanical test-run can still shell out. This is what makes the demo visibly LangGraph-driven. Highest effort, so do it last. |

---

## 4. Target architecture — node → specific agent (or plain code)

LangGraph owns: **routing, sequencing, all loops, retries, budgets, typed state, the human gate,
and telemetry.** Each node is either **one narrow agent** or **plain deterministic code**.

```
START
  └─ classify           [agent]  route = coding | rca | unsupported ; detect domain_bucket
        ├─ unsupported ─────────────────────────────────► stop (terminal)
        ├─ rca ─► rca_investigate [agent] ─► rca_report [code: post Jira] ─► (fix? → research | done)
        └─ coding ▼
  research              [agent]  gather Jira/GitHub/history → typed research state (NO routing, NO SME dispatch)
  sme_consult           [agent]  graph picks the SME(s) by domain_bucket, fans out, merges findings
  dependency_check      [agent]  blockers vs self-solves
  reachability_verify   [agent]  execution-verify claims → binding reachability state
  plan_units            [agent]  decompose ticket into atomic coding units (graph then maps over them)
  code                  [agent]  implement the units (edits files only)
  commit_push           [code]   git add/commit/push on the ticket branch
  review                [agent]  adversarial review → verdict
      └─(CHANGES_REQUIRED & iter<2)─► back to code      (loop owned by the graph)
      └─(APPROVE | iter≥2)─▼
  open_pr               [code]   gh pr create --draft (idempotent)
  sit_author            [agent]  author/locate the local SIT (uses ocean-qa knowledge)
  sit_run               [code]   run SIT local + mock-first (Docker/mock mechanics)
  sit_triage            [agent]  classify failures: test_fault | code_fault | could_not_verify
      └─ code_fault (attempts<budget) ─► back to code    (loop owned by the graph)
      └─ could_not_verify | budget exhausted ─► stop
      └─ passed ─▼
  open_test_pr          [code]   gh pr create --draft in cloudqwest/test-automation
  human_gate            [interrupt]  (optional) pause for engineer approval
  flip_ready            [code]   cross-link test PR + gh pr ready ; Jira → In Review + comment
  END
```

**Key differences from today:** SME dispatch, unit decomposition, and all git/PR ops are now
**graph-owned nodes**, not things buried inside a station agent. Station 6 is four nodes, not one
black box. Workers receive **explicit typed state**, not a "you are Station X" prefix.

---

## 5. What each vendored agent becomes (the "one job" contract)

Each lives in `src/ocean_pipeline/agents/<name>.md` — a **short** prompt: one job, explicit inputs
from state, structured output. No station framing, no "load your skill," no self-routing.

| Agent | One job | Input (from state) | Output (to state) |
|---|---|---|---|
| `classify` | Decide route + domain bucket | ticket text | `route`, `domain_bucket` |
| `research` | Gather context, summarize | ticket, context | `research` (typed packet) |
| `sme_consult` | Answer "which repo/file owns X" | domain_bucket, question | `sme_findings` — *reuses fk-aideveloper SME knowledge* |
| `dependency_check` | List blockers vs self-solves | research | `dependencies` |
| `reachability_verify` | Prove/refute each claim | research, dependencies | `reachability` (binding) |
| `plan_units` | Split into atomic coding units | research, reachability | `units[]` |
| `code` | Implement one unit (edits only) | unit, reachability | files changed |
| `review` | Adversarial review of the diff | branch/diff | `verdict`, `findings[]` |
| `sit_author` | Author/locate the SIT | changed repo, diff | SIT path |
| `sit_triage` | Classify SIT failures | test results | `failure_class`, `findings_for_coder[]` |
| `rca_investigate` | Evidence-cited root cause | ticket | `rca_findings`, `fix_needed` |

Everything else (`commit_push`, `open_pr`, `sit_run`, `open_test_pr`, `flip_ready`, `rca_report`
Jira post) is **plain Python** — no agent.

---

## 6. Migration plan (phased, incremental — never a big-bang rewrite)

> Do this as small, testable steps. The control-flow test suite (`tests/test_graph.py`) must stay
> green after every phase, since it mocks the agents and asserts the *graph* — exactly what we're
> making authoritative.

### Phase A — Take ownership of the process (no new agents yet) 🔴
1. **Delete the station framing.** Remove `UNIVERSAL_PREFIX`; rename `run_station` → `run_agent`;
   pass only a node-scoped prompt + explicit state. (`agents.py`)
2. **Move git/PR ops into plain code nodes.** Rewrite `open_pr`, `flip_ready` (and drop/absorb
   `graph_augment`, `release_intel` if not needed) as Python running `gh`/`git` with idempotency.
   No agent call. (`nodes.py`)
3. **Reliability:** add node-level retry/backoff (LangGraph `RetryPolicy`) and fix the SDK
   `ResultMessage` handling so a benign `success` envelope isn't treated as fatal (this is the
   §9.1 crash). (`agents.py`, `graph.py`)

### Phase B — Vendor the narrow worker agents 🟠
4. ✅ **DONE** — Vendored slim, one-job workers into `src/ocean_pipeline/workers/` (`research.md`,
   `code.md`, `review.md`), derived by trimming. `config.VENDORED_AGENTS_DIR` + `agents._agent_path`
   resolve them first, falling back to fk-aideveloper for un-migrated nodes. Workers get inputs
   inline via state, not by reading handoff files.
   - Also fixed (review): `run_agent` now uses the resolver (was a real FileNotFoundError bug);
     and the coder's clone is threaded to the reviewer via `worktree_dir` (graph-owned workspace).
5. ✅ **DONE** — `sme_consult` graph node: research classifies `domain_bucket`; the graph picks the
   ocean SME (fk-aideveloper `sme-*.md` as referenced expert knowledge) and feeds findings to the
   coder. No-op when no bucket applies. RCA path skips it.
6. ⏳ **PENDING** — Wire `telemetry._call_mcp` to the real aidev-db MCP tools (best-effort,
   non-blocking). Needs the aidev-db endpoint + a Python-side MCP transport + creds — not
   testable locally; treat as its own integration task.

### Phase C — Decompose Station 6 & add the human gate 🟡
7. ✅ **DONE** — Split `automation_testing` into `sit_resolve → sit_run → sit_triage` graph nodes
   (driving the skill one `--only` phase at a time). Graph branches at the two real decision points:
   `onboard` after resolve (before authoring/running) and the verdict after triage. Kept the
   test-automation PR-open inside the report phase (no branch → no separate node). Reconciled with
   both the `learn_repo` onboard branch and the `human_gate`. 24/24 tests green.
8. ✅ **DONE** — Optional `human_gate` `interrupt()` before `flip_ready`, env-gated
   (`OCEAN_PIPELINE_REQUIRE_APPROVAL`, default off → auto-flip). `--resume <exe> --approve|--reject`
   injects the decision. Jira lifecycle via best-effort `jira.py` (In Progress at research start;
   In Review + PR-link comment at flip; no-op without `JIRA_API_TOKEN`).
9. ✅ **DONE** — README corrected (interrupt() + telemetry claims now match reality).
10. ✅ **DONE (added on request)** — SIT **QA review gate**: `sit_author` drafts the scenarios + sample
    test and stops; `qa_review_gate` is a human **3-way** review (default ON, `interrupt()`; auto via
    `OCEAN_PIPELINE_QA_AUTOAPPROVE`) — `approve-testrail` / `approve-no-testrail` / `changes` (loops back
    to redraft). On `approve-testrail`, `sit_testrail` writes the TestRail cases **in parallel** with the
    local run (TestRail's API is slow/rate-limited). CLI: `--resume <exe> --qa <choice> [--note …]`.

### Phase D — Demo hardening ❌ DROPPED (team wants a real flow)
The team decided to demo a **real end-to-end run**, not a mock/simulated one, so the demo-safety
scaffolding is not built:
10. ~~`--mock` mode~~ — not needed; the demo runs the real pipeline.
11. ~~Golden pre-recorded run as a fallback exhibit~~ — not needed.
12. ~~Expanded preflight~~ — skipped (optional standalone nicety if ever wanted).

**Consequence to be aware of:** with no mock/golden-run safety net, the demo depends on a real run
working live. Everything is verified by the mocked control-flow suite (24 tests), but **no real
end-to-end run has exercised the workers/skill wiring since these changes**. Strongly recommend one
real rehearsal run (`ocean-pipeline MM-XXXX` on a throwaway ticket) before the demo to shake out
real-world issues — this is prudent rehearsal, not Phase-D scaffolding.

---

## 7. What "complete development cycle" means (scope lock)

Ticket → **classify** → research → SME consult → dependencies → reachability → plan units → code →
commit/push → adversarial review (loop) → draft PR → SIT author/run/triage (loop) → test PR →
**(optional human approval)** → flip service PR to ready-for-review + Jira → In Review.

**Human boundary preserved:** never auto-merge, never auto-deploy. (The old `fk-cicd-trigger` /
deploy stations were deliberately removed in TRCY-4802 — do **not** re-add them for this demo;
full CD is a separate future ticket.)

---

## 8. Demo runbook (what you show the team)

1. **"The graph is the process."** `ocean-pipeline --print-graph` → show the mermaid topology.
   Message: *"every arrow — routing, the review loop, the SIT loop, the ready-flip — is a
   deterministic state machine LangGraph controls. The agents are just workers it calls."*
2. **Fast, safe run.** `ocean-pipeline MM-XXXX --mock` → full runner log in ~60s, no side effects.
   Same topology + telemetry + Langfuse spans as a real run.
3. **Observability.** Langfuse → filter tag `aquaman` → per-node spans/timings named by ticket;
   show `run-report.md`.
4. **The real thing** (or the pre-recorded golden run): walk node by node — classify → research →
   SME consult → reachability → code pushes a branch → review APPROVE → **LangGraph opens the
   draft PR itself** → SIT author/run/triage green → **LangGraph flips the service PR to
   ready-for-review** + linked test PR; show the PRs in GitHub and the ticket in Jira.
5. **The human boundary.** *"It stops at ready-for-review — never merges, never deploys."* If the
   `human_gate` is in: show LangGraph pausing to ask a human.
6. **Resilience.** Checkpointer + `--resume` + node retries: a station hiccup retries; a crash
   resumes from the last checkpoint.

**Fallback:** if the live run stalls, switch to the golden run's report + Langfuse trace.

---

## 9. Other gaps carried from the reliability review

### 9.1 🔴 A real run currently crashes on the first agent hiccup
`run.log` shows the last real run died at `dep_resolver`:
`StationError: … agent run failed: Exception: Claude Code returned an error result: success`.
No node-level retry; a benign SDK `success` envelope is surfaced as fatal. **Fixed in Phase A.3.**

### 9.2 🟠 Telemetry to aidev-db is a no-op stub
`telemetry._call_mcp` is a `TODO`. Aquaman runs don't reach the team's dashboards. **Phase B.6.**

### 9.3 🟠 README claims a human `interrupt()` the code doesn't do
Doc/behavior drift. **Fixed in Phase C (add the gate) + C.9 (fix the doc).**

### 9.4 🟡 No Jira lifecycle transitions
Only RCA posts a comment. Add In Progress / In Review touchpoints. **Phase C.8.**

---

## 10. Success criteria (definition of done for the demo)

_Updated 2026-07-28 against the current code (not just the phase checkmarks above) — see the note
on each item for what was actually checked before ticking it._

- [x] `--print-graph` shows SME-consult, git/PR ops, and the four SIT nodes as **graph nodes** (proof LangGraph owns the process). _`graph.py`'s `build_graph()` registers `sme_consult`, `open_pr`/`flip_ready` (plain code), and `sit_resolve`/`sit_author`/`sit_run`/`sit_triage` (+ `sit_testrail`/`learn_repo`) as real nodes; `--print-graph` renders `build_graph().compile().get_graph().draw_mermaid()` directly from that live graph._
- [ ] No `UNIVERSAL_PREFIX` / "Station X" framing reaches any agent; workers get typed state only. _Partial: the old `UNIVERSAL_PREFIX` boilerplate itself is gone, but several `nodes.py` task prompts still say "Station 0 (resolve)", "Station 5 APPROVED", etc. as human-readable phase labels sent to the ocean-automation-testing skill. Left unchecked pending a decision on whether that counts as the framing this item means to exclude._
- [x] `open_pr` / `flip_ready` run as plain code (no agent), and are idempotent + unit-tested. _Confirmed: both are plain `gitops.py` calls with no `run_agent`; `open_draft_pr`'s idempotent reuse and `cross_link_and_ready`'s idempotent link-dedup are directly unit-tested (`tests/test_graph.py`)._
- [ ] Node-level retry proven — a single transient agent error no longer aborts the run. _`_drive_with_retry` exists and is used by every `run_agent`/`run_skill` call, but no test directly proves a transient failure is retried-and-recovered rather than propagated. Left unchecked until that test exists._
- [ ] `ocean-pipeline --mock MM-XXXX` completes the full graph in <90s with zero side effects. _No `--mock` flag exists in `cli.py` today — not yet implemented, not merely unverified._
- [ ] One real run completes end-to-end; its `run-report.md` + Langfuse trace archived (golden run). _Requires an actual live run artifact — can't be verified from source; needs a human to confirm/attach one._
- [x] `tests/test_graph.py` green after every phase; new nodes covered. _73/73 passing as of this update; coverage has grown substantially (gitops.py, jira.py, telemetry status mapping, the PreToolUse allowlist, cli.py's preflight/resume paths are now directly tested, not just exercised via the full-graph mocks)._
- [x] README has no false claims (interrupt / telemetry). _Verified both claims are real, not aspirational: `human_gate`'s `interrupt()` exists and is exercised by `test_human_gate_interrupts_then_resume_approves`; `telemetry.py`'s `_call_async` is a real `mcp.client.streamable_http` call against the aidev-db MCP server, not a stub._
- [ ] Demo runbook (§8) rehearsed once end-to-end. _Requires a human to actually run it — can't be verified from source._

---

## 11. Appendix — agents/skills the pipeline will call

**Vendored into Aquaman (owned by LangGraph):** `research`, `sme_consult` wrapper, `dependency_check`,
`reachability_verify`, `plan_units`, `code`, `review`, `sit_author`, `sit_triage`, `rca_investigate`
(`src/ocean_pipeline/agents/*.md`).

**Referenced from fk-aideveloper as expert knowledge (NOT process):**
`sme-callback-notification`, `sme-load-creation`, `sme-ocean-milestones` (ocean domain facts);
`ocean-qa-agent` (SIT authoring conventions); `ocean-rca` references.

**Plain code (no agent):** `commit_push`, `open_pr`, `sit_run`, `open_test_pr`, `flip_ready`,
`rca_report` Jira post — all git/gh/Jira/Docker mechanics.

**Do NOT depend on:** `skills/fk-execute` and the fk-aideveloper "station process" — that
orchestration now lives entirely in the LangGraph graph.
```
