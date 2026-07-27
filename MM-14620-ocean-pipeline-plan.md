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
4. Create `src/ocean_pipeline/agents/*.md` — slim, one-job prompts (§5). Start with `research`,
   `code`, `review`; migrate one node at a time, each behind a green test run.
5. **Make SME dispatch a graph node.** Pull SME selection out of research; add a `sme_consult`
   node that the graph routes to by `domain_bucket`, calling the fk-aideveloper SME agents as
   *expert lookups*.
6. Wire `telemetry._call_mcp` to the real aidev-db MCP tools (best-effort, non-blocking).

### Phase C — Decompose Station 6 & add the human gate 🟡
7. Split `automation_testing` into `sit_author` / `sit_run` / `sit_triage` / `open_test_pr` nodes;
   the graph owns the triage branch and the code_fault loop.
8. Add the optional `human_gate` `interrupt()` before `flip_ready` (env-gated); add Jira
   lifecycle touchpoints (In Progress at start, In Review + PR-link comment at flip).
9. Update `README.md` — remove the stale "interrupt()" and "telemetry done" claims.

### Phase D — Demo hardening 🔴 (can start in parallel with A)
10. **`--mock` mode:** run the *real graph* with canned agent outputs from a fixture (productize
    what `tests/test_graph.py` already does). 60-second, side-effect-free, deterministic demo run.
11. Record one **golden real run** end-to-end; archive its `run-report.md` + Langfuse trace as a
    fallback exhibit.
12. Expand preflight: `gh auth`, Docker daemon, MCP reachability, Langfuse creds — fail fast
    before a demo, not mid-run.

**Sequencing for the demo:** Phases A + D are the minimum for a compelling, safe live demo
(LangGraph visibly owns the process + git ops, and `--mock` makes it reproducible). B and C deepen
the "fully LangGraph-controlled" story and can land after the first demo.

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

- [ ] `--print-graph` shows SME-consult, git/PR ops, and the four SIT nodes as **graph nodes** (proof LangGraph owns the process).
- [ ] No `UNIVERSAL_PREFIX` / "Station X" framing reaches any agent; workers get typed state only.
- [ ] `open_pr` / `flip_ready` run as plain code (no agent), and are idempotent + unit-tested.
- [ ] Node-level retry proven — a single transient agent error no longer aborts the run.
- [ ] `ocean-pipeline --mock MM-XXXX` completes the full graph in <90s with zero side effects.
- [ ] One real run completes end-to-end; its `run-report.md` + Langfuse trace archived (golden run).
- [ ] `tests/test_graph.py` green after every phase; new nodes covered.
- [ ] README has no false claims (interrupt / telemetry).
- [ ] Demo runbook (§8) rehearsed once end-to-end.

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
