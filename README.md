# Aquaman

**MM-14615 — LangGraph orchestrator for the Ocean/MM (isbu) lean manufacturing pipeline.**

Aquaman is the deterministic, code-driven replacement for the `fk-execute` prose
"floor captain" checklist, for the Ocean/MM (`isbu`) boards only. The pipeline
control flow — station sequence, the RCA router, the review loop, the ready-flip
gate — is a LangGraph state machine. The *work inside each node* is still done by
the existing station agents in `fk-aideveloper/agents/pipeline/fk-*.md`, invoked
as Claude Agent SDK subprocesses. This repo owns orchestration; it does not
re-express any station logic.

## Why LangGraph

| Pipeline feature (CLAUDE.md) | LangGraph primitive |
|---|---|
| Fixed sequence 0 → 1 → 1.5 → 4 → 5 → 6 | linear edges |
| Router: RCA → stop vs coding → continue | conditional edge (`route_after_research`) |
| Review loop 5↔4, **max 2 iterations** | conditional edge + `review_iteration` counter |
| Never auto-merge; engineer owns the ready-flip | `interrupt()` before `gh pr ready` |
| Telemetry START/END + station events | node hooks in `telemetry.py` |
| AP-223 orphaned `running` rows | SQLite checkpointer → resume, not orphan |
| Language-scoped Docker (ruby=docker, java/go=native) | carried per-repo in `state["target_repos"]` |

## Graph

```
START → researcher ─┬─(rca)────→ rca_agent ─┬─(no fix)──────────→ rca_done → END
                    │                        └─(fix needed)──┐
                    └─(coding)──────────────────────────────►├─→ dep_resolver → reachability_gate → coder ◄─┐
                                                                                                     │       │
                                                                                              harsh_reviewer  │
                                                          ┌──(CHANGES_REQUIRED & review_iter<2)───────────────┘
                                                          └──(APPROVE | review_iter≥2)→ open_pr* → graph_augment
                                                                → release_intel → automation_testing
                                                                                          │
                    ┌── code_fault & coding_attempts<budget → prep_rework ────────────────┤
                    │                                                                      │
   coder ◄──────────┘                                                                      │
                              passed → flip_ready (raise test PR + link it in service ─────┤ → END
                                        PR, gh pr ready)                                    │
                              could_not_verify | budget exhausted → stop_run ──────────────┘ → END
```
`open_pr*` is idempotent — the code_fault loop re-enters it as a no-op since the service PR is already open.
An RCA that concludes **Fix needed** joins the coding pipeline at `dep_resolver`, so the fix gets the same
dependency resolution and reachability gating as any coding ticket.

Station 6 is a single node driving the `ocean-automation-testing` skill end-to-end (headless). Its internal
`write → run → pass` stages (the design diagram's boxes) belong to the skill (SKILL.md Stations 0–3), which
owns their sequencing and failure handling and persists state to
`memory/tickets/<TICKET>-automation-testing.json`. The node reads that verdict and the graph branches on it;
a Docker/infra bring-up failure is the skill's own `could_not_verify`. `flip_ready` then cross-links the test
PR into the service PR and flips it to ready.

## Layout

```
src/ocean_pipeline/
├── state.py       OceanState TypedDict threaded through every node
├── schemas.py     per-station verdict models (agents write <station>.verdict.json)
├── config.py      paths, model, MAX_REVIEW_ITERATIONS, isbu project set
├── agents.py      run_station(): drives an fk-*.md agent via Claude Agent SDK
├── telemetry.py   aidev_db START/END + station-event hooks
├── nodes.py       one node per station (thin wrappers)
├── graph.py       StateGraph wiring: router, review loop, ready-flip
└── cli.py         `ocean-pipeline <TICKET>` — the /fk-execute replacement
```

## Setup

```bash
pip install -e .
export FK_AIDEVELOPER_DIR=/path/to/fk-aideveloper   # source of the station agents
export ANTHROPIC_API_KEY=...                          # or `ant auth login`
```

Prerequisites: the Claude Agent SDK spawns the `claude` CLI, so Claude Code must be
installed and authenticated on the host. The pipeline runs fully headless
(`permission_mode="bypassPermissions"`) so stations can push branches and open PRs
unattended — run it in a trusted environment (or set `OCEAN_PIPELINE_PERMISSION_MODE`
+ a tool allowlist to tighten). The checkpointer uses an async SQLite saver
(`AsyncSqliteSaver`, backed by `aiosqlite`) at `OCEAN_PIPELINE_CHECKPOINT_DB`.

## Run

```bash
ocean-pipeline MM-14615
ocean-pipeline MM-14615 --context "Bug only affects SCAC=ABCD loads"
ocean-pipeline --resume EXE-1a2b3c4d      # continue a crashed run from its last checkpoint
ocean-pipeline --print-graph              # mermaid diagram straight from the compiled graph
```

Node completions stream to the console as the run proceeds. The pipeline **never
auto-merges or auto-deploys**; on SIT PASS it flips the service PR to ready-for-review
automatically. The engineer still owns final merge, sign-off, and deploy.

## Tests

```bash
pip install -e '.[test]'
pytest
```

`tests/test_graph.py` mocks the station agents and asserts every graph path
deterministically (happy path, RCA no-fix / RCA-fix-through-gates, review-loop cap,
Station-6 code_fault loop + budget exhaustion, could_not_verify, unsupported route) —
no API key, no `claude` CLI, milliseconds to run. The `claude_agent_sdk` import is lazy
so the suite needs only `langgraph` + `pydantic`.

## Station 6 — ocean-automation-testing

The `automation_testing` node drives the `ocean-automation-testing` skill headless
(`agents.run_skill`), which resolves the changed repo, gates on the Station-5 APPROVED
verdict, authors/locates the SIT via `ocean-qa-agent`, runs it **local + mock-first**
(only the changed ocean repo runs locally under its language-scoped build env; the rest
are mocked), opens the test-automation draft PR on pass, and writes its verdict to
`memory/tickets/<TICKET>-automation-testing.json`. The node reads that file and the
graph branches on it:

| Skill verdict | Graph action |
|---|---|
| `automation_result: passed` | `flip_ready`: cross-link the test PR into the service PR, `gh pr ready` → **completed** |
| `failed` + `code_fault` (attempts < `MAX_CODING_ATTEMPTS`) | `prep_rework` → **full loop**: coder → review → open_pr(no-op) → SIT, carrying `findings_for_coder` |
| `failed` + `code_fault` (budget exhausted) | `stop_run` → **failed**, service PR left draft |
| `failed` + `could_not_verify` | `stop_run` → **failed**, no loop (no isolated defect to hand back) |

It never merges or deploys, and it never flips the *service* PR itself — that flip is the
orchestrator's `flip_ready` node.

## Status

MM-14615: full node/edge topology, checkpointer, telemetry hooks, the Claude Agent SDK
bridge, the RCA→fix→coder branch, the review loop, and **Station 6 (ocean-automation-testing)
with the code_fault full-loop** are all in place. The one remaining hook is the aidev-db MCP
transport in `telemetry.py` (`_call_mcp`), left thin so the transport is swappable.
