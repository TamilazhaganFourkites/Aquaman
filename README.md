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

Requires **Python ≥ 3.11** (LangGraph needs ≥ 3.10). On macOS the default `python3` is
often an old 3.7/3.9 — create the venv with an explicit modern interpreter:

```bash
python3.12 -m venv .venv && source .venv/bin/activate   # NOT `python3` if that's 3.7/3.9
pip install -e .                                        # add '.[studio]' for the Studio UI
export FK_AIDEVELOPER_DIR=/path/to/fk-aideveloper       # source of the station agents
export ANTHROPIC_API_KEY=...                            # or `ant auth login`
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

## Observability

**Runner log (default) — clean, management-legible.** Every run prints a plain-English
process log: a `▶` header per station, live **milestones** for the significant actions
(clone / branch / edit / test / commit / push / PR / docker / pytest — noise suppressed), a
`✓` line with elapsed time + outcome, and structured facts (`└`). No jargon:

```
════════════════════════════════════════════════════════════════
  FK Ocean Pipeline   ·   MM-14609
  run EXE-1a2b3c4d
════════════════════════════════════════════════════════════════

▶  Research & routing
     · querying Atlassian
     · dispatching sub-agent: ocean SME
  ✓  Research & routing                        34s   → routed to coding
        └ repo: ocean-worker (ruby, docker)

▶  Coding
     · cloning the target repo
     · creating the ticket branch
     · editing exception_clearer.rb
     · installing gems (Docker)
     · committing changes
     · pushing the branch
  ✓  Coding                                  3m30s   branch pushed
        └ 3 file(s) changed

▶  Adversarial code review
  ✓  Adversarial code review                   42s   APPROVE
        └ no CRITICAL/MAJOR findings

▶  Local SIT (automation testing)
     · repointing config to local + mocks
     · starting the mock server
     · bringing up local Docker infra
     · running the SIT (pytest)
     · opening the draft PR
  ✓  Local SIT (automation testing)          6m20s   SIT passed
        └ 2/2 tests passed
        └   passed: test_eta_exception_cleared_on_pod
        └ ran ocean-worker local · mocked: tracking-service
────────────────────────────────────────────────────────────────
  RESULT: COMPLETED   ·   took 11m06s   ·   finished 23:04:48
  sit_passed; service PR #123 ready-for-review
  spend: $1.41 · 330k tokens · 74 tool calls   across 5 station runs
════════════════════════════════════════════════════════════════
```

Each station shows: a timestamped `▶` header, live `·` milestones, per-station spend
(`done — $0.52 · 140k tokens · 24 tool calls`), and a `✓` line with elapsed + facts —
including review findings spelled out (severity + summary + file) and SIT test names. The
footer totals cost / tokens / tool-calls for the whole run.

**`--verbose` (engineers) — the deep dive.** Adds each agent's raw tool calls + text on top
of the milestones, for debugging. Off by default. Per-station verdicts also land in
`$OCEAN_PIPELINE_ARTIFACTS/<EXE-id>/*.verdict.json`.

**Langfuse (self-hosted run UI) — recommended.** FourKites runs open-source Langfuse at
`https://langfuse.fourkites.com` (on FK infra), so run data stays in-house — this is the FK
alternative to LangSmith/`smith.langchain.com`, which we do **not** use. When the
`LANGFUSE_*` credentials are present, every run traces automatically — each node shows up as a
span (timing, state, tool activity) in the Langfuse UI:

```bash
pip install -e '.[langfuse]'
# creds go in ~/.fourkites-secrets.env (auto-loaded) — get keys from Luvkush:
#   LANGFUSE_PUBLIC_KEY=pk-lf-...
#   LANGFUSE_SECRET_KEY=sk-lf-...
#   LANGFUSE_BASE_URL=https://langfuse.fourkites.com
ocean-pipeline MM-14609 --verbose
# -> prints "Langfuse tracing → https://langfuse.fourkites.com"; open that UI to watch the run
```

It's fully opt-in and gated: no `langfuse` package or no creds → tracing is a silent no-op.
There is **no signup and no cloud export** — the FK Langfuse instance is self-hosted.

**LangGraph Studio (visual graph UI) — optional, needs a LangSmith account.** The graph is
exposed via `langgraph.json` (`pip install -e '.[studio]'` then `langgraph dev`). Note the
Studio *front-end* is hosted at `smith.langchain.com` and requires a LangSmith login to open,
so for FK use prefer Langfuse above. For a zero-dependency static view of the topology use
`ocean-pipeline --print-graph`.

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
