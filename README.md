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
| Never auto-merge/deploy; auto-flip to ready on green, with an optional human gate | plain-code `flip_ready`; optional `interrupt()` in `human_gate` (`OCEAN_PIPELINE_REQUIRE_APPROVAL`) |
| Telemetry START/END + station events | `telemetry.py` → aidev-db HTTP MCP (`Bearer $RCA_TOKEN`), best-effort |
| AP-223 orphaned `running` rows | SQLite checkpointer → resume, not orphan |
| Language-scoped Docker (ruby=docker, java/go=native) | carried per-repo in `state["target_repos"]` |
| Unsupported ocean repo → learn it, don't dead-stop | `learn_repo` node + conditional edge (`MAX_ONBOARD_ATTEMPTS`) |

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
                    ┌── needs_onboarding & onboard_attempts<budget → learn_repo ───────────┤
                    │       (onboards the unsupported repo, then re-enters automation_testing)
   automation_testing ◄─────┘                                                              │
                              could_not_verify | budget exhausted → stop_run ──────────────┘ → END
```
`open_pr*` is idempotent — the code_fault loop re-enters it as a no-op since the service PR is already open.
An RCA that concludes **Fix needed** joins the coding pipeline at `dep_resolver`, so the fix gets the same
dependency resolution and reachability gating as any coding ticket.

When Station 6 finds the ticket's changed repo is an ocean service it doesn't yet support locally, it does
**not** self-onboard (that would be a hidden write to the control-plane repo). It reports `needs_onboarding` +
`onboard_repo` in its verdict and stops; the graph's `learn_repo` node then owns the decision and the
persistence — it invokes the skill's *learn-a-repo mechanic* (`local_service_execution.md` Steps N1–N5) as an
authorized onboarding pass (clone → profile → commit the profile), then re-runs `automation_testing`. Capped
by `MAX_ONBOARD_ATTEMPTS`. Standalone/interactive `/ocean-automation-testing` runs still self-onboard.

Station 6 is **decomposed into graph nodes** so LangGraph owns its sequence rather than the skill running
end-to-end: `sit_resolve → sit_author → qa_review_gate → sit_run [‖ sit_testrail] → sit_triage`. Each drives
the `ocean-automation-testing` skill one `--only` phase at a time, with the skill's own
`memory/tickets/<TICKET>-automation-testing.json` carrying state between them.

- **`sit_resolve`** resolves the changed repo; an unsupported repo branches to `learn_repo` *before* any
  authoring/running.
- **`sit_author`** drafts the SIT scenarios + sample test and **stops** (no TestRail, no run).
- **`qa_review_gate`** is a **human 3-way review** (default ON) — the same choice `ocean-qa-agent` offers
  interactively, surfaced as an `interrupt()` so it works headless: `--qa approve-testrail` |
  `approve-no-testrail` | `changes` (loops back to redraft). `OCEAN_PIPELINE_QA_AUTOAPPROVE` skips the pause.
- **`sit_run`** executes the approved SIT local + mock-first; on *approve-testrail*, **`sit_testrail`** writes
  the TestRail cases **in parallel** (the API is slow + rate-limited, so it never blocks the functional run).
- **`sit_triage`** parses junit, triages (`pass` → human gate → ready-flip; `code_fault` → rework;
  `could_not_verify` → stop), and opens the test-automation PR on pass.

`flip_ready` then cross-links the test PR into the service PR and flips it to ready.

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

### 0. Do this first: fk-aideveloper

Aquaman drives fk-aideveloper's station agents/skills as subprocesses — it doesn't work
standalone. Before anything below, complete **fk-aideveloper's own README → Onboarding**
section (GitHub/`cloudqwest` org access, Claude Code CLI license, `gh auth login`,
`./scripts/dev-setup.sh --preflight`, cloning fk-aideveloper itself, `claude-setup.sh`).

Two things that repo's onboarding won't warn you about, specific to Aquaman:

- **Path mismatch:** fk-aideveloper's own onboarding clones it to `~/Dev/ai/fk-aideveloper`.
  Aquaman's default (`FK_AIDEVELOPER_DIR` in `config.py`) is `~/Documents/projects/fk-aideveloper`.
  These do **not** match — either clone fk-aideveloper to the `~/Documents/projects/` path
  instead, or explicitly `export FK_AIDEVELOPER_DIR=~/Dev/ai/fk-aideveloper` (or wherever
  you actually put it).
- **Checkout state, not just presence:** `sme_consult` reads 4 ocean SME files
  (`sme-callback-notification.md`, `sme-load-creation.md`, `sme-ocean-data-quality.md`,
  `sme-ocean-milestones.md`) from whatever branch is checked out in `FK_AIDEVELOPER_DIR` at
  run time — there's no version pinning. `origin/main` does **not** have these files yet.
  If a ticket in the `callback_notification` / `load_creation` / `ocean_data_quality` /
  `ocean_tracking_milestones` domain bucket fails at the SME-consult step, ask in
  `#fk-aideveloper` Slack which branch currently carries this work (same place fk-aideveloper's
  own README sends you for a missing `GH_TOKEN`/`RCA_TOKEN`).

### 1. Clone and install

Requires **Python ≥ 3.11** (LangGraph needs ≥ 3.10). On macOS the default `python3` is
often an old 3.7/3.9 — create the venv with an explicit modern interpreter:

```bash
git clone <this-repo-url> && cd Aquaman
python3.12 -m venv .venv && source .venv/bin/activate   # NOT `python3` if that's 3.7/3.9
pip install -e .                                        # add '.[studio]' for the Studio UI
export FK_AIDEVELOPER_DIR=/path/to/fk-aideveloper       # wherever you cloned it in step 0
export ANTHROPIC_API_KEY=...                            # or just run `claude` once — it prompts to log in
```

The Claude Agent SDK spawns the `claude` CLI, so Claude Code must be installed and
authenticated on the host (step 0 covers the license; running `claude` interactively once is
enough to log in — there's no separate `ant`/`ai` auth command). The pipeline runs fully
headless (`permission_mode="bypassPermissions"`) so stations can push branches and open PRs
unattended — run it in a trusted environment (or set `OCEAN_PIPELINE_PERMISSION_MODE`
+ a tool allowlist to tighten). The checkpointer uses an async SQLite saver
(`AsyncSqliteSaver`, backed by `aiosqlite`) at `OCEAN_PIPELINE_CHECKPOINT_DB`.

### 2. Prerequisites (env vars + external tools)

Nothing here is enforced by a preflight check beyond what `cli.py::_preflight()` already
covers (dirs exist, `claude`/`gh` on PATH, `gh auth status`) — this list is deliberately just
*documentation*, not code validation, so it stays in one place instead of scattered across
`config.py`/`jira.py`/`telemetry.py`/`tracing.py`. Every row below is a real variable read
somewhere in `src/ocean_pipeline/` — check the "Read in" column if you want the exact line.

**Required to run anything:**

| Variable | Default | Read in |
|---|---|---|
| `FK_AIDEVELOPER_DIR` | `~/Documents/projects/fk-aideveloper` | `config.py` |
| `ANTHROPIC_API_KEY` (or `claude` already logged in) | — | consumed by the `claude` CLI itself |

**Only needed if the ticket reaches Station 6 (local SIT)** — install/obtain these before
running a coding ticket, not just when a run fails partway through:

| Requirement | Notes | How to get it |
|---|---|---|
| Docker Desktop installed and running, sized `>= OCEAN_PIPELINE_MIN_DOCKER_MEMORY_GB` (default `4`) GB / `>= OCEAN_PIPELINE_MIN_DOCKER_CPUS` (default `2`) CPUs | checked at the top of `sit_run` (`nodes.py::_docker_preflight_reason`), but only once the run gets that far | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/) |
| `pipenv` on PATH | the SIT skill runs pytest through it | `brew install pipenv` or `pip install pipenv` |
| LocalStack reachable at `OCEAN_PIPELINE_SQS_ENDPOINT_URL` (default `http://localhost:4566`) | needed for the SQS-driven leg of a multi-repo callback E2E; not checked anywhere today | see fk-aideveloper's `skills/local-infra-setup` |
| `cloudqwest/test-automation` cloned locally | the SIT skill `cd`s into it to run pytest | `gh repo clone cloudqwest/test-automation` — see fk-aideveloper's README for the expected path |
| `environment-configuration` cloned locally | Ruby worker Docker builds copy settings out of it | `gh repo clone cloudqwest/environment-configuration` |
| `test_rail_email` / `test_rail_password` env vars | only if the QA review gate is answered `--qa approve-testrail` | ask in `#fk-aideveloper` Slack |
| `~/.aws/credentials` `[qat]` profile | only for chains that push SQS test messages | ask in `#fk-aideveloper` Slack (see fk-aideveloper's `references/sqs_local_testing.md` for the verification snippet once you have it) |

**Optional, all fail open (absence = silent no-op, never blocks a run):**

| Variable | Default | Read in | Effect if unset |
|---|---|---|---|
| `JIRA_API_TOKEN` | `""` | `jira.py` | ticket never transitions to In Review, no PR-link comment |
| `JIRA_BASE_URL` | `https://fourkites.atlassian.net` | `jira.py` | n/a |
| `RCA_TOKEN` | `""` | `telemetry.py` | no rows land in the team's aidev-db dashboards |
| `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` / `LANGFUSE_BASE_URL` | — | `tracing.py` | no Langfuse trace (see Observability below) |

**Tuning knobs (safe defaults, override only if you know why):** `OCEAN_PIPELINE_ARTIFACTS`,
`OCEAN_PIPELINE_CHECKPOINT_DB`, `OCEAN_PIPELINE_MODEL`, `OCEAN_PIPELINE_LOG_LEVEL` (see
`--log-level` below), `OCEAN_PIPELINE_RCA_ONLY`, `OCEAN_PIPELINE_REQUIRE_APPROVAL`,
`OCEAN_PIPELINE_QA_AUTOAPPROVE`, `OCEAN_PIPELINE_TESTRAIL`, `OCEAN_PIPELINE_MAX_*`,
`OCEAN_PIPELINE_PERMISSION_MODE` / `OCEAN_PIPELINE_SKILL_PERMISSION_MODE`,
`OCEAN_PIPELINE_REPO_ORG`, `OCEAN_PIPELINE_GH_TIMEOUT_SECONDS`, `OCEAN_PIPELINE_ENGINEER`,
`OCEAN_PIPELINE_SESSION_ENV`. Every one of these has a hardcoded default in `config.py` —
that file is the source of truth if this list ever drifts.

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
  usage: 330k tokens · 74 tool calls   across 5 station runs
════════════════════════════════════════════════════════════════
```

Each station shows: a timestamped `▶` header, live `·` milestones, per-station usage
(`done — 140k tokens · 24 tool calls`), and a `✓` line with elapsed + facts —
including review findings spelled out (severity + summary + file) and SIT test names. The
footer totals cost / tokens / tool-calls for the whole run.

**Run report (written at the end).** Every run leaves a consolidated, shareable
`run-report.md` (+ `run-report.json`) in `$OCEAN_PIPELINE_ARTIFACTS/<EXE-id>/` — header
(ticket, timing, result, PR links, total usage) + a timeline table of every node with its
duration and outcome. Written even if the run fails (partial timeline). The console prints
`Full report: <path>` at the end.

**`--log-level {management,team,developer}` — three audiences, one run.** `team` is the
default and is exactly what's shown above. `management` prints only the `▶` header + the
`✓` outcome line per station — no milestones, no detail bullets. `developer` (same as the
older `-v`/`--verbose` flag, still supported as a shorthand) adds each agent's raw tool
calls, tool results, and thinking text on top of the milestones, for debugging. Per-node
verdicts also land in `$OCEAN_PIPELINE_ARTIFACTS/<EXE-id>/*.verdict.json` regardless of level.

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

Each run's trace is **named by the Jira key** (`MM-14475`, not the generic `LangGraph`),
carries `session = <EXE-id>`, and is **tagged `aquaman` + the ticket** — so in a shared
project you filter to `aquaman` (or search the ticket) and see only pipeline runs. Note
Langfuse exports a span when it *ends*, so a long node's trace appears once it completes;
the full trace lands when the run finishes (Aquaman flushes on exit).

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

MM-14615 / MM-14621: full node/edge topology, checkpointer, the Claude Agent SDK bridge, the
RCA→fix→coder branch, the review loop, and **Station 6 (ocean-automation-testing) with the
code_fault full-loop** are in place. MM-14621 made LangGraph the sole control plane — vendored
slim workers (`workers/research|code|review.md`), graph-owned SME consult, deterministic
git/PR code nodes, the coder's worktree threaded to the reviewer, an optional human-approval
gate before the ready-flip, and Jira lifecycle transitions. **Telemetry is wired** to the
aidev-db HTTP MCP server (`telemetry.py`, best-effort, no-op without `RCA_TOKEN`; transport
validated live). Remaining: decompose Station 6 into per-step graph nodes (deferred behind the
`learn_repo` onboarding work).
