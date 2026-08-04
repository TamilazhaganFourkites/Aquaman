# Aquaman

**MM-14615 — LangGraph orchestrator for the Ocean/MM (isbu) lean manufacturing pipeline.**

Aquaman is the deterministic, code-driven replacement for the `fk-execute` prose
"floor captain" checklist, for the Ocean/MM (`isbu`) boards only. The pipeline
control flow — station sequence, the RCA router, the review loop, the ready-flip
gate — is a LangGraph state machine. The *work inside each node* is still done by
narrow worker prompts that live in fk-aideveloper's `skills/ocean-coding-agent/workers/`
(the ocean domain SMEs live alongside them in `skills/ocean-coding-agent/agents/`),
invoked as Claude Agent SDK subprocesses. This repo owns orchestration; it holds no
worker/domain content itself (`config.OCEAN_WORKERS_DIR` / `OCEAN_AGENTS_DIR`) and
does not re-express any station logic.

## Why LangGraph

| Pipeline feature (CLAUDE.md) | LangGraph primitive |
|---|---|
| Fixed sequence 0 → 1 → 1.5 → 4 → 5 → 6 | linear edges |
| Router: RCA → stop vs coding → continue | conditional edge (`route_after_research`) |
| Review loop 5↔4, **max 2 iterations** | conditional edge + `review_iteration` counter |
| Never auto-merge/deploy; auto-flip to ready on green, with an optional human gate | plain-code `flip_ready`; optional `interrupt()` in `human_gate` (`OCEAN_PIPELINE_REQUIRE_APPROVAL`) |
| A human reviews the RCA's own conclusion before it's acted on (report or auto-coding) | `interrupt()` in `rca_review_gate`, default ON (`OCEAN_PIPELINE_RCA_REVIEW_AUTO` to skip) |
| Telemetry START/END + station events | `telemetry.py` → aidev-db HTTP MCP (`Bearer $RCA_TOKEN`), best-effort |
| AP-223 orphaned `running` rows | SQLite checkpointer → resume, not orphan |
| Language-scoped Docker (ruby=docker, java/go=native) | carried per-repo in `state["target_repos"]` |
| Unsupported ocean repo → learn it, don't dead-stop | `learn_repo` node + conditional edge (`MAX_ONBOARD_ATTEMPTS`) |

## Graph

```
START → researcher ─┬─(rca)────→ rca_agent → rca_report → rca_review_gate ─┬─(reject)────→ stop_run → END
                    │                                          ├─(no fix)────→ rca_done → END
                    │                                          └─(fix needed)──┐
                    └─(coding)─────────────────────────────────────────────────┼─→ dep_resolver → reachability_gate → coder ◄─┐
                                                                                                     │       │
                                                                                              harsh_reviewer  │
                                                          ┌──(CHANGES_REQUIRED & review_iter<2)───────────────┘
                                                          └──(APPROVE | review_iter≥2)→ open_pr* → automation_testing
                                                                                           │
                    ┌── code_fault & coding_attempts<budget → prep_rework ─────────────────┤
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

`rca_agent` produces the 5-part evidence-cited report to a file and the plain-code `rca_report` node
posts it to Jira as a single comment (deterministic, via `jira.py` — not the worker's MCP); **`rca_review_gate`**
(default ON) then pauses for a human to read that comment before the graph acts on the RCA's own
conclusion — an LLM's root-cause call should not silently trigger either the terminal report or an
autonomous coding run unread. `--resume <exe> --approve` proceeds to whichever the RCA already
decided (report-only, or on to `dep_resolver`); `--reject` stops here, before any code is touched.
`OCEAN_PIPELINE_RCA_REVIEW_AUTO` skips the pause for the headless `--rca-only` control-plane flow.

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
├── agents.py      run_agent()/run_skill(): drives an ocean-coding-agent worker via Claude Agent SDK
├── telemetry.py   aidev_db START/END + station-event hooks
├── nodes.py       one node per station (thin wrappers)
├── graph.py       StateGraph wiring: router, review loop, ready-flip
└── cli.py         `ocean-pipeline <TICKET>` — the /fk-execute replacement
```

## Setup

**Quick checklist (0 → running a real ticket).** Each step links to the detailed subsection
below if something doesn't match your machine — this is just the linear path with nothing skipped:

1. Complete fk-aideveloper's own onboarding (GitHub org access, `gh auth login`,
   `./scripts/dev-setup.sh --preflight`, `claude-setup.sh`) — **§0** below.
2. Clone Aquaman, create a **Python ≥ 3.11** venv, `pip install -e .`, export
   `FK_AIDEVELOPER_DIR` if it isn't at the default path, make sure `claude` is authenticated
   (`ANTHROPIC_API_KEY` or an interactive login) — **§1** below.
3. If the ticket will reach Station 6 (almost every coding ticket will): a Docker backend
   (Docker Desktop or Rancher Desktop) running + sized, Rosetta on if you're on Apple
   Silicon, `pipenv` on PATH, `cloudqwest/test-automation` and `environment-configuration`
   cloned locally — **§2** below.
4. Run `ocean-pipeline --print-graph` to confirm the install and the graph compile with zero
   external calls — **Verify your setup**, at the end of this section.
5. Run a real ticket: `ocean-pipeline <TICKET-ID>` — see **Run** below.

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
| A Docker backend installed and running (Docker Desktop **or** Rancher Desktop — either is fine), sized `>= OCEAN_PIPELINE_MIN_DOCKER_MEMORY_GB` (default `4`) GB / `>= OCEAN_PIPELINE_MIN_DOCKER_CPUS` (default `2`) CPUs at minimum, but budget **8–12 GB / ≥4 CPU** if the ticket needs a full multi-service E2E, not just a single mocked repo (`local_service_execution.md` "Docker memory ceiling") | checked at the top of `sit_run` (`nodes.py::_docker_preflight_reason`); a backend that doesn't expose `MemTotal`/`NCPU` via `docker info` (Rancher Desktop/colima/Podman sometimes don't) is treated as unmeasurable-but-running, not down | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/) or [rancherdesktop.io](https://rancherdesktop.io/) |
| **On Apple Silicon, enable Rosetta** in whichever backend's VM settings | the Ruby-worker repos (`ocean-worker`, `multimodal-worker`, `MMCUW`, `tracking-service`, `global_worker`) build from a private **amd64-only** azurecr base image — without Rosetta, QEMU emulation segfaults on native-gem builds. Only turn it off once you've confirmed an arm64 base variant exists for the repo you're building | Docker Desktop/Rancher Desktop → Settings → General/Virtual Machine |
| `pipenv` on PATH | the SIT skill runs pytest through it | `brew install pipenv` or `pip install pipenv` |
| LocalStack reachable at `OCEAN_PIPELINE_SQS_ENDPOINT_URL` (default `http://localhost:4566`) | needed for the SQS-driven leg of a multi-repo callback E2E; not checked anywhere today | see fk-aideveloper's `skills/local-infra-setup` |
| `cloudqwest/test-automation` cloned locally | the SIT skill `cd`s into it to run pytest | `gh repo clone cloudqwest/test-automation ~/Documents/projects/test-automation` — fk-aideveloper's own README flags this path as inconsistently documented (`~/Documents/projects/test-automation` vs `~/Documents/workspace/test-automation`); use the `~/Documents/projects/` path to match `FK_AIDEVELOPER_DIR`'s own default location |
| `environment-configuration` cloned locally | Ruby worker Docker builds copy settings out of it | `gh repo clone cloudqwest/environment-configuration ~/Documents/projects/environment-configuration` — same `~/Documents/projects/` convention; fk-aideveloper's README notes no canonical location is documented yet |
| TestRail creds — `TESTRAIL_EMAIL` / `TESTRAIL_API_KEY` exported in your `~/.zshrc` (a developer's local pair; CI instead injects `test_rail_email`/`test_rail_password`, both names accepted) | only if the QA review gate is answered `--qa approve-testrail` — `sit_testrail`'s `create_testrail_cases.py` aborts pre-flight and creates zero cases if absent | usually already set from a prior TestRail setup — verify with `printenv TESTRAIL_EMAIL` (name only, never echo the value); if genuinely unset, ask in `#fk-aideveloper` Slack |
| `~/.aws/credentials` `[qat]` profile | only for chains that push SQS test messages | ask in `#fk-aideveloper` Slack (see fk-aideveloper's `references/sqs_local_testing.md` for the verification snippet once you have it) |

**Set TestRail creds in `~/.zshrc`, not a one-off `export` in your terminal.** `sit_testrail`
shells out to `create_testrail_cases.py` via `zsh -ic '<full command>'` specifically so
`~/.zshrc`'s exports load in their native shell and reach the child process — a plain bash
`source ~/.zshrc` beforehand is unreliable (wrong interpreter, interactive guards, and the env
doesn't survive across separate tool calls). An `export` typed directly into an already-running
shell works for that shell, but won't be there the next time the pipeline spawns a fresh one.

**Optional, all fail open (absence = silent no-op, never blocks a run):**

| Variable | Default | Read in | Effect if unset |
|---|---|---|---|
| `JIRA_API_TOKEN` | `""` | `jira.py` | ticket never transitions to In Review, no PR-link comment |
| `JIRA_BASE_URL` | `https://fourkites.atlassian.net` | `jira.py` | n/a |
| `RCA_TOKEN` | `""` | `telemetry.py` | no rows land in the team's aidev-db dashboards |
| `GH_TOKEN` / `GITHUB_TOKEN` | — | `nodes.py::_gh_token` | rarely needed — `gh auth token` (from `gh auth login`, §0) is tried first and covers most setups; this is only a fallback for cold Ruby-image builds if that fails |
| `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` / `LANGFUSE_BASE_URL` | — | `tracing.py` | no Langfuse trace (see Observability below) |

**Where these actually need to live.** `tracing.py::_load_fk_secrets` reads `~/.fourkites-secrets.env`
and `setdefault`s *every* `key=value` line in it into `os.environ` (never overriding anything
already exported) — it isn't filtered to `LANGFUSE_*`. But it only runs once, at the top of
`_execute()`, well **after** Python has already imported `jira.py`/`telemetry.py`/`config.py` and
frozen `JIRA_API_TOKEN`/`JIRA_BASE_URL`/`RCA_TOKEN`/`FK_AIDEVELOPER_DIR` into module-level constants
at import time — so putting those specific vars in the secrets file is too late to help, even
though the loader itself doesn't reject them. `~/.zshrc` (or your shell's equivalent profile) has
no such timing gap, since it's sourced before the Python process even starts — that's why it's the
reliable place for `FK_AIDEVELOPER_DIR`, `JIRA_API_TOKEN`, `JIRA_BASE_URL`, `RCA_TOKEN`,
`GH_TOKEN`/`GITHUB_TOKEN`, and TestRail's `TESTRAIL_*` pair above, and why `~/.fourkites-secrets.env`
is documented as a Langfuse-only convention rather than a general one. A one-off `export` typed
into an already-running terminal works only for that terminal — it won't be there the next
terminal, IDE-integrated shell, or subprocess the pipeline spawns. `ANTHROPIC_API_KEY` and `gh auth`
are the two that usually need neither: an interactive `claude` login and `gh auth login` (§0) each
persist their own credential outside the shell environment, so most users never export either one.

**Tuning knobs (safe defaults, override only if you know why):** `OCEAN_PIPELINE_ARTIFACTS`,
`OCEAN_PIPELINE_CHECKPOINT_DB`, `OCEAN_PIPELINE_MODEL`, `OCEAN_PIPELINE_LOG_LEVEL` (see
`--log-level` below), `OCEAN_PIPELINE_RCA_ONLY`, `OCEAN_PIPELINE_RCA_REVIEW_AUTO`,
`OCEAN_PIPELINE_REQUIRE_APPROVAL`, `OCEAN_PIPELINE_QA_AUTOAPPROVE`,
`OCEAN_PIPELINE_TESTRAIL`, `OCEAN_PIPELINE_MAX_*`,
`OCEAN_PIPELINE_PERMISSION_MODE` / `OCEAN_PIPELINE_SKILL_PERMISSION_MODE`,
`OCEAN_PIPELINE_REPO_ORG`, `OCEAN_PIPELINE_GH_TIMEOUT_SECONDS`, `OCEAN_PIPELINE_ENGINEER`,
`OCEAN_PIPELINE_SESSION_ENV`. Every one of these has a hardcoded default in `config.py` —
that file is the source of truth if this list ever drifts.

### Verify your setup

Before pointing the pipeline at a real ticket, confirm each piece independently — cheaper
than debugging a mid-run failure at Station 4 or 6:

```bash
ocean-pipeline --print-graph      # compiles the graph, prints the mermaid diagram — zero
                                   # external calls (no Claude, no GitHub, no Docker); if this
                                   # fails, it's an install/import problem, not a runtime one
gh auth status                    # required for open_pr/flip_ready — see §0 above
claude --version                  # confirms the CLI is installed; run `claude` once
                                   # interactively if it isn't already logged in
docker info                       # only if the ticket will reach Station 6 — confirms your
                                   # backend (Docker Desktop or Rancher Desktop) is actually
                                   # running (not just installed)
pipenv --version                  # only if the ticket will reach Station 6 — the SIT skill
                                   # shells out to this
```

All five green is a good sign, but the only one enforced at run time is `cli.py::_preflight()`
(dirs exist, `claude`/`gh` on PATH, `gh auth status`) — the rest fail later, at whichever
station first needs them (Docker/`pipenv` at Station 6, `gh auth` at `open_pr`).

## Run

```bash
ocean-pipeline MM-14615
ocean-pipeline MM-14615 --context "Bug only affects SCAC=ABCD loads"
ocean-pipeline MM-14615 --rca-only        # research + RCA report only — never auto-codes a fix
ocean-pipeline --print-graph              # mermaid diagram straight from the compiled graph
```

Node completions stream to the console as the run proceeds. The pipeline **never
auto-merges or auto-deploys**; on SIT PASS it flips the service PR to ready-for-review
automatically. The engineer still owns final merge, sign-off, and deploy.

### Resuming a paused or crashed run

A run pauses at one of three `interrupt()` gates — `rca_review_gate`, `qa_review_gate`,
`human_gate` — or stops on a crash. Either way, `--resume <EXE-id>` continues it from its last
checkpoint; which flag it needs depends on which gate it's actually paused at:

```bash
ocean-pipeline --resume EXE-1a2b3c4d                                # crash resume — no flag needed
ocean-pipeline --resume EXE-1a2b3c4d --approve                      # rca_review_gate / human_gate: proceed
ocean-pipeline --resume EXE-1a2b3c4d --reject                       # rca_review_gate / human_gate: stop here
ocean-pipeline --resume EXE-1a2b3c4d --qa approve-testrail           # qa_review_gate: run SIT + write TestRail cases
ocean-pipeline --resume EXE-1a2b3c4d --qa approve-no-testrail        # qa_review_gate: run SIT, skip TestRail
ocean-pipeline --resume EXE-1a2b3c4d --qa changes --note "add a case for X"   # qa_review_gate: redraft
```

Don't guess which pair applies — the console tells you. A paused run prints
`[PAUSED] <gate-specific message with the exact flags it expects>`
(`cli.py::_pause_message`); e.g. `--approve` on a paused `qa_review_gate` silently falls through
to the wrong branch instead of erroring, so the printed hint is the source of truth, not this table.

## Observability

**`--log-level {management,team,developer}` — three audiences, one run, each a strict
superset of the one before it.**

- **`management`** — the run banner + a `▶` header per station. Nothing else: no outcome,
  no milestones, no detail bullets. Just "what's running right now."
- **`team`** (default) — management, plus exactly **one outcome line per station** (`✓`/`✗`,
  elapsed time, a one-line highlight).
- **`developer`** (same as the older `-v`/`--verbose` flag, still supported as a shorthand)
  — team, plus the curated **milestones** streamed live during each station (clone / branch /
  edit / test / commit / push / PR / docker / pytest — noise suppressed), that station's
  detail bullets (`└`), and each agent's raw tool calls, tool results, thinking text, and
  sub-agent (Task) lifecycle — the full firehose, for debugging.

`management`:
```
════════════════════════════════════════════════════════════════
  FK Ocean Pipeline   ·   MM-14609
  run EXE-1a2b3c4d   ·   started 22:53:42
════════════════════════════════════════════════════════════════

▶  22:53:43  Research & routing

▶  22:54:17  Coding

▶  22:57:47  Adversarial code review

▶  22:58:29  Local SIT (automation testing)
```

The one exception to "nothing else" at `management`: the closing `RESULT:`/`[DONE]`/`Full
report:` block below always prints, at every `--log-level` (`ui.summary`, and the two lines
`cli.py` prints after it, have no level gate — only the per-station `✓`/`✗` line and its
milestones/details do). `management` genuinely suppresses everything *during* the run, not
the final result.

`team` (default) — same banner, but each `▶` header is followed by its one outcome line:
```
▶  22:53:43  Research & routing
  ✓  Research & routing                       34s   → routed to coding

▶  22:54:17  Coding
  ✓  Coding                                 3m 30s   branch pushed

▶  22:57:47  Adversarial code review
  ✓  Adversarial code review                  42s   APPROVE

▶  22:58:29  Local SIT (automation testing)
  ✓  Local SIT (automation testing)         6m 20s   SIT passed
────────────────────────────────────────────────────────────────
  RESULT: COMPLETED   ·   took 11m 06s   ·   finished 23:04:48
  sit_passed; service PR #123 ready-for-review
  usage: 330k tokens · 74 tool calls   across 5 station runs
════════════════════════════════════════════════════════════════

[DONE] MM-14609 status=completed pr=#123
  Full report: /tmp/ocean-pipeline/EXE-1a2b3c4d/run-report.md
  Cleaned 3 run Docker resource(s): container:ocean-worker-EXE-1a2b3c4d, image:ocean-worker-mm14609-coder …
```

`[DONE] <ticket> status=<...> pr=#<n>` is a deliberate machine-readable line, not log noise —
headless runners (e.g. `oas-autodev`'s reconcile step) grep the tail of the process output for
`pr=#<n>` to know a run finished green without re-parsing the whole transcript. The `Cleaned …`
line only appears if this run actually created Docker resources (`cli.py::_cleanup_run_docker`)
and is skipped entirely on a pause (the container stack stays up for the eventual resume).

`developer` additionally streams milestones live and prints detail bullets after each outcome:
```
▶  22:53:43  Research & routing
     · querying Atlassian
     · dispatching sub-agent: ocean SME
    [researcher] ⚡ sub-agent progress: ocean SME (last tool: mcp__fk-code-graph__execute_cypher_query)
  ✓  Research & routing                       34s   → routed to coding
        └ repo: ocean-worker (ruby, docker)

▶  22:58:29  Local SIT (automation testing)
     · bringing up local Docker infra
     · running the SIT (pytest)
  ✓  Local SIT (automation testing)         6m 20s   SIT passed
        └ 2/2 tests passed
        └   passed: test_eta_exception_cleared_on_pod
        └ ran ocean-worker local · mocked: tracking-service
```

The `[label] ...` lines are the raw per-agent dump: every tool call, tool result, thinking
block, and — since a worker can itself dispatch a sub-agent via the Task tool — that
sub-agent's own start/progress/completion lifecycle, which previously vanished silently even
under `--verbose` (neither shape `_format_message` checked for matched a `SystemMessage`).

**Run report (written at the end, regardless of console level).** Every run leaves a
consolidated, shareable `run-report.md` (+ `run-report.json`) in
`$OCEAN_PIPELINE_ARTIFACTS/<EXE-id>/` — header (ticket, timing, result, PR links, total
usage) + a timeline table of every node with its duration and full outcome line (the same
detail `developer` shows, independent of what the console printed). Written even if the run
fails (partial timeline). The console prints `Full report: <path>` at the end. Per-node
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
code_fault full-loop** are in place. MM-14621 made LangGraph the sole control plane — slim
single-job workers, graph-owned SME consult, deterministic git/PR code nodes, the coder's
worktree threaded to the reviewer, an optional human-approval gate before the ready-flip, and
Jira lifecycle transitions. **MM-14620: the workers now live in fk-aideveloper's
`ocean-coding-agent` skill (single source), not vendored here — this repo is a pure control
plane that references them (`config.OCEAN_WORKERS_DIR`); it holds no worker/domain content.** **Telemetry is wired** to the
aidev-db HTTP MCP server (`telemetry.py`, best-effort, no-op without `RCA_TOKEN`; transport
validated live). **Station 6 is now decomposed into per-step graph nodes** (`sit_resolve →
sit_author → qa_review_gate → sit_run ‖ sit_testrail → sit_triage`), with graph-owned repo
onboarding (`learn_repo`). Remaining: strip the residual "Station X" phase labels still passed
in some node prompts; a test that proves node-level retry *recovers* (not just propagates) a
transient failure; and one archived real end-to-end run (no `--mock`/golden-run scaffold exists).
