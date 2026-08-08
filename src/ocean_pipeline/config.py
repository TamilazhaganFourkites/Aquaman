"""Runtime configuration.

The station agents (`agents/pipeline/fk-*.md`) still live in the fk-aideveloper
checkout. This orchestrator repo points at that checkout rather than vendoring
the prompts, so the two evolve together. Set FK_AIDEVELOPER_DIR to the local
clone.
"""
from __future__ import annotations

import os
from pathlib import Path

# Where the fk-aideveloper repo (station agent .md files + skills) is checked out.
FK_AIDEVELOPER_DIR = Path(
    os.environ.get("FK_AIDEVELOPER_DIR", str(Path.home() / "Documents/projects/fk-aideveloper"))
)

AGENTS_DIR = FK_AIDEVELOPER_DIR / "agents" / "pipeline"

# Root that holds the sibling target-repo checkouts the analysis stations (researcher/SME/
# dep-resolver/reachability) read at `<PROJECTS_ROOT>/<repo>` (the coder clones fresh into its own
# per-run workspace instead). The orchestrator re-syncs this checkout to the default-branch tip once
# before the analysis stations (G2), so they all analyze current code rather than a stale base.
PROJECTS_ROOT = Path(os.environ.get("FK_PROJECTS_ROOT", str(FK_AIDEVELOPER_DIR.parent)))

# Ocean coding WORKERS now live in fk-aideveloper's `ocean-coding-agent` skill — the single home for
# the slim, single-job ocean coding worker prompts (MM-14620 Q1: the control plane holds NO worker
# content, it references them). run_agent resolves a worker `agent_md` here FIRST, then falls back to
# AGENTS_DIR for the ocean domain SMEs (`sme-*.md`, still referenced expert knowledge). Previously these
# workers were vendored into this repo (`src/ocean_pipeline/workers/`); that copy was removed to make
# fk-aideveloper the single source (no duplication / drift).
OCEAN_WORKERS_DIR = FK_AIDEVELOPER_DIR / "skills" / "ocean-coding-agent" / "workers"

# Ocean domain SMEs (`sme-*.md`) also live under the ocean-coding-agent skill (MM-14620: ocean-
# introduced, so housed in the ocean home, not the generic agents/pipeline). Aquaman's sme_consult
# node dispatches them; _agent_path resolves them here.
OCEAN_AGENTS_DIR = FK_AIDEVELOPER_DIR / "skills" / "ocean-coding-agent" / "agents"

# Per-run artifact root (reachability-report.json, per-station verdict.json, etc.).
#
# NOT /tmp. macOS runs /System/Library/LaunchDaemons/com.apple.tmp_cleaner.plist daily at 00:00,
# deleting /tmp files whose atime+mtime+ctime all exceed 3 days and then the emptied directories —
# so a run's evidence disappears between one ticket and the next. Measured: 38 unique execution_ids
# in timings.jsonl, exactly 2 EXE directories still on disk.
#
# This is not a hypothetical tidiness point. `lessons.py:21-28` justifies the whole cross-ticket
# memory design on the artifacts "surviving across runs on this machine" — a 3-day cleaner defeats
# "ticket 200 is just as smart as ticket 1" outright. (The loss is prospective: lessons.py landed
# 2026-08-07, after the last real run, so nothing has been lost yet.) It also gates every measurement
# item in the queue, since the evidence they compute on is what gets purged.
#
# The diagnosis already existed one line down — TIMINGS_LOG was moved to ~/.ocean-pipeline for
# exactly this reason and says so. It was fixed for one file and left for everything else.
ARTIFACTS_ROOT = Path(os.environ.get("OCEAN_PIPELINE_ARTIFACTS",
                                     str(Path.home() / ".ocean-pipeline" / "artifacts")))

# Durable per-run station-timing log (one JSON line per finished run). Kept out of ARTIFACTS_ROOT
# and out of any git repo; both now live under ~/.ocean-pipeline for the reason given above.
TIMINGS_LOG = Path(os.environ.get("OCEAN_PIPELINE_TIMINGS_LOG",
                                  str(Path.home() / ".ocean-pipeline" / "timings.jsonl")))

# Latency prototype (measure before/after with the SAME instrumentation):
#   on  (default) — researcher fans out to sme_consult ∥ dep_resolver ∥ prep_image (they are
#                   independent: dep_resolver reads only the research packet, not sme_findings),
#                   joining at reachability_gate; prep_image pre-builds the Ruby image off the
#                   coder's critical path.
#   off — the original strictly-sequential research -> sme -> dep -> reachability chain, for a
#         clean baseline run. Set OCEAN_PIPELINE_PARALLEL_ANALYSIS=0.
PARALLEL_ANALYSIS = os.environ.get("OCEAN_PIPELINE_PARALLEL_ANALYSIS", "1").lower() not in ("0", "false", "no")

# Latency lever #1 — orchestrator-owned persistent Docker container. When on, the graph starts ONE
# booted container from the pre-warmed image before the coder (prep_container) and every Docker
# station (coder, reviewer, sit_author, sit_run) reuses it (docker cp current code + docker exec) instead
# of each doing its own `docker run` + image/env re-derivation; teardown_container removes it at the end.
# DEFAULT ON: the Docker mechanics are validated (a real run showed each station rolling its OWN
# container — reachability `ocean-<repo>-<TICKET>` and coder `ocean-ow-<ticket>-coder` never shared one,
# so the per-station docker-run/re-derive cost + leaked containers persisted). Enabling it makes the
# graph own ONE shared container and tear it down. Fully fallback-safe — if the container can't start
# (no Docker/image/checkout) or a station's exec probe fails, container_ready stays False and stations
# use their existing self-contained recipe, so the pipeline behaves exactly as before. Disable per-run
# with OCEAN_PIPELINE_PERSISTENT_CONTAINER=0.
PERSISTENT_CONTAINER = os.environ.get("OCEAN_PIPELINE_PERSISTENT_CONTAINER", "1").lower() in ("1", "true", "yes")

# Latency lever #6 — keep the SIT infra (localstack/es/redis/mock) warm ACROSS the code_fault /
# environment_failure retry loop. When on, sit_run brings the infra up under a stable compose project
# `ocean-sit-<exec>` and REUSES it on a re-entry instead of tearing down + re-bootstrapping the whole
# stack each attempt (the ~23m sit_run bring-up is paid once, not once per loop); teardown_container
# removes the project at run end. Same DEFAULT OFF + fallback-safe rationale as PERSISTENT_CONTAINER:
# if anything about the stack differs, the skill just brings up a fresh stack as it does today.
WARM_SIT_INFRA = os.environ.get("OCEAN_PIPELINE_WARM_SIT_INFRA", "0").lower() in ("1", "true", "yes")

# Latency lever #7 — bake the `test` bundle group into the pre-warmed image (ruby_image_cache
# --include-test-group) so the coder's unit rspec and the SIT run start test-ready and NEVER re-run
# `bundle install` at runtime. WHY: EXE-1e3d6530 observed the coder running 49 runtime `bundle install`s
# across two repos, each crawling under Rosetta, which pushed it past its wait window and the run was
# killed with no verdict (manual-findings #4 / tracker #1+#16). DEFAULT ON (B5): EXE-6fca4a71 confirmed the
# prod image can't run specs — reachability AND coder each fell back to a separate `--units` test image, and
# the SIT couldn't reuse the shared prod container. Fully fallback-safe: the test-group child build is
# best-effort; if it fails, prep falls back to the prod image and the station installs as it does today.
# Set OCEAN_PIPELINE_BAKE_TEST_GROUP=0 to disable; a follow-up run should confirm the -test image boots
# green. Pair with OCEAN_IMAGE_PLATFORM=linux/arm64 on Apple Silicon (ruby_image_cache reads it) to also
# drop Rosetta (manual-findings #15).
BAKE_TEST_GROUP = os.environ.get("OCEAN_PIPELINE_BAKE_TEST_GROUP", "1").lower() in ("1", "true", "yes")

# Max number of SIT stacks allowed to run CONCURRENTLY across all pipeline runs on this machine
# (manual-findings #18). The SIT stage stands up a heavy stack (localstack/es/redis/mock + the changed
# repos); several at once over-commit the one Docker VM (EXE-caa5c082 died at Step-0 for this). sit_run
# acquires one of N cross-process file-lock slots BEFORE bring-up and holds it until teardown, so a 2nd/3rd
# run WAITS for a free slot instead of failing preflight — a queue, not a crash. Default 1 = serialize SIT
# (the safe default given the observed over-commit); raise it on a big-memory machine. The lock is a flock,
# so a crashed holder auto-frees its slot.
MAX_CONCURRENT_SIT = int(os.environ.get("OCEAN_PIPELINE_MAX_CONCURRENT_SIT", "1"))
# How long a run waits for a free SIT slot before giving up and proceeding anyway (so a wedged holder can't
# block forever — the preflight over-commit check is still the backstop). Default 2h.
SIT_SLOT_WAIT_SECONDS = int(os.environ.get("OCEAN_PIPELINE_SIT_SLOT_WAIT_SECONDS", "7200"))

# Separate concurrency pool for the BUILD-type stages (prep_image, prep_container's `docker run -d`,
# coder/harsh_reviewer/reachability_gate's Ruby `bundle install`/rspec-in-Docker) — deliberately not
# folded into MAX_CONCURRENT_SIT above. A SIT stack is long (~20-30 min bring-up) and heavy (the shared
# infra floor below plus N repos); a build-type session is one repo's image/container (~2.5-3 GB per
# _REPO_FOOTPRINT_GB) and much shorter. Forcing both through one pool sized for SIT (default 1 slot, 2h
# wait) would serialize a 3-5 min coder Docker phase behind a run that's mid-SIT for two hours. A slot
# COUNT alone doesn't bound real memory, though (a fixed count says nothing about how big any one
# session actually is) — _try_acquire_build_slot (nodes.py) pairs this count with a live
# _docker_used_gb() headroom read as a HARD condition, not just an advisory log line, which is what
# lets this compose safely with MAX_CONCURRENT_SIT above: both ultimately defer to the same `docker
# stats` ground truth, so a live SIT stack from another process is visible to a build-slot acquisition
# attempt (and vice versa, since SIT's own preflight already reads that same signal).
MAX_CONCURRENT_BUILDS = int(os.environ.get("OCEAN_PIPELINE_MAX_CONCURRENT_BUILDS", "2"))
# Shorter than SIT_SLOT_WAIT_SECONDS (2h) since build-type stages are lighter/shorter-lived — same
# "proceed anyway past the deadline, the live headroom check is the real backstop" philosophy.
BUILD_SLOT_WAIT_SECONDS = int(os.environ.get("OCEAN_PIPELINE_BUILD_SLOT_WAIT_SECONDS", "1800"))

# Third, SEPARATE pool: qa_scenarios' GAN loop (S2, run-monitoring-findings.md) fans out up to 4
# sub-agents/round for up to 3 rounds INSIDE one node's single run_skill call — invisible to the SIT
# and build slots above (this is LLM sub-agent/API concurrency, not Docker memory). Two tickets'
# GANs running head-to-head observed at ~20-22 min each vs. Tier A's ~18-19 min solo-run projection,
# plus a TestRail 429 the agent attributed to "the parallel sit_run/other load" — i.e. contention
# hits both local CPU/LLM concurrency and the shared external TestRail API. Default 1 = serialize the
# GAN loop machine-wide (the single biggest contention source identified); a wedged holder still can't
# block forever (same wait-then-proceed-anyway posture as the two pools above).
MAX_CONCURRENT_GAN = int(os.environ.get("OCEAN_PIPELINE_MAX_CONCURRENT_GAN", "1"))
# Shorter than SIT's 2h (a GAN loop is ~20-25 min end to end, not a multi-hour stack).
GAN_SLOT_WAIT_SECONDS = int(os.environ.get("OCEAN_PIPELINE_GAN_SLOT_WAIT_SECONDS", "1800"))


def sit_infra_project(execution_id: str) -> str:
    """Deterministic compose project name for a run's SIT infra, so a retry reuses the SAME stack and
    teardown_container can remove exactly it (#6)."""
    return f"ocean-sit-{execution_id}"

# At run end (terminal exit — NOT a pause), also delete the SHARED `<repo>-cached:<lockhash>` base image
# this run built/used. DEFAULT ON (delete): the common workflow is one ticket at a time, so nothing else
# needs the image — dropping it leaves no residue. The cache still helps WITHIN the run (built once,
# reused across coder/reviewer/SIT); only the final teardown removes it. Set OCEAN_PIPELINE_KEEP_CACHED_IMAGE=1
# to KEEP it for reuse across runs (concurrent tickets / back-to-back same-repo runs) — then it's capped
# by ruby_image_cache's newest-N eviction instead.
KEEP_CACHED_IMAGE = os.environ.get("OCEAN_PIPELINE_KEEP_CACHED_IMAGE", "0").lower() in ("1", "true", "yes")

# LangGraph checkpointer DB — durable resume + the AP-223 orphan fix.
CHECKPOINT_DB = os.environ.get("OCEAN_PIPELINE_CHECKPOINT_DB", str(ARTIFACTS_ROOT / "checkpoints.sqlite"))

# Default model for station agents. Always the latest capable Opus unless overridden.
STATION_MODEL = os.environ.get("OCEAN_PIPELINE_MODEL", "claude-opus-5")  # F6 (architecture review): was claude-opus-4-8 (one gen back); opus-5 is current — accuracy win across all stations, env-overridable
# F7 (architecture review): the JUDGE nodes (harsh_reviewer, sit_triage) run on a DIFFERENT model than
# the coder/generator (STATION_MODEL) to reduce self-preference bias — a generator that grades its own
# family maximizes it (LLM-as-judge literature). Defaults to sonnet-5 (a distinct model from opus-5,
# strong enough for adversarial review, cheaper/faster). Set == STATION_MODEL to disable cross-model judging.
JUDGE_MODEL = os.environ.get("OCEAN_PIPELINE_JUDGE_MODEL", "claude-sonnet-5")

# Console log detail. Three audiences, one run:
#   management  — top station headers + one-line outcome per station only (no milestones,
#                 no per-station detail bullets, no raw agent activity). For a non-engineer
#                 skimming progress.
#   team        — headers + outcome + the curated milestone/detail lines already built for
#                 this log (significant tool calls, a handful of facts per station).
#   developer   — (DEFAULT) team, plus the full raw per-message agent activity (every tool
#                 call with its args, every tool result, every thinking/text block, every
#                 sub-agent lifecycle event) — the full firehose, on by default so nothing
#                 is silently missing; --log-level management/team opt into LESS detail.
# Toggled by env, or the CLI --log-level flag (--verbose is a no-op shorthand for the
# already-default --log-level developer, kept for back-compat).
_LOG_LEVELS = ("management", "team", "developer")
_env_log_level = os.environ.get("OCEAN_PIPELINE_LOG_LEVEL", "").strip().lower()
LOG_LEVEL = _env_log_level if _env_log_level in _LOG_LEVELS else "developer"

# RCA-only mode: run research -> ocean-rca report and STOP after the report, even when
# the root cause needs a code fix (do NOT auto-proceed to coding). This preserves the
# staged control-plane workflow where "In RCA" analyses + reports, and the fix is a
# separate run once the ticket moves to "RCA Done". Toggled by env (set by the
# oas-autodev spawn-aquaman.sh for the rca action) or the CLI --rca-only flag.
RCA_ONLY = os.environ.get("OCEAN_PIPELINE_RCA_ONLY", "").lower() in ("1", "true", "yes")

# RCA review gate. The graph's rca_report node already posts the worker's 7-part evidence-cited
# report as a Jira comment (rca_agent itself never posts — see nodes.py), but nothing gated on it
# before this — an LLM's own root-cause conclusion could silently kick
# off an entire autonomous coding run (fix_needed=true) with no human having read the analysis
# first. Default ON (the graph interrupt()s and waits) — unlike REQUIRE_APPROVAL below, this is
# the FIRST checkpoint before autonomous work starts, not the last one before a already-tested
# PR ships, so it defaults to the more conservative posture (same as QA_REVIEW_AUTO's gate).
# Set for the headless --rca-only control-plane flow (oas-autodev spawn-aquaman.sh) if it should
# run unattended.
RCA_REVIEW_AUTO = os.environ.get("OCEAN_PIPELINE_RCA_REVIEW_AUTO", "").lower() in ("1", "true", "yes")

# Hard cap on the Station 5 <-> Station 4 review loop (CLAUDE.md: max 2 iterations).
MAX_REVIEW_ITERATIONS = 2

# ---- Station 4.5: deterministic quality gate --------------------------------------------------
# Zero-config static checks over the coder's CHANGED files, run in plain Python between `coder` and
# `harsh_reviewer`. Closes the one path with no deterministic check at all: today nothing mechanical
# looks at a diff before an LLM reviews it and a draft PR opens (the junit parse, F1, is downstream at
# Station 6). Defaults ON — a gate shipped default-off is not shipped.
QUALITY_GATE = os.environ.get("OCEAN_PIPELINE_QUALITY_GATE", "1").lower() in ("1", "true", "yes")
# BOUNCES back to the coder before giving up — not gate firings. Kept at 1 deliberately: one bounce
# costs a FULL coder station (~30 min observed), and a syntax error the coder was handed verbatim and
# still failed to fix is a run that needs a human, not another attempt.
#
# The router compares `attempts > MAX`, NOT `>=`. With `>=` and this value the FIRST blocking finding
# terminated the run: `quality_gate` increments before the router reads it, so attempts was already 1
# on the first firing, the `rework` edge was unreachable, and the coder-prompt injection this whole
# feature hangs on was dead code. A judge caught it — the exact inertness class this gate exists to
# prevent, reproduced inside the gate. Read the comparison in graph.after_quality_gate before changing
# either side.
MAX_QUALITY_GATE_ATTEMPTS = int(os.environ.get("OCEAN_PIPELINE_MAX_QUALITY_GATE_ATTEMPTS", "1"))
QUALITY_GATE_MAX_FILES = int(os.environ.get("OCEAN_PIPELINE_QUALITY_GATE_MAX_FILES", "200"))
QUALITY_GATE_CMD_TIMEOUT = float(os.environ.get("OCEAN_PIPELINE_QUALITY_GATE_CMD_TIMEOUT", "60"))
# Java is OFF by default: only eta-worker declares spotless, and `mvn` there resolves offline against
# hand-installed local JARs (install-local-deps.sh), so an on-by-default Java path would report
# could-not-run on most runs and add minutes to the rest.
QUALITY_GATE_JAVA = os.environ.get("OCEAN_PIPELINE_QUALITY_GATE_JAVA", "").lower() in ("1", "true", "yes")

# ---- B1: multi-repo review coverage gate ------------------------------------------------------
# `harsh_reviewer` runs with ONE cwd (nodes.py, cwd=Path(state["worktree_dir"])) while `open_pr` and
# `flip_ready` act on EVERY slug in `_service_slugs(state)`. Nothing in between notices a repo that
# gets a PR without ever being reviewed.
#
# Default ON with an env escape, per the agreed decision in
# important-notes/MM-14816-multi-repo-review-gate-plan.md. READ THAT PLAN'S "Known costs" BEFORE
# CHANGING THIS: with the gate on, a genuine multi-repo ticket halts with ZERO PRs, because one
# review cwd cannot cover two repos and the coder cannot make it. That is a TRUE positive and the
# intended behaviour, not a defect -- but it is unrecoverable until the deferred per-repo review loop
# lands. Knob OFF still RECORDS the gap; it only declines to act on it.
MULTI_REPO_REVIEW_GATE = os.environ.get(
    "OCEAN_PIPELINE_MULTI_REPO_REVIEW_GATE", "1").lower() in ("1", "true", "yes")
# Bounces before giving up. Same `attempts > MAX` comparison as MAX_QUALITY_GATE_ATTEMPTS.
MAX_COVERAGE_ATTEMPTS = int(os.environ.get("OCEAN_PIPELINE_MAX_COVERAGE_ATTEMPTS", "1"))

# ---- D4: mechanical secret scan before the ready-flip -----------------------------------------
# Default ON. Unlike NODE_EVAL above this is free (one `gitleaks stdin` per changed repo over the
# diff, measured at ~23ms), it is deterministic rather than a judgment, and "a gate shipped
# default-off is not shipped" (the QUALITY_GATE comment above). Fails CLOSED on a finding and OPEN,
# loudly, when the scanner could not run -- an absent gitleaks must not halt every flip.
SECRET_SCAN = os.environ.get("OCEAN_PIPELINE_SECRET_SCAN", "1").lower() in ("1", "true", "yes")
SECRET_SCAN_TIMEOUT = float(os.environ.get("OCEAN_PIPELINE_SECRET_SCAN_TIMEOUT", "120"))

# ---- D5/D6: independent per-node accuracy evaluation ------------------------------------------
# An LLM judge scores one node's output against the rubrics in the ocean-coding-agent worker
# `node-evaluator.md`. The worker was specced end to end and monitor/app.py already handles its
# output; only the driver was missing.
#
# Default OFF, unlike QUALITY_GATE next door, and the difference is deliberate rather than timid.
# QUALITY_GATE is free (plain Python over changed files); this spends a FULL extra SDK session per
# evaluated node, on every run, forever. Turning that on by default as part of first implementing it
# would change the cost of every run in the same commit that makes it possible to measure that cost,
# leaving no baseline to compare against. Turn it on, measure the spend telemetry it already emits
# (station "eval_<node>"), then choose the default from data.
NODE_EVAL = os.environ.get("OCEAN_PIPELINE_NODE_EVAL", "").lower() in ("1", "true", "yes")
# Which nodes get evaluated. Only nodes with a `_eval_node(...)` call site can actually be scored --
# naming one here that has no call site is silently inert, which is why `test_eval_nodes_all_have_a_
# call_site` asserts this list against the source rather than trusting it.
EVAL_NODES = tuple(n.strip() for n in os.environ.get("OCEAN_PIPELINE_EVAL_NODES", "coder").split(",")
                   if n.strip())
# node-evaluator.md's own policy line: "ADVISORY by default; a coder FAIL can gate a rework only
# when OCEAN_PIPELINE_EVAL_ENFORCE is on." OFF means every evaluation is recorded and surfaced but
# routes nothing -- so turning NODE_EVAL on can never, by itself, change where a run goes.
EVAL_ENFORCE = os.environ.get("OCEAN_PIPELINE_EVAL_ENFORCE", "").lower() in ("1", "true", "yes")
# Bounces back to the coder on an enforced FAIL before giving up. Same reasoning and the same
# `attempts > MAX` comparison as MAX_QUALITY_GATE_ATTEMPTS above -- read that comment before
# changing either side.
MAX_EVAL_ATTEMPTS = int(os.environ.get("OCEAN_PIPELINE_MAX_EVAL_ATTEMPTS", "1"))

# Optional human-approval gate before the ready-flip. Default OFF (auto-flip on green, the
# intended terminal action). When ON, the graph interrupt()s and waits for an engineer to
# resume with an approve/reject decision — the pipeline still never merges or deploys.
# NOTE (Findings 2d/2e): OFF is no longer an unconditional auto-flip. human_gate ALWAYS pauses,
# regardless of this setting, when the green cannot be trusted on its own — a run that didn't
# exercise the change against real services (nodes._real_service_gap: Rung < 2, unrecognized
# execution_mode, or a changed repo that didn't really run), or whose pass followed a mid-run test
# edit (nodes._test_edit_ack_reason). Unattended/batch operators should expect those pauses; the
# reason is always stated in the pause message.
REQUIRE_APPROVAL = os.environ.get("OCEAN_PIPELINE_REQUIRE_APPROVAL", "").lower() in ("1", "true", "yes")

# SIT QA review gate. After the SIT scenarios + sample test are drafted, a human reviews them and
# makes the same 3-way call ocean-qa-agent offers interactively: approve-with-TestRail /
# approve-without-TestRail / changes-needed. Default ON (the graph interrupt()s and waits). Set
# QA_REVIEW_AUTO for a hands-off run (no pause) — it then auto-approves, creating TestRail cases
# only if QA_TESTRAIL is on. The TestRail branch runs in PARALLEL with the local run (TestRail's
# API is slow + rate-limited, so it must not block the functional gate).
QA_REVIEW_AUTO = os.environ.get("OCEAN_PIPELINE_QA_AUTOAPPROVE", "").lower() in ("1", "true", "yes")
QA_TESTRAIL = os.environ.get("OCEAN_PIPELINE_TESTRAIL", "").lower() in ("1", "true", "yes")
MAX_QA_REVIEW_ITERATIONS = int(os.environ.get("OCEAN_PIPELINE_MAX_QA_REVIEW_ITERATIONS", "3"))

# Shared coding-attempts budget for the code_fault full-loop
# (Station 6 code_fault -> fk-coder -> Station 5 re-review -> Station 6).
MAX_CODING_ATTEMPTS = 2

# How many times the graph will onboard an unsupported ocean repo (learn_repo) and re-run
# Station 6 before giving up. 1 is enough for the normal case (learn once, re-run once); a
# repo that still reports unsupported after being profiled is a genuine could_not_verify stop.
MAX_ONBOARD_ATTEMPTS = int(os.environ.get("OCEAN_PIPELINE_MAX_ONBOARD_ATTEMPTS", "1"))

# How many times the graph will retry Station 6 (sit_run only, not the full loop) after an
# AGENT-DIAGNOSED environment_failure (Docker/mock/network broke, an image was stale, infra didn't
# come up cleanly) before giving up. Does NOT apply to the deterministic resource-insufficient
# preflight short-circuit (preflight_failed=True) -- more Docker memory doesn't appear between
# attempts, so that case never retries regardless of this budget (see graph.py::after_sit_triage).
MAX_ENV_RETRY_ATTEMPTS = int(os.environ.get("OCEAN_PIPELINE_MAX_ENV_RETRY_ATTEMPTS", "1"))

# Wall-clock cap for the prep_image pre-warm (a cold Ruby image build can be minutes). On timeout the
# pre-warm is abandoned best-effort and the coder builds normally -- pre-warm never blocks the run.
# B1: a COLD MMCUW build (many private FK git-gems, each a clone + native build) exceeds 15 min, so 900s
# made prep_image ALWAYS time out on a fresh lockfile → orphaned build (B2) + a duplicate rebuild by
# prep_container (B3). 1800s lets a cold build finish in-stage. (EXE-6fca4a71.)
IMAGE_PREWARM_TIMEOUT = int(os.environ.get("OCEAN_PIPELINE_IMAGE_PREWARM_TIMEOUT", "1800"))

# I8 (run-monitoring-findings-06af6088.md): prep_image now clones a repo's first-ever local
# checkout (via `gh repo clone`) so it gets a chance to pre-warm before the coder does -- bounded
# separately from IMAGE_PREWARM_TIMEOUT since this is a plain git clone, not a Docker build.
CLONE_PREWARM_TIMEOUT = int(os.environ.get("OCEAN_PIPELINE_CLONE_PREWARM_TIMEOUT", "300"))

# Local mock-first SIT: the LocalStack SQS endpoint the test-automation SQS client must target.
# Without this exported, that client silently constructs as None and crashes on `.meta`, so the
# SQS-driven leg of a multi-repo callback E2E (e.g. ocean-worker's TRACKING_UNIT_UPDATED consumer)
# never assembles and the ticket lands could_not_verify (root cause of EXE-c5ec3e4c / MM-13437).
# Exported into every worker/skill subprocess by agents._drive so the SIT pytest inherits it.
SQS_ENDPOINT_URL = os.environ.get("OCEAN_PIPELINE_SQS_ENDPOINT_URL", "http://localhost:4566")
SQS_LOCAL_ACCOUNT = os.environ.get("OCEAN_PIPELINE_SQS_LOCAL_ACCOUNT", "723008196684")

# Docker resource pre-flight (ocean-qa-agent-ac-driven-plan.md Workstream 3.4). MM-13437's full
# multi-repo callback E2E attempt ground for ~40 minutes before hitting the documented 8-12 GB
# memory ceiling (local_service_execution.md "Docker memory ceiling") — this check catches that in
# ~1s BEFORE sit_run spends an entire expensive agent invocation attempting a run that's going to
# OOM. Conservative floor, not an exact per-ticket requirement (the skill's own resolve step is the
# one that knows the actual repo count/complexity for THIS ticket) — it exists to catch the clear-fail
# case fast, not to replace the skill's own judgment.
MIN_DOCKER_MEMORY_GB = float(os.environ.get("OCEAN_PIPELINE_MIN_DOCKER_MEMORY_GB", "4"))
MIN_DOCKER_CPUS = int(os.environ.get("OCEAN_PIPELINE_MIN_DOCKER_CPUS", "2"))

# Bash-tool timeout ceiling for the SDK-spawned worker (agents.py sets these in the worker env; the
# spawned claude CLI honors them). An SDK worker's Bash otherwise has NO effective timeout, so a runaway
# command hangs the station indefinitely (EXE-0417bc97: a whole-disk `grep /` ran 30+ min). /fk-execute
# never hit this — it runs under the standard interactive Bash tool, which already enforces a timeout.
# DEFAULT = ceiling for a command that doesn't set its own; MAX = the most the model may request for a
# known-slow step (cold docker build, full suite). Generous vs the ocean long-poles, far below a runaway.
BASH_DEFAULT_TIMEOUT_MS = os.environ.get("OCEAN_PIPELINE_BASH_DEFAULT_TIMEOUT_MS", "600000")    # 10 min
BASH_MAX_TIMEOUT_MS = os.environ.get("OCEAN_PIPELINE_BASH_MAX_TIMEOUT_MS", "1200000")            # 20 min

# MM-14628: the node-level preflight scales its budget to the CHANGED/target-repo set (1..N repos all
# run real) instead of the fixed MIN_DOCKER_* floor. This MIRRORS the skill's
# fk-aideveloper/skills/ocean-qa-agent/tools/docker_preflight.py (REPO_FOOTPRINT_GB + budget_for_repos)
# so the deterministic node gate and the skill's Station-2 `--repos` gate agree. Ruby repos run in Docker
# and dominate; Go/Java run native (0 Docker-VM footprint). Keep in sync with docker_preflight.py /
# ocean-repos.md when a repo is onboarded.
_REPO_FOOTPRINT_GB = {
    # Ruby (Docker-only, heavy) — the only repos that consume Docker VM memory
    "ocean-worker": 2.5, "multimodal-worker": 2.5, "multimodal-carrier-updates-worker": 3.0,
    "tracking-service": 2.5, "global_worker": 2.5,
    # Go / Java (native — 0 Docker VM memory; host RAM is not what the gate measures)
    "ocean-service": 0.0, "booking-service": 0.0, "notification-worker": 0.0,
    "eta-service": 0.0, "eta-worker": 0.0,
}
_DEFAULT_REPO_GB = 2.5     # unknown/just-onboarded repo -> assume a heavy Docker-Ruby footprint (safe over-estimate)
_BASE_INFRA_GB = 3.0       # shared kafka/postgres/redis/es/localstack/mock floor present in any Docker chain


def docker_budget_for_repos(target_repos) -> tuple[float, int]:
    """(min_gb, min_cpus) to run every repo in `target_repos` real on one shared Docker infra stack:
    the infra floor plus each DOCKER-run repo's footprint (native Go/Java add 0), one CPU per Docker repo
    beyond the first — never below the MIN_DOCKER_* floor. `target_repos` is the state list of
    {repo, language, build_env} dicts. Mirrors docker_preflight.py::budget_for_repos so the node preflight
    and the skill preflight agree. Empty/None -> the plain MIN_DOCKER_* floor (backward-compatible)."""
    names = [(r.get("repo") or "").split("/")[-1] for r in (target_repos or [])
             if isinstance(r, dict) and r.get("repo")]
    footprints = [_REPO_FOOTPRINT_GB.get(n, _DEFAULT_REPO_GB) for n in names]
    docker_count = sum(1 for f in footprints if f > 0)
    gb = _BASE_INFRA_GB + sum(footprints) if names else MIN_DOCKER_MEMORY_GB
    cpus = 2 + max(0, docker_count - 1)
    return round(max(gb, MIN_DOCKER_MEMORY_GB), 1), max(cpus, MIN_DOCKER_CPUS)


def docker_budget_for_build(target_repos) -> float:
    """(min_gb) to run a single BUILD-type stage (prep_image/prep_container's boot, or coder/
    harsh_reviewer/reachability_gate's Ruby bundle-install-in-Docker) for one repo out of
    `target_repos`. Unlike docker_budget_for_repos, this is deliberately NOT the SIT-stage budget:
    no _BASE_INFRA_GB floor (a build/coder session doesn't bring up the shared kafka/es/localstack/
    mock stack SIT does — only its own image/container), and max() not sum() (prep_container/coder
    operate on ONE Ruby repo's image/container at a time, matching prep_container's own single-repo
    selection — never every target repo's footprint added together). Empty/None -> a small flat
    floor (nothing Docker-heavy to budget for)."""
    names = [(r.get("repo") or "").split("/")[-1] for r in (target_repos or [])
             if isinstance(r, dict) and r.get("repo")]
    footprints = [_REPO_FOOTPRINT_GB.get(n, _DEFAULT_REPO_GB) for n in names if _REPO_FOOTPRINT_GB.get(n, _DEFAULT_REPO_GB) > 0]
    return round(max(footprints) if footprints else 0.5, 1)

# Claude Agent SDK permission mode. This pipeline runs fully headless — every
# station shells out (git push, gh pr create/ready, docker, pytest), and "acceptEdits"
# only auto-approves Edit/Write, NOT Bash, so a non-bypass mode would stall with no
# approver. "bypassPermissions" is the correct default for autonomous operation; it
# is what makes the pipeline able to push branches / open PRs unattended. Tighten via
# env (and a can_use_tool allowlist) only in environments that require it.
STATION_PERMISSION_MODE = os.environ.get("OCEAN_PIPELINE_PERMISSION_MODE", "bypassPermissions")
SKILL_PERMISSION_MODE = os.environ.get("OCEAN_PIPELINE_SKILL_PERMISSION_MODE", "bypassPermissions")

# Boards the ocean-pipeline handles end-to-end. Scoped to the MM (Ocean) board only for now — the
# other isbu boards (ANG/RAIL/INTMOD/BAR/ISBUETA/ISAI/DO) run the non-isbu coding-only default in
# fk-execute (draft PR). Widen this set when the ocean-pipeline is rolled out to them.
ISBU_PROJECTS = {"MM"}

# Isolate the spawned claude CLI from UNREACHABLE ambient MCP servers. Each station shells out to a
# `claude` CLI subprocess (via the claude-agent-sdk); by default that subprocess inherits the ambient
# MCP config from ~/.claude.json, which can list servers whose host is currently DOWN (e.g. fk-code-graph
# at http://neo4j-mcp-server.fourkites.internal/mcp, which fails DNS with NXDOMAIN). When the CLI hangs/
# crashes trying to connect to a dead server, the SDK's transport pipe to the subprocess breaks and the
# station dies with `BrokenPipeError: [Errno 32] Broken pipe` at a station transition (2 of 3 recent SIT
# runs). When ON (default), agents._station_mcp_config() probes each ambient MCP server's reachability
# ONCE per process and, if it can drop the dead ones while keeping >=1 reachable, hands the spawned CLI an
# explicit reachable-only mcp_servers set with strict_mcp_config=True (so the CLI ignores the broken
# ambient config). Fully fallback-safe: if ~/.claude.json can't be read, probing errors, nothing is dead,
# or every server looks unreachable (likely a total false-negative), it sets NOTHING and today's ambient
# behavior is preserved — this can never make things worse. Disable per-run with
# OCEAN_PIPELINE_ISOLATE_UNREACHABLE_MCP=0.
ISOLATE_UNREACHABLE_MCP = os.environ.get("OCEAN_PIPELINE_ISOLATE_UNREACHABLE_MCP", "1").lower() in ("1", "true", "yes")

# Node-level resilience. A single transient claude-CLI/SDK failure (e.g. a ProcessError
# that the SDK surfaces as `Claude Code returned an error result: ...`, seen in run.log for
# MM-14472) must NOT abort a whole run. The agent/skill driver retries with exponential
# backoff before giving up; the checkpointer still allows a full --resume if all retries fail.
# Default 4 (raised from 2): cheap insurance for a transient BURST (e.g. an MCP server flapping
# during a station transition) — ISOLATE_UNREACHABLE_MCP above removes the PERSISTENT dead-server
# case, and these extra retries cover the remaining short-lived transport blips.
MAX_AGENT_RETRIES = int(os.environ.get("OCEAN_PIPELINE_MAX_AGENT_RETRIES", "4"))
AGENT_RETRY_BACKOFF_SECONDS = float(os.environ.get("OCEAN_PIPELINE_AGENT_RETRY_BACKOFF", "3"))

# Default GitHub org for FK service repos. Branches are pushed to upstream, never forked
# (see the guardrails), so the git/PR code nodes address repos as `<org>/<name>`.
DEFAULT_REPO_ORG = os.environ.get("OCEAN_PIPELINE_REPO_ORG", "cloudqwest")

# gitops.py's `gh` subprocess timeout. Without one, a hung/stalled `gh` call (auth prompt,
# network stall) blocks its node — and the whole run — forever, with no way to recover short
# of a hard kill. 30s comfortably covers a real PR create/list/edit/ready round-trip.
GH_TIMEOUT_SECONDS = float(os.environ.get("OCEAN_PIPELINE_GH_TIMEOUT_SECONDS", "30"))


def artifacts_dir(execution_id: str) -> Path:
    d = ARTIFACTS_ROOT / execution_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def workspace_dir(execution_id: str) -> Path:
    """Per-run workspace the coder clones the target repo into. The graph owns this location
    (rather than letting the worker pick an opaque sandbox) so the reviewer and a rework coder
    run against the SAME working tree — the coder reports the clone path back as repo_dir."""
    d = ARTIFACTS_ROOT / execution_id / "workspace"
    d.mkdir(parents=True, exist_ok=True)
    return d


def automation_verdict_path(ticket_id: str) -> Path:
    """Canonical machine-readable verdict the ocean-automation-testing skill writes
    (SKILL.md Station 3). This orchestrator reads it rather than imposing its own schema."""
    return FK_AIDEVELOPER_DIR / "memory" / "tickets" / f"{ticket_id}-automation-testing.json"


def qa_scenarios_path(ticket_id: str) -> Path:
    """GAN-hardened scenario artifact ocean-qa-agent writes on `--scenarios-only` (SKILL.md Step
    5d/10, MM-14738). The qa_scenarios node writes this pre-code; sit_author reads it back and
    passes it down as `--use-scenarios` so the pytest is written from these, not designed fresh."""
    return FK_AIDEVELOPER_DIR / "memory" / "tickets" / f"{ticket_id}-qa-scenarios.json"
