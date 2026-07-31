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

# Per-run artifact root (reachability-report.json, per-station verdict.json, etc.)
ARTIFACTS_ROOT = Path(os.environ.get("OCEAN_PIPELINE_ARTIFACTS", "/tmp/ocean-pipeline"))

# Durable per-run station-timing log (one JSON line per finished run). ARTIFACTS_ROOT lives under
# /tmp and gets cleaned, losing the run-report timings; this log persists them so before/after
# latency comparisons survive. Out of any git repo by default.
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
# killed with no verdict (manual-findings #4 / tracker #1+#16). DEFAULT OFF + fully fallback-safe: the
# test-group child build is best-effort; if it fails, prep falls back to the prod image and the station
# installs as it does today. Turn on per-run with OCEAN_PIPELINE_BAKE_TEST_GROUP=1 to validate, then flip
# the default once a run confirms the -test image boots green. Pair with OCEAN_IMAGE_PLATFORM=linux/arm64
# on Apple Silicon (ruby_image_cache reads it) to also drop Rosetta (manual-findings #15).
BAKE_TEST_GROUP = os.environ.get("OCEAN_PIPELINE_BAKE_TEST_GROUP", "0").lower() in ("1", "true", "yes")


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
STATION_MODEL = os.environ.get("OCEAN_PIPELINE_MODEL", "claude-opus-4-8")

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

# RCA review gate. rca_agent already posts its 5-part evidence-cited report as a Jira comment,
# but nothing gated on it before this — an LLM's own root-cause conclusion could silently kick
# off an entire autonomous coding run (fix_needed=true) with no human having read the analysis
# first. Default ON (the graph interrupt()s and waits) — unlike REQUIRE_APPROVAL below, this is
# the FIRST checkpoint before autonomous work starts, not the last one before a already-tested
# PR ships, so it defaults to the more conservative posture (same as QA_REVIEW_AUTO's gate).
# Set for the headless --rca-only control-plane flow (oas-autodev spawn-aquaman.sh) if it should
# run unattended.
RCA_REVIEW_AUTO = os.environ.get("OCEAN_PIPELINE_RCA_REVIEW_AUTO", "").lower() in ("1", "true", "yes")

# Hard cap on the Station 5 <-> Station 4 review loop (CLAUDE.md: max 2 iterations).
MAX_REVIEW_ITERATIONS = 2

# Optional human-approval gate before the ready-flip. Default OFF (auto-flip on green, the
# intended terminal action). When ON, the graph interrupt()s and waits for an engineer to
# resume with an approve/reject decision — the pipeline still never merges or deploys.
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
IMAGE_PREWARM_TIMEOUT = int(os.environ.get("OCEAN_PIPELINE_IMAGE_PREWARM_TIMEOUT", "900"))

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

# Node-level resilience. A single transient claude-CLI/SDK failure (e.g. a ProcessError
# that the SDK surfaces as `Claude Code returned an error result: ...`, seen in run.log for
# MM-14472) must NOT abort a whole run. The agent/skill driver retries with exponential
# backoff before giving up; the checkpointer still allows a full --resume if all retries fail.
MAX_AGENT_RETRIES = int(os.environ.get("OCEAN_PIPELINE_MAX_AGENT_RETRIES", "2"))
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
