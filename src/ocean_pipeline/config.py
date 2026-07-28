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

# Slim, single-job worker prompts VENDORED into this repo (Phase B onward): the graph owns
# these workers, stripped of the fk-aideveloper station process. run_agent resolves an
# agent_md here FIRST, falling back to AGENTS_DIR for nodes not yet migrated. Named "workers"
# (not "agents") to avoid clashing with the agents.py module in the same package.
VENDORED_AGENTS_DIR = Path(__file__).resolve().parent / "workers"

# Per-run artifact root (reachability-report.json, per-station verdict.json, etc.)
ARTIFACTS_ROOT = Path(os.environ.get("OCEAN_PIPELINE_ARTIFACTS", "/tmp/ocean-pipeline"))

# LangGraph checkpointer DB — durable resume + the AP-223 orphan fix.
CHECKPOINT_DB = os.environ.get("OCEAN_PIPELINE_CHECKPOINT_DB", str(ARTIFACTS_ROOT / "checkpoints.sqlite"))

# Default model for station agents. Always the latest capable Opus unless overridden.
STATION_MODEL = os.environ.get("OCEAN_PIPELINE_MODEL", "claude-opus-4-8")

# Console log detail. Three audiences, one run:
#   management  — top station headers + one-line outcome per station only (no milestones,
#                 no per-station detail bullets, no raw agent activity). For a non-engineer
#                 skimming progress.
#   team        — (default) headers + outcome + the curated milestone/detail lines already
#                 built for this log (significant tool calls, a handful of facts per station).
#   developer   — team, plus the full raw per-message agent activity (every tool call with
#                 its args, every tool result, every thinking/text block) — the actual
#                 --verbose firehose, for debugging a stuck or misbehaving station.
# Toggled by env, or the CLI --log-level flag (--verbose is shorthand for --log-level developer).
_LOG_LEVELS = ("management", "team", "developer")
_env_log_level = os.environ.get("OCEAN_PIPELINE_LOG_LEVEL", "").strip().lower()
if _env_log_level not in _LOG_LEVELS:
    # Back-compat: the old boolean OCEAN_PIPELINE_VERBOSE still selects "developer".
    _env_log_level = "developer" if os.environ.get("OCEAN_PIPELINE_VERBOSE", "").lower() in ("1", "true", "yes") else "team"
LOG_LEVEL = _env_log_level

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
