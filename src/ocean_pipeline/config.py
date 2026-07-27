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

# When true, stream each station agent's inner activity (tool calls + text) to the log,
# so a long-running node isn't a black box. Toggled by env or the CLI --verbose flag.
VERBOSE = os.environ.get("OCEAN_PIPELINE_VERBOSE", "").lower() in ("1", "true", "yes")

# RCA-only mode: run research -> ocean-rca report and STOP after the report, even when
# the root cause needs a code fix (do NOT auto-proceed to coding). This preserves the
# staged control-plane workflow where "In RCA" analyses + reports, and the fix is a
# separate run once the ticket moves to "RCA Done". Toggled by env (set by the
# oas-autodev spawn-aquaman.sh for the rca action) or the CLI --rca-only flag.
RCA_ONLY = os.environ.get("OCEAN_PIPELINE_RCA_ONLY", "").lower() in ("1", "true", "yes")

# Hard cap on the Station 5 <-> Station 4 review loop (CLAUDE.md: max 2 iterations).
MAX_REVIEW_ITERATIONS = 2

# Optional human-approval gate before the ready-flip. Default OFF (auto-flip on green, the
# intended terminal action). When ON, the graph interrupt()s and waits for an engineer to
# resume with an approve/reject decision — the pipeline still never merges or deploys.
REQUIRE_APPROVAL = os.environ.get("OCEAN_PIPELINE_REQUIRE_APPROVAL", "").lower() in ("1", "true", "yes")

# Shared coding-attempts budget for the code_fault full-loop
# (Station 6 code_fault -> fk-coder -> Station 5 re-review -> Station 6).
MAX_CODING_ATTEMPTS = 2

# How many times the graph will onboard an unsupported ocean repo (learn_repo) and re-run
# Station 6 before giving up. 1 is enough for the normal case (learn once, re-run once); a
# repo that still reports unsupported after being profiled is a genuine could_not_verify stop.
MAX_ONBOARD_ATTEMPTS = int(os.environ.get("OCEAN_PIPELINE_MAX_ONBOARD_ATTEMPTS", "1"))

# Claude Agent SDK permission mode. This pipeline runs fully headless — every
# station shells out (git push, gh pr create/ready, docker, pytest), and "acceptEdits"
# only auto-approves Edit/Write, NOT Bash, so a non-bypass mode would stall with no
# approver. "bypassPermissions" is the correct default for autonomous operation; it
# is what makes the pipeline able to push branches / open PRs unattended. Tighten via
# env (and a can_use_tool allowlist) only in environments that require it.
STATION_PERMISSION_MODE = os.environ.get("OCEAN_PIPELINE_PERMISSION_MODE", "bypassPermissions")
SKILL_PERMISSION_MODE = os.environ.get("OCEAN_PIPELINE_SKILL_PERMISSION_MODE", "bypassPermissions")

# isbu profile boards that run the full end-to-end pipeline (per CLAUDE.md).
ISBU_PROJECTS = {"MM", "ANG", "RAIL", "INTMOD", "BAR", "ISBUETA", "ISAI", "DO"}

# Node-level resilience. A single transient claude-CLI/SDK failure (e.g. a ProcessError
# that the SDK surfaces as `Claude Code returned an error result: ...`, seen in run.log for
# MM-14472) must NOT abort a whole run. The agent/skill driver retries with exponential
# backoff before giving up; the checkpointer still allows a full --resume if all retries fail.
MAX_AGENT_RETRIES = int(os.environ.get("OCEAN_PIPELINE_MAX_AGENT_RETRIES", "2"))
AGENT_RETRY_BACKOFF_SECONDS = float(os.environ.get("OCEAN_PIPELINE_AGENT_RETRY_BACKOFF", "3"))

# Default GitHub org for FK service repos. Branches are pushed to upstream, never forked
# (see the guardrails), so the git/PR code nodes address repos as `<org>/<name>`.
DEFAULT_REPO_ORG = os.environ.get("OCEAN_PIPELINE_REPO_ORG", "cloudqwest")


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
