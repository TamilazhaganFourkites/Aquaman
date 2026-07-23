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

# Per-run artifact root (reachability-report.json, per-station verdict.json, etc.)
ARTIFACTS_ROOT = Path(os.environ.get("OCEAN_PIPELINE_ARTIFACTS", "/tmp/ocean-pipeline"))

# LangGraph checkpointer DB — durable resume + the AP-223 orphan fix.
CHECKPOINT_DB = os.environ.get("OCEAN_PIPELINE_CHECKPOINT_DB", str(ARTIFACTS_ROOT / "checkpoints.sqlite"))

# Default model for station agents. Always the latest capable Opus unless overridden.
STATION_MODEL = os.environ.get("OCEAN_PIPELINE_MODEL", "claude-opus-4-8")

# When true, stream each station agent's inner activity (tool calls + text) to the log,
# so a long-running node isn't a black box. Toggled by env or the CLI --verbose flag.
VERBOSE = os.environ.get("OCEAN_PIPELINE_VERBOSE", "").lower() in ("1", "true", "yes")

# Hard cap on the Station 5 <-> Station 4 review loop (CLAUDE.md: max 2 iterations).
MAX_REVIEW_ITERATIONS = 2

# Shared coding-attempts budget for the code_fault full-loop
# (Station 6 code_fault -> fk-coder -> Station 5 re-review -> Station 6).
MAX_CODING_ATTEMPTS = 2

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


def artifacts_dir(execution_id: str) -> Path:
    d = ARTIFACTS_ROOT / execution_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def automation_verdict_path(ticket_id: str) -> Path:
    """Canonical machine-readable verdict the ocean-automation-testing skill writes
    (SKILL.md Station 3). This orchestrator reads it rather than imposing its own schema."""
    return FK_AIDEVELOPER_DIR / "memory" / "tickets" / f"{ticket_id}-automation-testing.json"
