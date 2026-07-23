"""Run a pipeline station as a Claude Agent SDK subprocess.

Two entry points, both driving the *existing* fk-aideveloper prompts verbatim —
no station logic is re-expressed here:

  run_station(agent_md=...)  -> drives an agents/pipeline/fk-*.md agent, injects
                                the verdict contract, and reads back a validated
                                <station>.verdict.json.
  run_skill(skill_name=...)  -> drives a skills/<name>/SKILL.md skill headless.
                                The skill defines its OWN output file/schema
                                (e.g. ocean-automation-testing writes
                                memory/tickets/<TICKET>-automation-testing.json),
                                so no verdict contract is injected; the caller
                                reads that canonical file.

This keeps CLAUDE.md's model intact: context is minimal, capabilities
(tools/MCP incl. fk-code-graph) are ambient, skills are fact-keyed by the agent.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Type, TypeVar

from pydantic import BaseModel

from . import config

T = TypeVar("T", bound=BaseModel)


class StationError(RuntimeError):
    """A station agent/skill failed to run or to produce a valid verdict. Carries
    station context so the top-level handler can emit a clean failed telemetry row."""

    def __init__(self, station: str, source: str, detail: str):
        super().__init__(f"station {station} ({source}): {detail}")
        self.station = station
        self.source = source
        self.detail = detail

UNIVERSAL_PREFIX = """\
You are operating as part of the FK Lean Manufacturing Pipeline for ticket {ticket_id}.
Your station: {station}. Load context JIT: read only your `## Inputs`, and pull the ONE skill
that matches the detected fact (repo language / ticket domain) — never a skill that does not
match the work. Capabilities (tools/MCP incl. fk-code-graph) are available on demand.

MANDATORY RULES:
- Never fork cloudqwest repos -- push branches directly to upstream
- Every commit must include {ticket_id} in the message
- Never write FK service code into fk-aideveloper -- clone the target repo
- Parameterized queries only -- no SQL string concatenation
- The pipeline never auto-merges or auto-deploys; the engineer owns final merge, sign-off, deploy

Ocean/MM build+test rule (LANGUAGE-SCOPED): Ruby workers (ocean-worker, multimodal-worker,
multimodal-carrier-updates-worker, global_worker; and tracking-service) build/test in Docker ONLY.
Java (eta-service, eta-worker) native is fine. Go (ocean-service, booking-service) native is the
default, Docker fallback.
"""

VERDICT_INSTRUCTION = """\

--- ORCHESTRATION CONTRACT (do this last, exactly once) ---
When your work is complete, write your machine-readable verdict to:
  {verdict_path}
It MUST be a single JSON object matching this schema (no prose, no code fences):
{schema}
Write the file with the Write tool. Do not print the JSON to stdout instead of writing it.
"""


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _emit(label: str, line: str) -> None:
    print(f"    [{label}] {line}", flush=True)


def _format_message(msg) -> list[str]:
    """Best-effort, SDK-shape-tolerant one-liners for a streamed agent message:
    tool calls, assistant/thinking text, and the final result."""
    out: list[str] = []
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        for block in content:
            name = getattr(block, "name", None)        # ToolUseBlock
            text = getattr(block, "text", None)         # Text/ThinkingBlock
            if name is not None:
                inp = getattr(block, "input", None)
                arg = json.dumps(inp, default=str) if inp is not None else ""
                out.append(f"⚙ {name} {arg[:200]}")
            elif text:
                t = " ".join(str(text).split())
                if t:
                    out.append(t[:220])
        return out
    result = getattr(msg, "result", None)               # ResultMessage
    if result:
        out.append(f"✔ {str(result)[:200]}")
    return out


async def _drive(system_prompt: str, prompt: str, cwd: Path, permission_mode: str,
                 label: str = "station") -> None:
    # Imported lazily so the graph/routing test suite runs without the SDK (or the
    # `claude` CLI it spawns) installed — the SDK is only needed at actual run time.
    from claude_agent_sdk import ClaudeAgentOptions, query

    options = ClaudeAgentOptions(
        model=config.STATION_MODEL,
        system_prompt=system_prompt,
        cwd=str(cwd),
        permission_mode=permission_mode,
    )
    async for message in query(prompt=prompt, options=options):
        # The artifact / canonical file is the return channel; with --verbose we also
        # surface the agent's live activity so a multi-minute node isn't a black box.
        if config.VERBOSE:
            for line in _format_message(message):
                _emit(label, line)


async def run_station(
    *,
    agent_md: str,
    station: str,
    ticket_id: str,
    execution_id: str,
    task_prompt: str,
    verdict_model: Type[T],
    cwd: Path | None = None,
    permission_mode: str | None = None,
) -> T:
    """Drive one agents/pipeline/fk-*.md station and return its validated verdict."""
    verdict_path = config.artifacts_dir(execution_id) / f"{station}.verdict.json"
    if verdict_path.exists():
        verdict_path.unlink()

    prefix = UNIVERSAL_PREFIX.format(ticket_id=ticket_id, station=station)
    contract = VERDICT_INSTRUCTION.format(
        verdict_path=verdict_path,
        schema=json.dumps(verdict_model.model_json_schema(), indent=2),
    )
    try:
        await _drive(
            system_prompt=_read(config.AGENTS_DIR / agent_md),
            prompt=f"{prefix}\n\n{task_prompt}\n{contract}",
            cwd=cwd or config.FK_AIDEVELOPER_DIR,
            permission_mode=permission_mode or config.STATION_PERMISSION_MODE,
            label=station,
        )
    except Exception as e:  # noqa: BLE001 — normalize any SDK/transport failure to StationError
        raise StationError(station, agent_md, f"agent run failed: {type(e).__name__}: {e}") from e

    if not verdict_path.exists():
        raise StationError(station, agent_md, f"no verdict written to {verdict_path}")
    try:
        return verdict_model.model_validate_json(_read(verdict_path))
    except Exception as e:  # noqa: BLE001 — malformed / schema-violating verdict
        raise StationError(station, agent_md, f"invalid verdict json: {type(e).__name__}: {e}") from e


async def run_skill(
    *,
    skill_name: str,
    station: str,
    ticket_id: str,
    task_prompt: str,
    cwd: Path | None = None,
    permission_mode: str | None = None,
) -> None:
    """Drive a skills/<skill_name>/SKILL.md skill headless.

    The skill owns its output contract; the caller reads whatever canonical file
    the skill writes. Returns nothing.
    """
    skill_md = config.FK_AIDEVELOPER_DIR / "skills" / skill_name / "SKILL.md"
    prefix = UNIVERSAL_PREFIX.format(ticket_id=ticket_id, station=station)
    await _drive(
        system_prompt=_read(skill_md),
        prompt=f"{prefix}\n\n{task_prompt}",
        cwd=cwd or config.FK_AIDEVELOPER_DIR,
        permission_mode=permission_mode or config.SKILL_PERMISSION_MODE,
        label=station,
    )
