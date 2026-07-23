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
import os
from pathlib import Path
from typing import Type, TypeVar

from pydantic import BaseModel

from . import config, metrics, ui

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


def _classify_bash(command: str) -> str | None:
    """Map a shell command to a readable milestone, or None to suppress it (ls/cat/cd/…)."""
    c = command.strip().lower()
    checks = [
        ("git clone", "cloning the target repo"),
        ("checkout -b", "creating the ticket branch"),
        ("git commit", "committing changes"),
        ("git push", "pushing the branch"),
        ("gh pr create", "opening the draft PR"),
        ("gh pr ready", "marking the PR ready for review"),
        ("gh pr edit", "cross-linking the test PR"),
        ("gh pr list", "checking for an existing PR"),
        ("route_local", "repointing config to local + mocks"),
        ("ocean_mock_helper", "starting the mock server"),
        ("start-infra", "bringing up local Docker infra"),
        ("docker compose up", "bringing up local Docker infra"),
        ("docker-compose up", "bringing up local Docker infra"),
        ("bundle install", "installing gems (Docker)"),
        ("rspec", "running Ruby unit tests"),
        ("pytest", "running the SIT (pytest)"),
        ("generate_test_report", "generating the test report"),
        ("stop-infra", "tearing down infra"),
        ("mvn ", "building/testing (Maven)"),
        ("go build", "building (Go)"),
        ("go test", "testing (Go)"),
    ]
    for needle, label in checks:
        if needle in c:
            return label
    return None


def _count_tools(msg) -> int:
    content = getattr(msg, "content", None)
    if not isinstance(content, list):
        return 0
    return sum(1 for b in content if getattr(b, "name", None) is not None)


def _usage(msg) -> tuple[float, int, int]:
    """Best-effort (cost_usd, input_tokens, output_tokens) from a ResultMessage."""
    cost = getattr(msg, "total_cost_usd", None) or getattr(msg, "cost_usd", None) or 0.0
    u = getattr(msg, "usage", None)
    in_tok = out_tok = 0
    if u is not None:
        in_tok = getattr(u, "input_tokens", None)
        out_tok = getattr(u, "output_tokens", None)
        if in_tok is None and isinstance(u, dict):
            in_tok, out_tok = u.get("input_tokens", 0), u.get("output_tokens", 0)
    try:
        return float(cost or 0.0), int(in_tok or 0), int(out_tok or 0)
    except (TypeError, ValueError):
        return 0.0, 0, 0


def _milestones(msg) -> list[str]:
    """Curated, human-readable actions from an agent message — the significant tool
    calls only (edits, git/gh, docker, tests, MCP queries); noise is dropped."""
    content = getattr(msg, "content", None)
    if not isinstance(content, list):
        return []
    out: list[str] = []
    for block in content:
        name = getattr(block, "name", None)
        if name is None:
            continue
        inp = getattr(block, "input", None) or {}
        if not isinstance(inp, dict):
            inp = {}
        if name == "Bash":
            m = _classify_bash(str(inp.get("command", "")))
            if m:
                out.append(m)
        elif name in ("Write", "Edit", "MultiEdit"):
            fp = inp.get("file_path") or inp.get("path") or ""
            out.append(f"editing {os.path.basename(str(fp))}" if fp else "editing a file")
        elif name.startswith("mcp__"):
            svc = name.split("__")[1] if "__" in name else name
            out.append(f"querying {svc}")
        elif name == "Task":
            out.append(f"dispatching sub-agent: {inp.get('description', 'subtask')}")
    return out


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
    ui.station_start(label)   # "▶ <station>" header; milestones stream underneath
    tools = 0
    cost = in_tok = out_tok = 0.0
    async for message in query(prompt=prompt, options=options):
        # Default: curated, readable milestones (the significant actions).
        for m in _milestones(message):
            ui.milestone(m)
        tools += _count_tools(message)
        c, i, o = _usage(message)
        cost += c
        in_tok += i
        out_tok += o
        # --verbose: also dump the raw per-message activity for debugging.
        if config.VERBOSE:
            for line in _format_message(message):
                _emit(label, line)
    spend = metrics.fmt(cost, int(in_tok), int(out_tok), tools)
    if spend:
        ui.milestone(f"done — {spend}")
    metrics.add(cost, int(in_tok), int(out_tok), tools)


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
