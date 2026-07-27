"""Run one pipeline node's worker as a Claude Agent SDK subprocess.

LangGraph owns the *process* (sequence, routing, loops, retries, gates); these entry
points only run a single narrow worker for one node and hand its result back to the graph.
No station/orchestration logic is expressed here — in particular there is no "you are
Station X, load your Inputs, pull the next skill" framing (that was the fk-aideveloper
*station process* the graph now replaces). The worker gets operating guardrails + one
node-scoped task, nothing more.

  run_agent(agent_md=...)   -> runs an agents/pipeline/fk-*.md worker for one node, injects
                               the verdict contract, and reads back a validated
                               <node>.verdict.json.
  run_skill(skill_name=...) -> runs a skills/<name>/SKILL.md skill headless. The skill defines
                               its OWN output file/schema (e.g. ocean-automation-testing writes
                               memory/tickets/<TICKET>-automation-testing.json), so no verdict
                               contract is injected; the caller reads that canonical file.

Both retry with exponential backoff so a transient claude-CLI/SDK failure does not abort the run.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Type, TypeVar

from pydantic import BaseModel

from . import config, metrics, ui

T = TypeVar("T", bound=BaseModel)


class StationError(RuntimeError):
    """A node's worker failed to run or to produce a valid verdict. Carries node
    context so the top-level handler can emit a clean failed telemetry row."""

    def __init__(self, node: str, source: str, detail: str):
        super().__init__(f"node {node} ({source}): {detail}")
        self.node = node
        self.source = source
        self.detail = detail

# Operating guardrails handed to every worker. These are SAFETY constraints, not process
# framing — the graph decides what runs when; the worker only obeys these while doing its one job.
AGENT_GUARDRAILS = """\
Operating guardrails for ticket {ticket_id}:
- Never fork cloudqwest repos -- push branches directly to upstream
- Every commit must include {ticket_id} in the message
- Never write FK service code into fk-aideveloper -- clone the target repo
- Parameterized queries only -- no SQL string concatenation
- This pipeline never auto-merges or auto-deploys; the engineer owns final merge, sign-off, deploy

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


def _agent_path(agent_md: str) -> Path:
    """Resolve a worker prompt: prefer the vendored slim worker owned by this repo, and fall
    back to the fk-aideveloper station agent for any node not yet migrated (Phase B is
    incremental — one node at a time points at a vendored file here)."""
    vendored = config.VENDORED_AGENTS_DIR / agent_md
    return vendored if vendored.exists() else config.AGENTS_DIR / agent_md


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

    # Signal to worker skills that they're running UNDER the control plane (inherited by the
    # spawned CLI subprocess). The ocean-qa-agent's learn-a-repo gate keys off this: under the
    # control plane a normal SIT run reports-only (needs_onboarding) and the graph's learn_repo
    # node owns onboarding; a standalone `/ocean-qa-agent` run (no Aquaman, flag unset) self-onboards.
    os.environ["OCEAN_PIPELINE_CONTROL_PLANE"] = "1"

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


async def _drive_with_retry(*, system_prompt: str, prompt: str, cwd: Path,
                            permission_mode: str, label: str) -> None:
    """Run the worker, retrying on any transient SDK/CLI failure with exponential backoff.

    A single claude-CLI ProcessError (e.g. the `Claude Code returned an error result: ...`
    crash in run.log) must not abort the whole run. Only after MAX_AGENT_RETRIES do we give
    up and let the caller raise StationError."""
    last: Exception | None = None
    for attempt in range(config.MAX_AGENT_RETRIES + 1):
        try:
            await _drive(system_prompt, prompt, cwd, permission_mode, label)
            return
        except Exception as e:  # noqa: BLE001 — retry ANY transport/SDK failure
            last = e
            if attempt >= config.MAX_AGENT_RETRIES:
                break
            delay = config.AGENT_RETRY_BACKOFF_SECONDS * (2 ** attempt)
            ui.milestone(f"transient error ({type(e).__name__}); retrying in {delay:.0f}s "
                         f"(attempt {attempt + 1}/{config.MAX_AGENT_RETRIES})")
            await asyncio.sleep(delay)
    assert last is not None
    raise last


async def run_agent(
    *,
    agent_md: str,
    node: str,
    ticket_id: str,
    execution_id: str,
    task_prompt: str,
    verdict_model: Type[T],
    cwd: Path | None = None,
    permission_mode: str | None = None,
) -> T:
    """Run one agents/pipeline/fk-*.md worker for a single node and return its validated verdict."""
    verdict_path = config.artifacts_dir(execution_id) / f"{node}.verdict.json"
    if verdict_path.exists():
        verdict_path.unlink()

    guardrails = AGENT_GUARDRAILS.format(ticket_id=ticket_id)
    contract = VERDICT_INSTRUCTION.format(
        verdict_path=verdict_path,
        schema=json.dumps(verdict_model.model_json_schema(), indent=2),
    )
    try:
        await _drive_with_retry(
            system_prompt=_read(_agent_path(agent_md)),
            prompt=f"{guardrails}\n\n{task_prompt}\n{contract}",
            cwd=cwd or config.FK_AIDEVELOPER_DIR,
            permission_mode=permission_mode or config.STATION_PERMISSION_MODE,
            label=node,
        )
    except Exception as e:  # noqa: BLE001 — normalize any SDK/transport failure to StationError
        raise StationError(node, agent_md, f"agent run failed: {type(e).__name__}: {e}") from e

    if not verdict_path.exists():
        raise StationError(node, agent_md, f"no verdict written to {verdict_path}")
    try:
        return verdict_model.model_validate_json(_read(verdict_path))
    except Exception as e:  # noqa: BLE001 — malformed / schema-violating verdict
        raise StationError(node, agent_md, f"invalid verdict json: {type(e).__name__}: {e}") from e


async def run_skill(
    *,
    skill_name: str,
    node: str,
    ticket_id: str,
    task_prompt: str,
    cwd: Path | None = None,
    permission_mode: str | None = None,
) -> None:
    """Run a skills/<skill_name>/SKILL.md skill headless.

    The skill owns its output contract; the caller reads whatever canonical file
    the skill writes. Returns nothing.
    """
    skill_md = config.FK_AIDEVELOPER_DIR / "skills" / skill_name / "SKILL.md"
    guardrails = AGENT_GUARDRAILS.format(ticket_id=ticket_id)
    try:
        await _drive_with_retry(
            system_prompt=_read(skill_md),
            prompt=f"{guardrails}\n\n{task_prompt}",
            cwd=cwd or config.FK_AIDEVELOPER_DIR,
            permission_mode=permission_mode or config.SKILL_PERMISSION_MODE,
            label=node,
        )
    except Exception as e:  # noqa: BLE001 — normalize any SDK/transport failure to StationError
        raise StationError(node, skill_name, f"skill run failed: {type(e).__name__}: {e}") from e
