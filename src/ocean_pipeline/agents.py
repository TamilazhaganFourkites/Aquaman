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
import re
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
# NOTE: the build+test rule below is LANGUAGE-scoped (Ruby -> Docker, decided by the repo's language,
# NOT an enumerated repo list) so a NEW Ruby repo is covered automatically — do not turn it back into
# a fixed list. It is deliberately also stated in-context in the vendored worker prompts + the
# learn_repo node prompt (nodes.py): every process that might build/test a Ruby repo must see it in
# its OWN context, or it runs native and fails on old gems. Those in-context copies are KEPT, not
# collapsed into a reference (that would reintroduce the native-run bug); test_language_scoped_docker
# _rule_present keeps any copy from silently losing the rule. Canonical statement of the same rule:
# fk-aideveloper skills/_shared/ocean-knowledge/ocean-repos.md ("Language-scoped build+test rule").
AGENT_GUARDRAILS = """\
Operating guardrails for ticket {ticket_id}:
- Never fork cloudqwest repos -- push branches directly to upstream
- Clone over HTTPS with the gh token: `git clone https://$(gh auth token)@github.com/cloudqwest/<repo>.git`
  -- never `git@github.com:` (SSH keys are not configured; it fails `Permission denied (publickey)`)
- Every commit must include {ticket_id} in the message
- Never write FK service code into fk-aideveloper -- clone the target repo
- Parameterized queries only -- no SQL string concatenation
- Any settings/config you materialize into the workspace (e.g. a copy of an environment-configuration
  `settings.yml`) holds LIVE credentials -- write it mode 0600, never commit it, and never echo its
  contents to logs or the transcript
- This pipeline never auto-merges or auto-deploys; the engineer owns final merge, sign-off, deploy

Ocean/MM build+test rule (LANGUAGE-SCOPED — decided by the repo's LANGUAGE, not a fixed repo list,
so a NEW Ruby repo is covered by the same rule automatically): ANY Ruby ocean repo builds/tests in
Docker ONLY — the host can't resolve old native gems (e.g. nokogiri 1.6.8.1); never present a
native-host Ruby build/test as valid (known Ruby repos today, illustrative not exhaustive:
ocean-worker, multimodal-worker, multimodal-carrier-updates-worker, global_worker, tracking-service).
Java repos (e.g. eta-service, eta-worker) native is fine. Go repos (e.g. ocean-service,
booking-service) native is the default, Docker fallback. If a repo's language is unknown, resolve it
first — then apply this rule by language.
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
    """Resolve a worker/agent prompt from fk-aideveloper (the single source — MM-14620 Q1: the
    control plane holds no worker content). The ocean coding WORKERS (`research.md`, `code.md`, …)
    live in fk-aideveloper's `ocean-coding-agent/workers/`; the ocean domain SMEs (`sme-*.md`) live
    in `agents/pipeline/`. Try the workers dir first, then the SME/station dir.

    If NEITHER location has the file, raise a clear error naming both candidates — a bare
    FileNotFoundError deep inside `_read` doesn't tell the operator where the resolver looked, and
    the usual cause is FK_AIDEVELOPER_DIR being on a branch that doesn't carry the file."""
    worker = config.OCEAN_WORKERS_DIR / agent_md
    if worker.exists():
        return worker
    sme = config.OCEAN_AGENTS_DIR / agent_md
    if sme.exists():
        return sme
    fallback = config.AGENTS_DIR / agent_md
    if fallback.exists():
        return fallback
    raise StationError(
        "agent-resolve", agent_md,
        f"worker/agent prompt {agent_md!r} not found — looked in the ocean-coding-agent workers dir "
        f"({config.OCEAN_WORKERS_DIR}), its agents/SME dir ({config.OCEAN_AGENTS_DIR}), and the "
        f"fk-aideveloper station dir ({config.AGENTS_DIR}). Check FK_AIDEVELOPER_DIR is on the branch "
        f"that carries this file (see the README's version-pinning note).",
    )


_FRONTMATTER_TOOLS_INLINE_RE = re.compile(r'^tools:\s*(\[.*\])\s*$', re.MULTILINE)
# Any `tools:` key line, inline-array or bare (block-list follows on subsequent lines) — used to
# detect "a tools: key exists but couldn't be parsed" so that case fails loudly instead of being
# silently treated the same as "no tools: key at all" (see _frontmatter_tools' docstring).
_FRONTMATTER_TOOLS_KEY_RE = re.compile(r'^tools:.*$', re.MULTILINE)
_FRONTMATTER_BLOCK_ITEM_RE = re.compile(r'^\s*-\s*(.+?)\s*$')


def _frontmatter_tools(path: Path) -> list[str] | None:
    """Extract the `tools:` allowlist from a worker/skill file's YAML frontmatter, if it declares
    one. Supports both styles: inline JSON-array (`tools: ["Read", "Write"]`, used by every real
    fk-aideveloper station/SME file today) and YAML block-list (`tools:\\n  - Read\\n  - Write`).

    Returns None when the file has no frontmatter or no `tools:` field at all — the vendored
    workers/*.md files in this repo don't declare one today, and callers MUST treat that as
    "no restriction", never as an empty allowlist, or every tool call would be denied.

    Raises ValueError when a `tools:` key IS present but neither style parses it — e.g. a
    single-line regex that silently returned None here for an unrecognized style would make a
    file LOOK unrestricted while its author believed it was scoped, which is worse than a loud
    failure. run_agent/run_skill's callers already wrap this in a try/except that normalizes any
    exception to a StationError, so this surfaces as a clear per-node failure, not a crash."""
    text = _read(path)
    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    front = text[:end]

    inline = _FRONTMATTER_TOOLS_INLINE_RE.search(front)
    if inline:
        try:
            tools = json.loads(inline.group(1))
        except json.JSONDecodeError as e:
            raise ValueError(f"{path}: tools: frontmatter is not valid JSON: {inline.group(1)!r} ({e})")
        if not isinstance(tools, list):
            raise ValueError(f"{path}: tools: frontmatter must be a JSON array, got {tools!r}")
        return tools

    key = _FRONTMATTER_TOOLS_KEY_RE.search(front)
    if key:
        items = []
        for line in front[key.end():].splitlines():
            if not line.strip():
                continue
            item = _FRONTMATTER_BLOCK_ITEM_RE.match(line)
            if not item:
                break  # left the block-list (a non-'-' line ends it)
            items.append(item.group(1).strip("\"'"))
        if not items:
            raise ValueError(f"{path}: found a tools: key but could not parse it as an inline "
                             f"JSON array or a YAML block-list")
        return items

    return None  # no tools: key at all -> no restriction, by design


def _ensure_verdict_tool_allowed(allowed: list[str] | None) -> list[str] | None:
    """run_agent's own VERDICT_INSTRUCTION contract mandates every worker write its verdict file
    with the Write tool, regardless of what that worker's own tools: frontmatter declares — the
    worker file was authored before this harness-level contract existed, so it can't have opted
    into naming Write. Without this, any worker whose frontmatter omits Write (e.g. every
    agents/pipeline/sme-*.md file, which lists only read/query tools) would have its own
    mandatory verdict write denied by _deny_outside_allowlist, making run_agent unusable for it.
    None (no declared allowlist -> no restriction) passes through unchanged; every other
    restriction the file DID declare is preserved — only Write is guaranteed on top."""
    if allowed is None:
        return None
    return allowed if "Write" in allowed else [*allowed, "Write"]


def _deny_outside_allowlist(allowed: list[str]):
    """A PreToolUse hook that denies any tool call not in `allowed`.

    Deliberately a hook, not `can_use_tool`: the SDK only ever consults `can_use_tool` for a
    call that would otherwise hit an interactive "ask" prompt, and this pipeline's stations run
    under permission_mode="bypassPermissions" (config.STATION_PERMISSION_MODE /
    SKILL_PERMISSION_MODE) — bypassPermissions auto-approves every tool call before
    `can_use_tool` is ever consulted (the SDK emits CanUseToolShadowedWarning if you wire it up
    alongside bypassPermissions for exactly this reason). A PreToolUse hook is the one
    mechanism the SDK documents as running regardless of permission_mode."""
    allowed_set = set(allowed)

    async def _hook(input_data, tool_use_id, context):  # noqa: ARG001 — SDK hook signature
        tool_name = input_data.get("tool_name", "")
        if tool_name in allowed_set:
            return {}
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"'{tool_name}' is not in this station's declared tools: allowlist "
                    f"({sorted(allowed_set)})."
                ),
            }
        }

    return _hook


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


def _usage(msg) -> tuple[int, int]:
    """Best-effort (input_tokens, output_tokens) from a ResultMessage."""
    u = getattr(msg, "usage", None)
    in_tok = out_tok = 0
    if u is not None:
        in_tok = getattr(u, "input_tokens", None)
        out_tok = getattr(u, "output_tokens", None)
        if in_tok is None and isinstance(u, dict):
            in_tok, out_tok = u.get("input_tokens", 0), u.get("output_tokens", 0)
    try:
        return int(in_tok or 0), int(out_tok or 0)
    except (TypeError, ValueError):
        return 0, 0


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
            parts = name.split("__")
            svc = parts[1] if len(parts) > 1 else name
            tool = "__".join(parts[2:])   # the specific MCP tool, e.g. getJiraIssue
            out.append(f"querying {svc}: {tool}" if tool else f"querying {svc}")
        elif name == "Task":
            out.append(f"dispatching sub-agent: {inp.get('description', 'subtask')}")
    return out


def _format_message(msg) -> list[str]:
    """Best-effort, SDK-shape-tolerant one-liners for a streamed agent message: tool
    calls, tool RESULTS, assistant text, thinking, the sub-agent (Task) lifecycle, and
    the final result.

    Two rounds of "verbose/developer isn't giving all logs" gaps closed here:
      1. ThinkingBlock (.thinking, not .text) and ToolResultBlock (the actual tool
         OUTPUT, e.g. command stdout/stderr) were silently dropped — only ToolUseBlock
         and TextBlock were matched.
      2. The entire SystemMessage family (TaskStartedMessage, TaskProgressMessage,
         TaskNotificationMessage, TaskUpdatedMessage, and any other `system` subtype —
         hook events, rate-limit notices, mirror errors) has NEITHER `.content` nor
         `.result`, so it fell through both checks below and vanished with no output at
         all. That's exactly the sub-agent dispatch story: `_milestones` shows the
         moment a worker calls the Task tool, but everything the sub-agent does after
         that — starting, progressing, finishing — arrives as one of these and was
         completely invisible even at developer level."""
    out: list[str] = []
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        for block in content:
            name = getattr(block, "name", None)            # ToolUseBlock
            text = getattr(block, "text", None)             # TextBlock
            thinking = getattr(block, "thinking", None)     # ThinkingBlock
            tool_result = getattr(block, "tool_use_id", None) is not None  # ToolResultBlock
            if name is not None:
                inp = getattr(block, "input", None)
                arg = json.dumps(inp, default=str) if inp is not None else ""
                out.append(f"⚙ {name} {arg[:200]}")
            elif tool_result:
                rc = getattr(block, "content", None)
                rc_text = rc if isinstance(rc, str) else json.dumps(rc, default=str) if rc is not None else ""
                rc_text = " ".join(rc_text.split())
                marker = "✗" if getattr(block, "is_error", False) else "→"
                out.append(f"{marker} result: {rc_text[:200]}")
            elif thinking:
                t = " ".join(str(thinking).split())
                if t:
                    out.append(f"💭 {t[:220]}")
            elif text:
                t = " ".join(str(text).split())
                if t:
                    out.append(t[:220])
        return out
    # SystemMessage family — see docstring point 2. Gated on BOTH .subtype and .data (a
    # SystemMessage base-class field, inherited by every Task*Message subclass) because
    # ResultMessage ALSO has its own unrelated .subtype (e.g. "success") but no .data —
    # checking .subtype alone would intercept ResultMessage here and silently swallow its
    # actual result text into a useless "system[success]" line instead of falling through
    # to the `result` handling below. Caught by test_format_message_result_message_still_
    # shows_result_text_not_swallowed_by_subtype_check.
    subtype = getattr(msg, "subtype", None)
    if subtype is not None and hasattr(msg, "data"):
        if subtype == "task_started":
            return [f"⚡ sub-agent started: {getattr(msg, 'description', '')} "
                    f"(task {getattr(msg, 'task_id', '')})"]
        if subtype == "task_progress":
            last_tool = getattr(msg, "last_tool_name", None)
            return [f"⚡ sub-agent progress: {getattr(msg, 'description', '')}"
                    + (f" (last tool: {last_tool})" if last_tool else "")]
        if subtype == "task_notification":
            return [f"⚡ sub-agent {getattr(msg, 'status', '?')}: {getattr(msg, 'summary', '')}"]
        if subtype == "task_updated":
            patch = getattr(msg, "patch", None) or {}
            return [f"⚡ sub-agent task update: {json.dumps(patch, default=str)[:200]}"]
        if subtype == "thinking_tokens":
            # A live "still thinking, ~N tokens so far" progress ping the CLI fires roughly
            # every ~50 thinking-tokens — not a discrete event like a tool call or task
            # completion. A single long thinking burst emits dozens of these; printing each
            # one is pure noise (the actual thinking CONTENT still shows via the 💭 line
            # above, from ThinkingBlock — this only ever duplicates the running token count).
            return []
        # Any other `system` subtype (hook events, rate limits, mirror errors, a future
        # kind we haven't named) — surface the raw payload rather than dropping it
        # silently; "developer level" means ALL logs, not just the ones we anticipated.
        data = getattr(msg, "data", None)
        return [f"⚙ system[{subtype}]: {json.dumps(data, default=str)[:200]}" if data else f"⚙ system[{subtype}]"]
    result = getattr(msg, "result", None)               # ResultMessage
    if result:
        out.append(f"✔ {str(result)[:200]}")
    return out


async def _drive(system_prompt: str, prompt: str, cwd: Path, permission_mode: str,
                 label: str = "station", allowed_tools: list[str] | None = None) -> None:
    # Imported lazily so the graph/routing test suite runs without the SDK (or the
    # `claude` CLI it spawns) installed — the SDK is only needed at actual run time.
    from claude_agent_sdk import ClaudeAgentOptions, HookMatcher, query

    # Enforce the worker's own tools: frontmatter (if it declared one) via a PreToolUse hook —
    # see _deny_outside_allowlist for why this has to be a hook and not can_use_tool.
    hooks = ({"PreToolUse": [HookMatcher(hooks=[_deny_outside_allowlist(allowed_tools)])]}
             if allowed_tools is not None else None)

    # Signal to worker skills that they're running UNDER the control plane (inherited by the
    # spawned CLI subprocess). The ocean-qa-agent's learn-a-repo gate keys off this: under the
    # control plane a normal SIT run reports-only (needs_onboarding) and the graph's learn_repo
    # node owns onboarding; a standalone `/ocean-qa-agent` run (no Aquaman, flag unset) self-onboards.
    os.environ["OCEAN_PIPELINE_CONTROL_PLANE"] = "1"

    # Local mock-first SIT wiring (MM-13437 / EXE-c5ec3e4c fix): the test-automation SQS client keys
    # off SQS_ENDPOINT_URL to target LocalStack; unset -> it builds as None and crashes on `.meta`,
    # so the SQS-driven leg of a multi-repo callback E2E never assembles (could_not_verify). setdefault
    # so an explicit override still wins. The spawned CLI subprocess (and its pytest) inherits os.environ.
    os.environ.setdefault("SQS_ENDPOINT_URL", config.SQS_ENDPOINT_URL)
    os.environ.setdefault("SQS_LOCAL_ACCOUNT", config.SQS_LOCAL_ACCOUNT)

    options = ClaudeAgentOptions(
        model=config.STATION_MODEL,
        system_prompt=system_prompt,
        cwd=str(cwd),
        permission_mode=permission_mode,
        hooks=hooks,
    )
    ui.station_start(label)   # "▶ <station>" header; milestones stream underneath
    tools = 0
    in_tok = out_tok = 0
    async for message in query(prompt=prompt, options=options):
        # Default: curated, readable milestones (the significant actions).
        for m in _milestones(message):
            ui.milestone(m)
        tools += _count_tools(message)
        i, o = _usage(message)
        in_tok += i
        out_tok += o
        # developer level: also dump the raw per-message activity for debugging.
        if config.LOG_LEVEL == "developer":
            for line in _format_message(message):
                _emit(label, line)
    spend = metrics.fmt(in_tok, out_tok, tools)
    if spend:
        ui.milestone(f"done — {spend}")
    metrics.add(in_tok, out_tok, tools)


async def _drive_with_retry(*, system_prompt: str, prompt: str, cwd: Path,
                            permission_mode: str, label: str,
                            allowed_tools: list[str] | None = None) -> None:
    """Run the worker, retrying on any transient SDK/CLI failure with exponential backoff.

    A single claude-CLI ProcessError (e.g. the `Claude Code returned an error result: ...`
    crash in run.log) must not abort the whole run. Only after MAX_AGENT_RETRIES do we give
    up and let the caller raise StationError."""
    last: Exception | None = None
    for attempt in range(config.MAX_AGENT_RETRIES + 1):
        try:
            await _drive(system_prompt, prompt, cwd, permission_mode, label, allowed_tools)
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

    path = _agent_path(agent_md)
    guardrails = AGENT_GUARDRAILS.format(ticket_id=ticket_id)
    contract = VERDICT_INSTRUCTION.format(
        verdict_path=verdict_path,
        schema=json.dumps(verdict_model.model_json_schema(), indent=2),
    )
    try:
        await _drive_with_retry(
            system_prompt=_read(path),
            prompt=f"{guardrails}\n\n{task_prompt}\n{contract}",
            cwd=cwd or config.FK_AIDEVELOPER_DIR,
            permission_mode=permission_mode or config.STATION_PERMISSION_MODE,
            label=node,
            allowed_tools=_ensure_verdict_tool_allowed(_frontmatter_tools(path)),
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
            allowed_tools=_frontmatter_tools(skill_md),
        )
    except Exception as e:  # noqa: BLE001 — normalize any SDK/transport failure to StationError
        raise StationError(node, skill_name, f"skill run failed: {type(e).__name__}: {e}") from e
