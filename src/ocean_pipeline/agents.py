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
import shlex
import socket
import subprocess
from pathlib import Path
from typing import Type, TypeVar
from urllib.parse import urlparse

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


# A recursive search rooted at a filesystem TOP-LEVEL is the MM-14457/EXE-0417bc97 stall: a coder
# worker ran `grep -rln "def port_of_loading?" /` (search from `/`), which walks the whole machine
# (/System, /Users, mounts…), never returns, and hangs the station for 30+ min with no captured log.
# These are the roots a worker should NEVER recursively scan — it must scope to its worktree instead.
_DANGEROUS_SEARCH_ROOTS = {
    "/", "/System", "/Users", "/Library", "/private", "/usr", "/var", "/opt", "/etc",
    "/Applications", "/Volumes", "/bin", "/sbin", "/cores", "/net", "/home", "~", "$HOME",
}
_GREP_TOOLS = {"grep", "egrep", "fgrep", "rg", "ag", "ack"}   # first bare positional is the PATTERN, not a path
# Leading wrappers a worker might prefix a search with — stripped so `sudo/time/xargs grep -r x /` is still caught.
_WRAPPER_CMDS = {"sudo", "time", "nice", "ionice", "xargs", "env", "command", "nohup", "stdbuf", "timeout"}


def _is_dangerous_root(path: str) -> bool:
    """True if `path` names a filesystem TOP-LEVEL root to recursively scan — including the `/*`, `/*/`,
    `/System/*` top-level-glob forms (the shell expands `/*` to every top-level dir = a whole-disk scan).
    An absolute path INTO the run's worktree (`/tmp/ocean-pipeline/<EXE>/…`) is NOT dangerous."""
    if path in _DANGEROUS_SEARCH_ROOTS:
        return True
    if (path.rstrip("/") or "/") in _DANGEROUS_SEARCH_ROOTS:
        return True
    stripped = re.sub(r"/\*+/?$", "", path) or "/"      # drop a trailing glob segment: /* , /*/ , /System/*
    return stripped in _DANGEROUS_SEARCH_ROOTS


def unscoped_root_search(command: str) -> str | None:
    """If `command` recursively searches a filesystem TOP-LEVEL root (e.g. `grep -r … /`, `find /System …`,
    `rg pat /Users`, `grep -r x /*`, `sudo grep -r x /`), return the offending root; else None. Only
    top-level system roots are flagged — an absolute path INTO the run's worktree is fine. For grep-family
    tools the FIRST bare positional is the PATTERN (skipped), so `grep -rn "/etc" app/` (searching FOR a
    path literal, scoped to app/) is NOT a false positive. Split on shell separators so one bad segment of
    a pipe is caught (the real incident was `grep … / | grep … | head`)."""
    for seg in re.split(r"\|\|?|&&?|;|\n", command):
        try:
            toks = shlex.split(seg)
        except ValueError:
            continue
        # Strip leading `FOO=bar` env-assignments and wrapper commands (+ their own flags) so a prefixed
        # search still resolves to its real tool at position 0.
        i = 0
        while i < len(toks) and not toks[i].startswith("-") and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", toks[i]):
            i += 1
        while i < len(toks) and os.path.basename(toks[i]) in _WRAPPER_CMDS:
            i += 1
            while i < len(toks) and toks[i].startswith("-"):   # skip the wrapper's own flags (e.g. xargs -n1)
                i += 1
        if i >= len(toks):
            continue
        tool = os.path.basename(toks[i])
        rest = toks[i + 1:]
        is_grep = tool in _GREP_TOOLS
        is_find = tool == "find"
        if not is_grep and not is_find:
            continue
        # grep needs an explicit -r/-R/--recursive; rg/ag/ack recurse by default; find always recurses.
        recursive = is_find or tool in ("rg", "ag", "ack") or any(
            t == "--recursive" or (t.startswith("-") and not t.startswith("--") and ("r" in t[1:].lower()))
            for t in rest)
        if not recursive:
            continue
        # For grep-family, the first bare positional is the search PATTERN — skip it; every bare token after
        # is a path. For `find`, the leading positionals ARE the search paths, so skip nothing.
        pattern_skipped = not is_grep
        for t in rest:
            if t.startswith("-"):            # a flag (or a flag value we conservatively ignore)
                continue
            if not pattern_skipped:
                pattern_skipped = True       # this bare token is grep's pattern, not a path
                continue
            if _is_dangerous_root(t):
                return t
    return None


def _guard_bash():
    """A PreToolUse hook that DENIES a Bash command recursively searching a filesystem root, so the worker
    gets an immediate, recoverable error and re-scopes to its worktree — instead of hanging the station on
    a whole-disk scan (the EXE-0417bc97 stall). Orchestrator-enforced: unlike a worker-prompt rule (which
    the coder ignored), a hook runs regardless of permission_mode."""
    async def _hook(input_data, tool_use_id, context):  # noqa: ARG001 — SDK hook signature
        if input_data.get("tool_name") != "Bash":
            return {}
        cmd = str((input_data.get("tool_input") or {}).get("command", ""))
        root = unscoped_root_search(cmd)
        if root is None:
            return {}
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"Refusing a recursive search rooted at {root!r} — that scans the whole machine and "
                    f"hangs the station. Scope the search to the repo worktree instead (e.g. run it from "
                    f"the checkout with a relative path like `.` / `app/` / `lib/`, never an absolute "
                    f"system root)."
                ),
            }
        }
    return _hook


def _guard_repeated_read(threshold: int = 3):
    """A PreToolUse hook that DENIES the Nth consecutive Read of the SAME file, so a worker waiting on a
    background task can't burn its window re-reading an unchanged `<task>.output` (manual-findings #3:
    EXE-b1f09c5a spun re-reading `bh2kir9p4.output`, the harness rejecting each as a 'Wasted call').
    Orchestrator-enforced: a prose 'don't busy-poll' rule was not enough. State is per-run (closure) and
    resets the moment ANY other tool runs or a DIFFERENT file is read — so normal repeated reads of
    changing files are unaffected, and the intended recovery (run `sleep`/wait in Bash, which resets the
    counter, THEN read once) is allowed immediately."""
    state = {"key": None, "count": 0}

    async def _hook(input_data, tool_use_id, context):  # noqa: ARG001 — SDK hook signature
        if input_data.get("tool_name") != "Read":
            state["key"], state["count"] = None, 0   # any other action breaks a busy-wait
            return {}
        ti = input_data.get("tool_input") or {}
        # Key on file_path AND the page window: a large file read in pages (offset 0/2000/4000) is NOT a
        # busy-wait — each page returns new content. Only an IDENTICAL re-read (same path+offset+limit) is.
        path = str(ti.get("file_path", ""))
        key = (path, ti.get("offset"), ti.get("limit"))
        if key == state["key"]:
            state["count"] += 1
        else:
            state["key"], state["count"] = key, 1
        if state["count"] < threshold:
            return {}
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"You have Read {path!r} {state['count']} times in a row with no other action — this is a "
                    f"busy-wait and makes no progress (the file is unchanged). Do NOT re-Read it. If you are "
                    f"waiting on a background task, WAIT first — run a bounded `sleep` in Bash (e.g. "
                    f"`sleep 30`) or block on the task — and only THEN Read once. Running any other tool "
                    f"clears this guard."
                ),
            }
        }
    return _hook


def _emit(label: str, line: str) -> None:
    print(f"    [{label}] {line}", flush=True)


def _station_logfile(label: str):
    """Open (append) a per-station stream log at artifacts_dir(<exec_id>)/<label>.log, or None if the
    exec-id isn't set (unit tests) or the path can't be opened. Best-effort: a logging hiccup must never
    break a run. `_execute` stamps OCEAN_PIPELINE_EXEC_ID; a station that re-runs (coder rework, env
    retry) appends after a separator so each pass is preserved."""
    exec_id = os.environ.get("OCEAN_PIPELINE_EXEC_ID", "")
    if not exec_id:
        return None
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", label) or "station"
    try:
        f = (config.artifacts_dir(exec_id) / f"{safe}.log").open("a", encoding="utf-8")
        f.write(f"\n===== {label} @ pass start =====\n")
        f.flush()
        return f
    except OSError:
        return None


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


# ---- MCP isolation (BrokenPipeError-at-station-transition fix) -----------------------------------
# Each station spawns a `claude` CLI subprocess that inherits the ambient MCP config from ~/.claude.json.
# If that config lists an UNREACHABLE server (e.g. fk-code-graph at neo4j-mcp-server.fourkites.internal,
# which fails DNS with NXDOMAIN), the CLI hangs/crashes trying to connect and the SDK's transport pipe to
# the subprocess breaks -> `BrokenPipeError: [Errno 32] Broken pipe`. We probe reachability ONCE per
# process and hand the CLI an explicit reachable-only mcp_servers set with strict_mcp_config=True so it
# never touches the dead server. Everything here is fallback-safe: any doubt -> return None -> ambient.
# 5s (not 2s) so a reachable-but-slow server isn't mistaken for dead and dropped for the WHOLE process
# (the probe result is cached once per run) — a false-drop makes graph-using SME stations run blind
# (judge MINOR-7). A genuinely dead host still fails fast on NXDOMAIN/refused; this only widens the
# grace for a slow TCP accept.
_MCP_PROBE_TIMEOUT_S = 5.0
_STATION_MCP_UNSET = object()
_station_mcp_cache = _STATION_MCP_UNSET   # module global: the probe runs ONCE per process, not per station


def _load_ambient_mcp_servers() -> dict | None:
    """The `mcpServers` map from ~/.claude.json. Its entries are already in the exact raw shape the SDK's
    `mcp_servers` dict accepts (http/sse -> {"type","url","headers?"}; stdio -> {"command","args","env"}),
    so we pass the reachable subset straight through. Returns None if the file is absent/unreadable/malformed
    or carries no mcpServers — the caller treats None as 'preserve ambient behavior'."""
    try:
        raw = json.loads((Path.home() / ".claude.json").read_text())
    except Exception:  # noqa: BLE001 — absent / unreadable / bad JSON -> ambient fallback
        return None
    servers = raw.get("mcpServers")
    return servers if isinstance(servers, dict) and servers else None


def _mcp_entry_reachable(entry, timeout: float = _MCP_PROBE_TIMEOUT_S) -> bool:
    """Is one ~/.claude.json MCP-server entry reachable right now?
      * url-based (http/sse): its host must DNS-resolve (socket.getaddrinfo) AND a short TCP connect to its
        port must succeed. A non-resolving host (NXDOMAIN) or a refused/timed-out port is UNREACHABLE — that
        is exactly what wedges the spawned CLI and breaks the SDK pipe.
      * stdio/command (no url): a LOCAL subprocess, never the DNS-broken-pipe culprit -> keep it (True), so
        working local servers (jira, clickhouse, ...) survive the isolation."""
    if not isinstance(entry, dict):
        return False
    url = entry.get("url")
    if not url:
        return True   # stdio/local — not a network MCP; keep today's behavior for it
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if not host:
            return False
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)   # NXDOMAIN -> gaierror (OSError)
    except OSError:
        return False
    for family, socktype, proto, _canon, sockaddr in infos:
        sock = None
        try:
            sock = socket.socket(family, socktype, proto)
            sock.settimeout(timeout)
            sock.connect(sockaddr)
            return True
        except OSError:
            continue
        finally:
            if sock is not None:
                sock.close()
    return False


def _filter_reachable_mcp_servers(ambient: dict) -> dict:
    """Subset of `ambient` whose servers are reachable right now (see _mcp_entry_reachable)."""
    return {name: cfg for name, cfg in ambient.items() if _mcp_entry_reachable(cfg)}


def _station_mcp_config() -> dict | None:
    """Explicit `mcp_servers` dict of ONLY the ambient servers reachable right now, for the spawned CLI to
    use with strict_mcp_config=True — so it never connects to a dead server (the BrokenPipeError source).
    Returns None to signal 'set NEITHER mcp_servers NOR strict_mcp_config — keep ambient behavior', the
    SAFE fallback whenever we can't confidently improve on ambient: flag off, ~/.claude.json unreadable,
    a probe error, nothing dead to drop, or an all-unreachable result (more likely a total false-negative
    than reality). Result is cached in a module global so the reachability probe runs ONCE per process."""
    global _station_mcp_cache
    if _station_mcp_cache is not _STATION_MCP_UNSET:
        return _station_mcp_cache
    result: dict | None = None
    try:
        if config.ISOLATE_UNREACHABLE_MCP:
            ambient = _load_ambient_mcp_servers()
            if ambient:
                reachable = _filter_reachable_mcp_servers(ambient)
                # Override ONLY when the probe confirmed >=1 reachable AND it actually drops something.
                # Nothing dropped -> strict config == ambient, so keep ambient (no added risk). Empty
                # reachable -> treat as a false-negative and keep ambient (never strip ALL servers).
                if reachable and len(reachable) < len(ambient):
                    dropped = sorted(set(ambient) - set(reachable))
                    ui.milestone(f"MCP isolation: using {len(reachable)}/{len(ambient)} reachable "
                                 f"server(s); skipping unreachable {dropped}")
                    result = reachable
    except Exception:  # noqa: BLE001 — any probe/parse error -> ambient fallback (never make it worse)
        result = None
    _station_mcp_cache = result
    return result


async def _drive(system_prompt: str, prompt: str, cwd: Path, permission_mode: str,
                 label: str = "station", allowed_tools: list[str] | None = None) -> None:
    # Imported lazily so the graph/routing test suite runs without the SDK (or the
    # `claude` CLI it spawns) installed — the SDK is only needed at actual run time.
    from claude_agent_sdk import ClaudeAgentOptions, HookMatcher, query

    # PreToolUse hooks (run regardless of permission_mode — see _deny_outside_allowlist). Always guard
    # against a whole-disk recursive search (the EXE-0417bc97 hang); additionally enforce the worker's
    # declared tool allowlist when it declared one.
    _pre = [_guard_bash(), _guard_repeated_read()]
    if allowed_tools is not None:
        _pre.append(_deny_outside_allowlist(allowed_tools))
    hooks = {"PreToolUse": [HookMatcher(hooks=_pre)]}

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

    # Bound the worker's Bash tool (inherited by the spawned claude CLI). WHY: an SDK-spawned worker's
    # Bash has NO effective timeout, so a runaway command (EXE-0417bc97's whole-disk `grep /`) hung the
    # coder 30+ min. /fk-execute never hit this — it runs under the standard interactive Bash tool, which
    # already enforces a timeout. These env vars give the same ceiling: a command without an explicit
    # timeout is capped at DEFAULT; the model may request up to MAX for a known-slow step. Generous
    # enough for ocean long-poles (Rails-boot rspec ~73s, full suite) but far below a 30-min runaway.
    os.environ.setdefault("BASH_DEFAULT_TIMEOUT_MS", config.BASH_DEFAULT_TIMEOUT_MS)
    os.environ.setdefault("BASH_MAX_TIMEOUT_MS", config.BASH_MAX_TIMEOUT_MS)

    options_kwargs = dict(
        model=config.STATION_MODEL,
        system_prompt=system_prompt,
        cwd=str(cwd),
        permission_mode=permission_mode,
        hooks=hooks,
    )
    # Isolate the spawned CLI from unreachable ambient MCP servers (fk-code-graph NXDOMAIN etc.) that break
    # the SDK transport pipe with BrokenPipeError at station transitions. Only when the helper hands back a
    # reachable-only set do we pin the CLI to it (strict_mcp_config=True makes it IGNORE the ambient config);
    # otherwise we set neither key and preserve today's ambient behavior. Covers BOTH run_agent and run_skill
    # stations — both route through here (the sole ClaudeAgentOptions construction). See config.ISOLATE_UNREACHABLE_MCP.
    _mcp = _station_mcp_config()
    if _mcp is not None:
        options_kwargs["mcp_servers"] = _mcp
        options_kwargs["strict_mcp_config"] = True
    options = ClaudeAgentOptions(**options_kwargs)
    ui.station_start(label)   # "▶ <station>" header; milestones stream underneath
    tools = 0
    in_tok = out_tok = 0
    logf = _station_logfile(label)   # per-station stream capture (EXE-0417bc97 stall was invisible w/o this)
    try:
        async for message in query(prompt=prompt, options=options):
            # Default: curated, readable milestones (the significant actions).
            for m in _milestones(message):
                ui.milestone(m)
            tools += _count_tools(message)
            i, o = _usage(message)
            in_tok += i
            out_tok += o
            # ALWAYS capture the raw per-message activity to the station log (so a hang/stall is
            # diagnosable in ~1s); ALSO echo it to the console only at developer LOG_LEVEL.
            lines = _format_message(message)
            if logf is not None:
                for line in lines:
                    logf.write(line + "\n")
                logf.flush()
            if config.LOG_LEVEL == "developer":
                for line in lines:
                    _emit(label, line)
    finally:
        if logf is not None:
            logf.close()
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


def _capture_partial(node: str, execution_id: str, cwd: Path | None, fast: bool = False) -> None:
    """Best-effort breadcrumb written when a station dies WITHOUT a verdict — a kill/timeout mid-run
    (manual-findings #16). Records the workspace git state so the coder's committed-but-interrupted work
    isn't an opaque loss: a resume reads `<node>.partial.json` and the coder is told to CONTINUE that
    branch instead of starting over. Never raises. `fast=True` (the KILL path) does ONE quick git probe
    (branch+HEAD) so a killed process still exits fast (~seconds); the SDK-error path does the full probe."""
    if cwd is None:
        return
    repo_dir = cwd
    try:
        if cwd.exists():
            for child in cwd.iterdir():   # the coder clones the repo into a subdir of the workspace
                if (child / ".git").exists():
                    repo_dir = child
                    break
    except Exception:  # noqa: BLE001
        pass

    def _git(*args: str, timeout: float = 5.0) -> str:
        try:
            r = subprocess.run(["git", "-C", str(repo_dir), *args],
                               capture_output=True, text=True, timeout=timeout)
            return r.stdout.strip() if r.returncode == 0 else ""
        except Exception:  # noqa: BLE001
            return ""

    try:
        note = ("station was killed/interrupted before writing a verdict; any committed work is on the "
                "branch above — a resume should CONTINUE it, not start over")
        if fast:
            # Two quick probes, not the full status/commits-ahead set below — tight timeout each, since
            # the kill path must not spend ~20s before the CancelledError re-propagates. (A single combined
            # `rev-parse --abbrev-ref HEAD HEAD` does NOT give branch+SHA: --abbrev-ref applies to every
            # following ref arg, so it prints the branch name TWICE — confirmed by direct reproduction.)
            info = {"node": node, "repo_dir": str(repo_dir),
                    "branch": _git("rev-parse", "--abbrev-ref", "HEAD", timeout=2.0),
                    "head": _git("rev-parse", "HEAD", timeout=2.0),
                    "note": note}
        else:
            info = {
                "node": node, "repo_dir": str(repo_dir),
                "branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
                "head": _git("rev-parse", "HEAD"),
                "commits_ahead": _git("rev-list", "--count", "@{upstream}..HEAD") or _git("rev-list", "--count", "HEAD"),
                "dirty": bool(_git("status", "--porcelain")),
                "note": note,
            }
        (config.artifacts_dir(execution_id) / f"{node}.partial.json").write_text(json.dumps(info, indent=2))
    except Exception:  # noqa: BLE001
        pass


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
        if not verdict_path.exists():
            _capture_partial(node, execution_id, cwd)   # SDK crash / timeout with no verdict (full probe)
        raise StationError(node, agent_md, f"agent run failed: {type(e).__name__}: {e}") from e
    except BaseException:   # noqa: BLE001 — a KILL / Ctrl-C / cancel: fast breadcrumb, then let it propagate
        if not verdict_path.exists():
            _capture_partial(node, execution_id, cwd, fast=True)   # tight budget — exit fast on a kill
        raise

    if not verdict_path.exists():
        # Backstop: the agent ended its turn WITHOUT writing the verdict. The common cause is that it
        # offloaded slow work (an image build / test suite) to a BACKGROUND task and then stopped to
        # "wait for a completion notification" — but a station is a SINGLE turn, so stopping tears it
        # down and kills that background task (EXE-928fd700). Re-drive ONCE with a corrective
        # instruction to finish synchronously, before failing the station.
        _emit(node, "no verdict on first turn — re-driving once (finish synchronously, no background-and-yield)")
        try:
            await _drive_with_retry(
                system_prompt=_read(path),
                prompt=(
                    f"{guardrails}\n\nYou ENDED YOUR TURN without writing the required verdict to "
                    f"{verdict_path}. Do NOT run long work (image builds, test suites) as a BACKGROUND "
                    f"task and then stop to wait for a notification — this station is a SINGLE turn, so "
                    f"any background task is killed the instant you stop. Run all such work "
                    f"SYNCHRONOUSLY in the FOREGROUND (one blocking Bash call with an explicit long "
                    f"timeout), then WRITE THE VERDICT before you finish.\n\n{task_prompt}\n{contract}"
                ),
                cwd=cwd or config.FK_AIDEVELOPER_DIR,
                permission_mode=permission_mode or config.STATION_PERMISSION_MODE,
                label=f"{node}:verdict-redrive",
                allowed_tools=_ensure_verdict_tool_allowed(_frontmatter_tools(path)),
            )
        except Exception as e:  # noqa: BLE001 — normalize any SDK/transport failure to StationError
            if not verdict_path.exists():
                _capture_partial(node, execution_id, cwd)
            raise StationError(node, agent_md, f"agent run failed (verdict re-drive): {type(e).__name__}: {e}") from e
        except BaseException:   # noqa: BLE001 — KILL / cancel during the re-drive: fast breadcrumb, then propagate
            if not verdict_path.exists():
                _capture_partial(node, execution_id, cwd, fast=True)
            raise
        if not verdict_path.exists():
            _capture_partial(node, execution_id, cwd)   # breadcrumb the pushed git state for the resume-hint
            raise StationError(node, agent_md, f"no verdict written to {verdict_path} (after re-drive)")
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
