"""Graph nodes.

Two kinds of node:
  * worker nodes — telemetry -> run ONE narrow agent/skill via the Claude Agent SDK ->
    return a partial state update. The graph (graph.py) owns all sequencing/routing/loops.
  * plain-code nodes (open_pr, flip_ready) — deterministic git/gh operations run directly
    here via gitops.py, NOT delegated to an agent, so the process is exact and testable.
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import signal
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from . import agents, config, gitops, jira, lessons, quality, schemas, telemetry, ui
from .state import OceanState


def _docker_resources() -> tuple[float, int] | None:
    """(mem_gb, cpus) from `docker info`, or None if Docker isn't reachable OR its output doesn't
    expose the fields we need. Returning None (rather than (0, 0)) for BOTH cases matters: an
    alternate Docker backend (colima/Podman/Rancher Desktop) that omits MemTotal/NCPU from
    `docker info --format '{{json .}}'` is genuinely running, just not introspectable this way —
    conflating that with "Docker isn't running" would misreport a real environment as down.
    Plain deterministic check — no agent call — so sit_run can fail fast (~1s) instead of
    spending an entire expensive agent invocation attempting a bring-up that's going to OOM
    (see config.MIN_DOCKER_MEMORY_GB)."""
    if not shutil.which("docker"):
        return None
    try:
        out = subprocess.run(["docker", "info", "--format", "{{json .}}"],
                             capture_output=True, text=True, timeout=10)
        if out.returncode != 0 or not out.stdout.strip():
            return None
        info = json.loads(out.stdout)
        if "MemTotal" not in info or "NCPU" not in info:
            return None
        return info["MemTotal"] / (1024 ** 3), info["NCPU"]
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError):
        return None


# ---- SIT concurrency slots (manual-findings #18): a cross-process counting semaphore over N flock files.
# Held for the lifetime of a run's SIT stack (sit_run acquires → teardown_container releases). The fd lives
# in this module-global registry keyed by execution_id (one graph == one process); the FILE lock is what
# coordinates across separate terminals/processes. A crashed holder's flock auto-releases (kernel), so a
# slot can never leak permanently.
_SIT_SLOT_FDS: dict = {}


def _sit_slot_dir() -> Path:
    # config.ARTIFACTS_ROOT, not a second `os.environ.get(..., "/tmp/ocean-pipeline")`. These three
    # slot dirs each re-implemented that lookup, so moving the artifacts root off /tmp would have
    # split the cross-process LOCKS away from the run evidence they coordinate — and left the locks
    # in the directory a daily cleaner empties. One definition, in config.
    d = config.ARTIFACTS_ROOT / "sit-slots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _try_acquire_sit_slot(execution_id: str) -> bool:
    """ONE non-blocking pass: grab a free slot if any is free, else return False. Fast + synchronous (the
    flock attempts are LOCK_NB), so it's safe to call from the async waiter without an executor. Idempotent
    per exec_id (a retry re-entering sit_run keeps its existing slot)."""
    if execution_id in _SIT_SLOT_FDS:
        return True
    n = max(1, config.MAX_CONCURRENT_SIT)
    for i in range(n):
        fd = open(_sit_slot_dir() / f"slot-{i}.lock", "w")
        try:
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fd.close()
            continue
        _SIT_SLOT_FDS[execution_id] = fd
        return True
    return False


async def _acquire_sit_slot(execution_id: str) -> str:
    """Wait (INTERRUPTIBLY) for a free SIT slot, then hold it. Polls `_try_acquire_sit_slot` between
    `await asyncio.sleep`s — so a cancel/Ctrl-C is observed immediately (unlike a blocking thread, which
    the interpreter would have to join at exit — review #1). Gives up after SIT_SLOT_WAIT_SECONDS and
    proceeds anyway (the preflight over-commit check is the backstop) so a wedged holder can't deadlock."""
    if _try_acquire_sit_slot(execution_id):
        return "held" if execution_id in _SIT_SLOT_FDS else "acquired"
    deadline = time.monotonic() + max(0, config.SIT_SLOT_WAIT_SECONDS)
    while True:
        await asyncio.sleep(5.0)
        if _try_acquire_sit_slot(execution_id):
            return "acquired after waiting"
        if time.monotonic() >= deadline:
            return f"no free slot after {config.SIT_SLOT_WAIT_SECONDS}s — proceeding (preflight is the backstop)"


def _release_sit_slot(execution_id: str) -> None:
    fd = _SIT_SLOT_FDS.pop(execution_id, None)
    if fd is None:
        return
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    finally:
        fd.close()


# ---- GAN concurrency slot (S2, run-monitoring-findings.md): a THIRD, separate cross-process
# semaphore from the SIT/build slots above, gating qa_scenarios' GAN loop specifically — that loop
# fans out up to 4 sub-agents/round for up to 3 rounds INSIDE one run_skill call, entirely inside the
# skill's own agentic session (invisible to this control plane's Python), so it can't be metered the
# way Docker memory is. Bounding it to config.MAX_CONCURRENT_GAN concurrent NODES machine-wide is a
# coarse but effective proxy: at most that many tickets' GAN fan-outs run at once, instead of every
# batch ticket's GAN racing every other's for the same local CPU/LLM concurrency and the shared
# TestRail API. Same shape as the SIT slot (simpler than the build slot — no Docker headroom check
# needed here, this isn't about container memory).
_GAN_SLOT_FDS: dict = {}


def _gan_slot_dir() -> Path:
    # See _sit_slot_dir: one definition of the artifacts root, in config.
    d = config.ARTIFACTS_ROOT / "gan-slots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _try_acquire_gan_slot(execution_id: str) -> bool:
    """ONE non-blocking pass: grab a free GAN slot if any is free, else return False. Idempotent per
    exec_id (a retry re-entering qa_scenarios keeps its existing slot)."""
    if execution_id in _GAN_SLOT_FDS:
        return True
    n = max(1, config.MAX_CONCURRENT_GAN)
    for i in range(n):
        fd = open(_gan_slot_dir() / f"slot-{i}.lock", "w")
        try:
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fd.close()
            continue
        _GAN_SLOT_FDS[execution_id] = fd
        return True
    return False


async def _acquire_gan_slot(execution_id: str) -> str:
    """Wait (INTERRUPTIBLY) for a free GAN slot, then hold it. Gives up after GAN_SLOT_WAIT_SECONDS
    and proceeds anyway (a queue, not a crash — same philosophy as the SIT/build slots) so a wedged
    holder can't deadlock a whole batch."""
    if _try_acquire_gan_slot(execution_id):
        return "held" if execution_id in _GAN_SLOT_FDS else "acquired"
    deadline = time.monotonic() + max(0, config.GAN_SLOT_WAIT_SECONDS)
    while True:
        await asyncio.sleep(5.0)
        if _try_acquire_gan_slot(execution_id):
            return "acquired after waiting"
        if time.monotonic() >= deadline:
            return f"no free GAN slot after {config.GAN_SLOT_WAIT_SECONDS}s — proceeding anyway"


def _release_gan_slot(execution_id: str) -> None:
    fd = _GAN_SLOT_FDS.pop(execution_id, None)
    if fd is None:
        return
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    finally:
        fd.close()


def _any_docker_repo(target_repos) -> bool:
    """True if ANY repo in target_repos is Ruby/Docker-run (per AGENT_GUARDRAILS in agents.py — Go/Java
    run native, 0 Docker-VM footprint). Same predicate prep_image/prep_container already use inline
    (kept inline there too, not refactored, to keep this change additive-only) — a ticket touching only
    Go/Java repos must never wait on the build-slot pool below at all."""
    return any(
        "docker" in (r.get("build_env") or "").lower() or (r.get("language") or "").lower() == "ruby"
        for r in (target_repos or []) if isinstance(r, dict)
    )


# ---- Build-slot concurrency pool: a SEPARATE cross-process counting semaphore (config.MAX_CONCURRENT_BUILDS)
# from the SIT slot above, gating prep_image / prep_container / coder / harsh_reviewer / reachability_gate —
# the Docker-heavy stages that had ZERO capacity protection before this (only sit_run did, via the SIT slot).
# Unlike the SIT slot, a free flock slot here is NOT sufficient on its own: _try_acquire_build_slot ALSO
# requires live Docker headroom (via _docker_resources/_docker_used_gb) for
# config.docker_budget_for_build(target_repos) before reporting success — a fixed slot COUNT doesn't bound
# real memory (one ticket's Ruby bundle install could be 1.5 GB, another's much heavier), so the live read
# is the actual safety backstop; the count only bounds how many things can be TRYING at once. This is what
# lets the two pools compose safely: both ultimately defer to the same `docker stats` ground truth, so a
# live SIT stack from another process is visible to a build-slot acquisition attempt, and vice versa.
_BUILD_SLOT_FDS: dict = {}


def _build_slot_dir() -> Path:
    # See _sit_slot_dir: one definition of the artifacts root, in config.
    d = config.ARTIFACTS_ROOT / "build-slots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _try_acquire_build_slot(execution_id: str, target_repos=None) -> bool:
    """ONE non-blocking pass: grab a free slot AND confirm live Docker headroom, else return False (and
    release any slot grabbed along the way — never hold a slot while denying the acquisition). Idempotent
    per exec_id (a retry keeps its existing hold). If Docker usage can't be measured
    (_docker_resources/_docker_used_gb return None), proceed permissively — this is best-effort capacity
    protection layered onto whatever already-fallback-safe stage calls it, not a hard preflight gate (that
    contract belongs to _docker_preflight_reason, used only by sit_run).
    SYNCHRONOUS on purpose (matches _try_acquire_sit_slot's contract, and what tests/test_build_slots.py
    calls directly) — but unlike the SIT slot this also runs `docker info`/`docker stats`
    (subprocess.run, ~10-20s worst case), so every async caller MUST invoke this via
    `await asyncio.to_thread(_try_acquire_build_slot, ...)`, never call it directly from an async
    function — a direct call would block the entire event loop for that long on every attempt."""
    if execution_id in _BUILD_SLOT_FDS:
        return True
    n = max(1, config.MAX_CONCURRENT_BUILDS)
    fd = None
    for i in range(n):
        candidate = open(_build_slot_dir() / f"slot-{i}.lock", "w")
        try:
            fcntl.flock(candidate.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            candidate.close()
            continue
        fd = candidate
        break
    if fd is None:
        return False
    resources = _docker_resources()
    if resources is not None:
        mem_gb, _cpus = resources
        used_gb = _docker_used_gb(exclude_name_substr=execution_id)
        needed_gb = config.docker_budget_for_build(target_repos)
        if used_gb is not None and (mem_gb - used_gb) < needed_gb:
            fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
            fd.close()
            return False
    _BUILD_SLOT_FDS[execution_id] = fd
    return True


async def _acquire_build_slot(execution_id: str, target_repos=None) -> str:
    """Wait (INTERRUPTIBLY) for a free build slot + headroom, then hold it. Same shape as
    _acquire_sit_slot: polls _try_acquire_build_slot between `await asyncio.sleep(5.0)`s (a cancel is
    observed immediately), gives up after config.BUILD_SLOT_WAIT_SECONDS and proceeds anyway — a queue,
    not a crash, same philosophy as the SIT slot. Off-thread (see _try_acquire_build_slot's docstring):
    this polls every 5s for up to BUILD_SLOT_WAIT_SECONDS, so keeping the docker subprocess calls off
    the event loop here matters even more than on the first attempt."""
    if await asyncio.to_thread(_try_acquire_build_slot, execution_id, target_repos):
        return "held" if execution_id in _BUILD_SLOT_FDS else "acquired"
    deadline = time.monotonic() + max(0, config.BUILD_SLOT_WAIT_SECONDS)
    while True:
        await asyncio.sleep(5.0)
        if await asyncio.to_thread(_try_acquire_build_slot, execution_id, target_repos):
            return "acquired after waiting"
        if time.monotonic() >= deadline:
            return f"no free build slot/headroom after {config.BUILD_SLOT_WAIT_SECONDS}s — proceeding"


def _release_build_slot(execution_id: str) -> None:
    fd = _BUILD_SLOT_FDS.pop(execution_id, None)
    if fd is None:
        return
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    finally:
        fd.close()


def _docker_used_gb(exclude_name_substr: str = "") -> float | None:
    """Memory (GB) CURRENTLY consumed by running containers, via `docker stats --no-stream`. None if it
    can't be measured. Lets preflight check FREE headroom, not just total capacity — so a run doesn't
    start its SIT stack when CONCURRENT pipeline runs already saturate Docker (manual-findings #18:
    EXE-caa5c082 died at Step-0 because two other tickets' full stacks were up). `exclude_name_substr`
    drops containers whose NAME contains it — pass THIS run's execution_id so its own warm SIT stack /
    persistent container (both named with the exec_id) don't count as competing usage on a retry (review
    #4). Best-effort: any failure returns None and the caller falls back to the total-capacity check
    (never a false environment_failure)."""
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
                             capture_output=True, text=True, timeout=20)
        if out.returncode != 0:
            return None
    except (subprocess.SubprocessError, OSError):
        return None
    # GB multipliers relative to a GiB (docker reports GiB/MiB/KiB or bare B): KiB, MiB, GiB, TiB, B.
    mult = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1.0, "T": 1024.0, "": 1 / 1024 ** 3}
    total = 0.0
    for line in out.stdout.splitlines():
        name, _, usage = line.partition("\t")
        if exclude_name_substr and exclude_name_substr in name:
            continue
        used = usage.split("/")[0].strip()   # "1.5GiB / 9.7GiB" -> "1.5GiB"
        m = re.match(r"([\d.]+)\s*([KMGT]?)i?B", used, re.I)
        if m:
            total += float(m.group(1)) * mult.get(m.group(2).upper(), 0)
    return total


def _docker_preflight_reason(target_repos=None, exclude_name_substr: str = "") -> str:
    """Empty string if Docker has enough resources for THIS ticket's changed/target-repo set; otherwise
    a ready-to-use environment_failure reason string. MM-14628: the budget scales to the repo set (1..N
    changed repos all run real) via config.docker_budget_for_repos — mirroring the skill's
    docker_preflight.py --repos — instead of a fixed floor, so an under-provisioned MULTI-repo chain
    (e.g. 3 Ruby repos needing ~10 GB) fails fast here at the node too, not only deeper in the skill.
    Native Go/Java repos add 0 (they run off the Docker VM). This is the deterministic
    resource-insufficient case specifically -- NEVER auto-retried (see graph.py::after_sit_triage's
    preflight_failed check), because more Docker memory doesn't appear between attempts. Still classified
    as environment_failure (not could_not_verify) because it IS a harness/infra limit, not a structural
    test limitation -- it's just a non-retriable one."""
    resources = _docker_resources()
    if resources is None:
        return ("environment_failure: Docker is not running, not reachable, or `docker info` didn't "
                 "expose memory/CPU (an alternate backend like colima/Podman may need a different "
                 "check) — could not determine available resources.")
    mem_gb, cpus = resources
    min_gb, min_cpus = config.docker_budget_for_repos(target_repos)
    if mem_gb < min_gb or cpus < min_cpus:
        return (f"environment_failure: insufficient_docker_resources — have {mem_gb:.1f} GB / {cpus} CPU, "
                f"need >= {min_gb} GB / {min_cpus} CPU for this repo set (see "
                f"local_service_execution.md 'Docker memory ceiling'). Raise Docker Desktop/Rancher "
                f"Desktop memory+CPU allocation before retrying.")
    # Total capacity is enough — but is it FREE? Concurrent pipeline runs (separate terminals) share this
    # one Docker VM; if their live containers already consume most of it, this run's SIT stack won't fit.
    # This is the case EXE-caa5c082 hit (manual-findings #18). Unlike insufficient total capacity, this is
    # transient — retry once the other runs finish or their stale stacks are torn down.
    used_gb = _docker_used_gb(exclude_name_substr)
    if used_gb is not None and (mem_gb - used_gb) < min_gb:
        return (f"environment_failure: docker_over_committed — {used_gb:.1f} GB of {mem_gb:.1f} GB is already "
                f"in use by other running containers, leaving {mem_gb - used_gb:.1f} GB free; this repo set "
                f"needs >= {min_gb} GB. A concurrent pipeline run is likely holding a full SIT stack — wait "
                f"for it to finish or tear down stale stacks (`docker ps`), then retry (manual-findings #18).")
    return ""


def _summary(state: OceanState) -> str:
    """<=300-token ticket summary pushed to each worker.

    Also RE-ESTABLISHES the lesson-capture run context (Finding 3). This is a side effect in a helper
    that otherwise just formats a string, and it is deliberate: `agents._capture_tool_failure` only
    installs its `PostToolUseFailure` hook when `_RUN_CONTEXT["domain_bucket"]` is set, and that
    module-global lives in ONE OS process. Setting it in `researcher` alone left capture dead for the
    entire post-interrupt half of every run — all three review gates default to pausing, and a resume
    is a fresh process driven by `Command(resume=...)` that re-enters at the interrupted node and
    never re-runs `researcher`. So `coder`, `harsh_reviewer`, `sit_run`, `sit_triage` and
    `flip_ready` — the heaviest tool users — recorded nothing at all (found in review).

    Every station builds its prompt through this helper, which makes it the one seam that is
    guaranteed to run in whichever process a station actually executes in. Cheap and idempotent."""
    try:
        agents.set_run_context(
            domain_bucket=str(state.get("domain_bucket") or ""),
            ticket_id=str(state.get("ticket_id") or ""),
            execution_id=str(state.get("execution_id") or ""))
    except Exception:  # noqa: BLE001 — context bookkeeping must never break prompt assembly
        pass
    s = (
        f"Ticket: {state['ticket_id']}\n"
        f"Context: {state.get('context', '(none)')}\n"
        f"Target repos: {json.dumps(state.get('target_repos', []))}\n"
    )
    if state.get("rca_findings"):
        # RCA-originated fix: the gates + coder work from the RCA brief, not a coding-route packet.
        s += f"Origin: RCA fix. RCA brief: {json.dumps(state['rca_findings'])}\n"
    # Finding 3 (architecture review, "Give Pipeline Memory"): recurring failure patterns this SAME
    # domain has hit before (lessons.recall_lessons via researcher) — top 3 by recurrence, one line
    # each, so every station sees "this has happened before" without blowing the token budget.
    recurring = state.get("recurring_lessons") or []
    if recurring:
        def _tickets_str(r: dict) -> str:
            # Judge-review finding: a hand-edited/corrupted lessons.json could carry a non-list
            # `tickets` value (e.g. null survives a `.get(..., [])` fallback since the KEY is present,
            # just not list-shaped) — guard here rather than let a malformed record on disk crash
            # EVERY downstream node for every future ticket in the domain via this shared helper.
            tickets = r.get("tickets")
            if not isinstance(tickets, list):
                return "?"
            return ", ".join(str(t) for t in tickets[-3:])
        def _one(r: dict) -> str:
            # The normalized fail_sig is a stable KEY, deliberately stripped of paths/numbers/quoted
            # literals — which makes it good for matching and nearly useless to read. Judge review:
            # "the only field with actionable detail, `note`, is written and never read", so the
            # station saw a de-specified signature with nothing to act on. Carry a trimmed raw error
            # alongside the key so the lesson names the ACTUAL failure, not just its fingerprint.
            note = str(r.get("note") or "").strip().replace("\n", " ")
            detail = f' — last seen as: "{note[:160]}"' if note else ""
            return (f"{r.get('fail_sig', '?')} at {r.get('action_sig', '?')} "
                    f"(x{r.get('recurrence_count', '?')}, tickets: {_tickets_str(r)}){detail}")
        lines = "; ".join(_one(r) for r in recurring[:3] if isinstance(r, dict))
        s += (f"Known recurring failure patterns in this domain (seen on 2+ DIFFERENT tickets — "
              f"check for these before repeating them): {lines}\n")
    return s


def _load_json(path: str) -> dict:
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {}


def _brief(obj, limit: int = 6000) -> str:
    """Serialize a state slice to feed a worker INLINE (explicit state, not a file handoff).
    Bounded so a large packet can't blow the prompt."""
    if not obj:
        return "(none)"
    s = json.dumps(obj, indent=2, default=str)
    return s if len(s) <= limit else s[:limit] + "\n… (truncated)"


def _reachability_for_coder(report: dict) -> str:
    """The coder MUST act on the reachability verdicts + overrides IN FULL — never truncate these
    (R2: a real run truncated the whole report at 12k in the coder prompt, so the binding
    `overrides_for_coder` were cut off and the coder had to Read the file). Serialize just the binding
    slice (verdicts + overrides + advisories) untruncated — it is small (~a dozen entries); the full
    report with audits/provenance stays on disk if the coder wants more."""
    if not report:
        return "(none)"
    slim = {
        "blocking": report.get("blocking"),
        "verified_claims": [
            {"verdict": c.get("verdict"), "claim": c.get("source_claim") or c.get("claim_summary")}
            for c in (report.get("verified_claims") or []) if isinstance(c, dict)
        ],
        "overrides_for_coder": report.get("overrides_for_coder") or [],
        "advisory_findings": report.get("advisory_findings") or [],
        # MM-14816 (G20): the reachability-VERIFIED [ENGINEER TO FILL]/blank values the coder must USE
        # (each {placeholder, resolved_value, source, evidence}) — binding, so untruncated here. Unverified
        # ones were downgraded by reachability and are NOT in this list (coder is C10/C11-bound not to
        # invent a value that isn't here).
        "resolved_placeholders": report.get("resolved_placeholders") or [],
    }
    return json.dumps(slim, indent=2, default=str)


def _gh_token() -> str:
    """GitHub token for private-gem Docker builds. Ocean Ruby images (ocean-worker et al.) need it on a
    COLD build — VALIDATED: without it `bundle install` fails exit 11, so the pre-warm/container build
    silently fails and falls back to a no-op. Prefer `gh auth token`, fall back to env."""
    try:
        out = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=10)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""


def _image_cache_argv(tool: Path, name: str, worktree: Path, has_token: bool) -> list[str]:
    """Argv to build/resolve a repo's cached image. When has_token, adds --inject-github-token so
    ruby_image_cache reads the token from the child's ENVIRONMENT (supplied via env= at the call site,
    see _image_cache_env) instead of taking `--build-arg GITHUB_TOKEN=<value>` — the credential never
    rides the command line, where a logged argv would leak it (manual-findings #1). When BAKE_TEST_GROUP
    is on, also asks for the test-group child image so the coder/SIT container starts test-ready
    (manual-findings #4)."""
    argv = ["python3", str(tool), "--repo", name, "--worktree", str(worktree)]
    if has_token:
        argv.append("--inject-github-token")
    if config.BAKE_TEST_GROUP:
        argv.append("--include-test-group")
    return argv


def _image_cache_env() -> tuple[dict | None, bool]:
    """(env, has_token) for spawning ruby_image_cache. When a GitHub token is available, returns a COPY of
    os.environ with GH_TOKEN set — WITHOUT mutating this long-lived process's global env (review #5: a
    global mutation would leak the token into every later subprocess). Returns (None, False) when there's
    no token, so the child just inherits the parent env unchanged."""
    tok = _gh_token()
    if tok:
        return {**os.environ, "GH_TOKEN": tok}, True
    return None, False


async def _docker(args: list[str], timeout: int = 120) -> int:
    """Run `docker <args>` best-effort; return the exit code (127 if docker/subprocess is unusable).
    Never raises — the persistent-container path is an optimization that must never break the run."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        try:
            await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return 124
        return proc.returncode
    except OSError:
        return 127


def _container_directive(state: OceanState, repo_dir: str) -> str:
    """Latency #1: the prompt snippet telling a Docker station to REUSE the orchestrator's persistent
    container instead of standing up its own. Empty string unless prep_container actually started one
    (container_ready) — so with the feature off, or if the container failed to start, every station's
    prompt is byte-for-byte what it is today and the agent uses its own recipe (fallback-safe)."""
    name = state.get("container_name") or ""
    if not (config.PERSISTENT_CONTAINER and state.get("container_ready") and name):
        return ""
    rd = repo_dir or "<repo_dir>"
    return (
        f"\n\nPERSISTENT CONTAINER: the orchestrator has already started ONE booted container "
        f"`{name}` from the pre-warmed image (gems installed). For ALL builds/tests do NOT `docker "
        f"run`, `docker build`, or start your own container — reuse `{name}`.\n"
        f"KNOWN-GOOD DOCKER RECIPE — use it as-is; do NOT rediscover the app dir or env by trial or a "
        f"whole-disk search (MM-14132/EXE-aea95d39 issue #3: reachability, coder, AND reviewer each "
        f"independently re-derived the SAME path + env this run, minutes wasted per station). Run the "
        f"app-dir resolve + `docker cp` + `docker exec` as ONE chained shell command — `$APP` does NOT "
        f"survive across separate Bash tool calls, and an empty `$APP` would `docker cp` into the "
        f"container ROOT, so the `[ -n \"$APP\" ]` guard is required:\n"
        f"    APP=$(docker exec {name} sh -lc 'if [ -f Gemfile ]; then pwd; else for d in /usr/src/app /app/fourkites/test /app; do [ -f \"$d/Gemfile\" ] && echo \"$d\" && break; done; fi'); \\\n"
        f"    [ -n \"$APP\" ] && docker cp {rd}/. {name}:\"$APP\"/ \\\n"
        f"      && docker exec -e RAILS_ENV=test -e FK_ENVIRONMENT=test -e AWS_REGION=us-east-1 "
        f"-e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test {name} bash -lc \"cd \\\"$APP\\\" && bundle exec rspec <changed_spec_files>\"\n"
        f"(the resolve prefers the container's WORKDIR when it holds the Gemfile — correct per-image: "
        f"tracking-service=/usr/src/app, MMCUW=/app/fourkites/test; a missing AWS_REGION is the recurring "
        f"`Aws::Errors::MissingRegionError`; unit specs use FK_ENVIRONMENT=test, the SIT uses --env qat.)\n"
        f"Re-run the cp+exec (same `$APP`, same shell) after each edit — the container persists across "
        f"your RED→GREEN cycles AND the reviewer/SIT stations; batch specs into ONE `rspec` call. Full env "
        f"in `local-docker-run.md` §3; fall back to your own `docker run` recipe only if a `docker exec {name}` probe fails.\n"
    )


def _sit_infra_directive(state: OceanState) -> str:
    """Latency #6: tell sit_run to bring the SIT infra up under a STABLE compose project and reuse it
    across the retry loop instead of tearing it down + re-bootstrapping each attempt. Empty unless
    WARM_SIT_INFRA is on — so with it off, sit_run's prompt is unchanged and it manages infra as today
    (fallback-safe). The graph removes the project at run end (teardown_container)."""
    if not config.WARM_SIT_INFRA:
        return ""
    project = config.sit_infra_project(state["execution_id"])
    return (
        f"\n\nWARM SIT INFRA: bring the local infra (localstack/es/redis/mock + bridges) up under a "
        f"STABLE docker-compose project name `{project}` (`docker compose -p {project} …`). If it is "
        f"ALREADY up (this is a code_fault / environment_failure re-entry), REUSE it as-is — do NOT "
        f"`compose down` or rebuild the stack between attempts; only reset mutable state (re-seed the "
        f"queue/mock expectations) and re-run the specs. The graph tears the project down at run end, "
        f"so leave it running when you finish.\n"
    )


def _service_slugs(state: OceanState) -> list[str]:
    """EVERY `<org>/<name>` slug to open/flip a PR on — MULTI-REPO aware. A ticket's single branch
    can span 1..N changed repos, and the coder reports `service_repo` as a COMMA-JOINED string
    (e.g. "cloudqwest/tracking-service, cloudqwest/ocean-service"). Passing that joined string to
    `gh --repo` fails, so open ONE PR per repo instead.

    Precedence — the coder's `service_repo` FIRST: it is what the coder actually PUSHED, so it is
    authoritative for which repos hold the branch. Only if it's empty do we fall back to
    `changed_repos`, then the researcher's `target_repos` — a Station-0 *guess* that is often
    over-scoped (candidate/read-only repos), and opening a PR on a repo the coder never pushed to
    would fail. (`changed_repos` is only populated at Station 6, i.e. after open_pr, so it's normally
    empty here; kept as a defensive middle tier.)"""
    raw: list[str] = (state.get("service_repo") or "").split(",")
    if not any(x.strip() for x in raw):
        raw = [c.repo if not isinstance(c, dict) else c.get("repo", "")
               for c in (state.get("changed_repos") or [])]
    if not any(x.strip() for x in raw):
        raw = [r.get("repo", "") for r in (state.get("target_repos") or []) if r.get("repo")]
    seen: set[str] = set()
    slugs: list[str] = []
    for r in (gitops.repo_slug(x.strip()) for x in raw if x and x.strip()):
        if r and r not in seen:
            seen.add(r)
            slugs.append(r)
    return slugs


# ------------------------------------------------- qat-handoff Phase 1.2: 9b judge corroboration
# The SAME agent that decides whether to defer the Step-9b judge panel also writes the field claiming
# whether it ran. Two of the three real artifacts say outright that it deferred ("No fresh-agent judge
# panel spawned", "intentionally deferred to the human review gate"), so a self-report is worth
# nothing here. This codebase has closed exactly this surface three times before, always the same way:
# deterministic DISK evidence outranks the agent's account of itself (F1's junit, Finding 2a's mock
# audit, Finding 2e's fingerprinting).
#
# The sentinel is what makes the count trustworthy. Step 5d's GAN dispatches up to 4 sub-agents per
# round and would otherwise be indistinguishable from a 9b judge -- a 12-dispatch 5d log must yield 0,
# not 12. ocean-qa-agent's SKILL.md requires every Step-9b judge dispatch to carry STEP9B-JUDGE in its
# description, and forbids it anywhere in Step 5d.
_STEP9B_SENTINEL = "STEP9B-JUDGE"
# `⚡ sub-agent started:` is the ONLY line counted (agents.py's task_started rendering). Deliberately
# not `⚙ Task` (the tool call) and not `task_progress`: one judge emits all three, and counting any of
# the others turns a single judge into N+2. Nor the skill's own `⟳ [Step 9b/round 1]` narration -- an
# agent that PRINTS it without dispatching must score 0, which is the whole point.
_TASK_STARTED_RE = re.compile(r"^⚡ sub-agent started: (?P<desc>.*?)\s*\(task (?P<tid>[^)]*)\)\s*$")
# Two rounds is the documented ceiling. A raw count above it means the sentinel is colliding with
# something else, so it must NOT quietly present as a healthy 2 -- it fails closed instead.
_MAX_9B_ROUNDS = 2


def _corroborated_judge_rounds(raw: int) -> int:
    """The trustworthy judge count from a raw observed count. Pure, so the FAIL-CLOSED rule is
    testable without driving the station.

    A raw count above the documented 2-round ceiling means the sentinel is colliding with something
    else. `min(raw, 2)` would present that collision as a perfectly healthy 2 -- the most dangerous
    possible answer, because it is indistinguishable from a real two-round pass. Zero instead: a
    collision is not evidence the judge ran.
    """
    return raw if 0 <= raw <= _MAX_9B_ROUNDS else 0


def _genuine_passed(review: dict, judge_rounds: int, census: dict) -> tuple[bool, str]:
    """qat-handoff Phase 2.1's pre-registered composite. Returns (ok, reason-if-not).

    genuine PASSED = verdict == PASSED
                   AND judge_rounds_observed >= 1   (current-pass, sentinel-matched, raw <= 2)
                   AND zero CANNOT-VERIFY among assertion-bearing methods, CROSS-CHECKED FROM THE FILE
                   AND the coverage floor is met

    Every conjunct after the first exists because the first is agent-written. `cannot_verify_methods`
    is read from the JSON, but an agent that simply OMITS entries makes that conjunct true -- a
    self-report RAISING trust, which is exactly what Phase 1.2 forbids. So the file's own
    `@pytest.mark.needs_env` count (which SKILL.md mandates precisely so the classification survives
    past the skill's run) may only ever RAISE the JSON's count, never lower it. That is the standing
    rule's fifth instance.

    Fails closed on an unparsed census: "could not count" is not "counted zero".
    """
    if not isinstance(review, dict):
        return False, "no review verdict on disk"
    if str(review.get("qa_authoring_review_verdict") or "").strip().upper() != "PASSED":
        return False, f"verdict is {review.get('qa_authoring_review_verdict') or 'absent'}"
    if judge_rounds < 1:
        return False, "no Step-9b judge dispatch corroborated from the station log this pass"
    if not census.get("parsed"):
        return False, "the authored test file could not be parsed, so its methods were not counted"
    claimed_cv = len(review.get("cannot_verify_methods") or [])
    observed_cv = census.get("needs_env_on_assertion_bearing", 0)
    # max(), never the JSON alone: the file may raise, never lower.
    if max(claimed_cv, observed_cv) > 0:
        return False, (f"{max(claimed_cv, observed_cv)} assertion-bearing method(s) are CANNOT-VERIFY "
                       f"(claimed {claimed_cv}, observed {observed_cv} via @pytest.mark.needs_env)")
    if not quality.coverage_floor_met(census):
        return False, (f"coverage floor: {census['skip_guarded']} of {census['total']} test method(s) "
                       f"are skip-guarded (> 1/3)")
    return True, ""


def _log_offset(exec_id: str, label: str) -> int:
    """Byte length of a station log right now, or 0. Never raises.

    Captured BEFORE `run_skill` so the scan below covers this invocation only. Deliberately not "scan
    after the last `===== pass start =====` separator": `_drive_with_retry` re-drives internally and
    appends a fresh separator, so a last-separator scan would zero a genuine count recorded before the
    retry (the `verdict_path is None` case).
    """
    try:
        return (config.artifacts_dir(exec_id) / f"{label}.log").stat().st_size
    except OSError:
        return 0


def _judge_rounds_observed(exec_id: str, label: str, since: int) -> int:
    """How many Step-9b judge sub-agents this pass ACTUALLY dispatched, from the station log.

    Returns 0 -- never None -- for a missing, unreadable or empty log: "no evidence" and "no judge"
    take the same fail-closed answer here, because the field this corroborates is only ever used to
    DOWNGRADE a claim, never to manufacture one.
    """
    try:
        raw = (config.artifacts_dir(exec_id) / f"{label}.log").read_text(errors="replace")[since:]
    except OSError:
        return 0
    seen: set[str] = set()
    for line in raw.splitlines():
        m = _TASK_STARTED_RE.match(line.strip())
        if m and _STEP9B_SENTINEL in m.group("desc"):
            # Dedupe on task id: a judge that reconnects or re-narrates must still count once.
            seen.add(m.group("tid") or line)
    return len(seen)


# ------------------------------------------------------------------ B1: multi-repo review coverage
def _origin_slug(repo_dir: Path) -> str:
    """`<org>/<name>` for a checkout's `origin`, or "" on any doubt. NEVER the raw URL.

    SECURITY INVARIANT: a real origin can carry credentials in the URL. This returns only a
    canonicalized slug, enforced at the source so it dominates every sink -- milestone, telemetry
    `output_summary` (which folds all **extra), run-report.json/.md, and monitor/app.py -> monitor.db
    and the web UI. Returning "" rather than guessing is deliberate: an empty slug is dropped by both
    callers below, whereas a half-parsed URL would become a permanent gap no bounce could clear.
    """
    try:
        proc = subprocess.run(["git", "-C", str(repo_dir), "remote", "get-url", "origin"],
                              capture_output=True, text=True, timeout=15)
    except (subprocess.TimeoutExpired, OSError):
        return ""
    if proc.returncode != 0:
        return ""
    url = (proc.stdout or "").strip()
    if not url:
        return ""
    # ssh (git@host:org/name.git), scp-ish (user@host:org/name), and https://host/org/name.git
    tail = url.split(":", 1)[-1] if "@" in url and "://" not in url else url
    tail = tail.rstrip("/")
    if tail.endswith(".git"):
        tail = tail[:-4]
    parts = [x for x in tail.replace("\\", "/").split("/") if x]
    if len(parts) < 2:
        return ""
    slug = f"{parts[-2]}/{parts[-1]}"
    # Route through the same canonicalizer `_service_slugs` uses, so the two sides of the comparison
    # cannot normalize differently.
    return gitops.repo_slug(slug)


def _branch_repos(state: OceanState) -> list[Path]:
    """Every checkout on disk that carries THIS ticket's branch.

    Scans `quality.repo_dirs(...)` plus the run dir's own siblings. The sibling step is load-bearing
    and measured: on EXE-90865766 `repo_dirs` alone returned just `[workspace]` because the coder
    cloned AS the workspace root, and the other clones were reachable only as siblings.

    `workspace.parent` is pinned LITERALLY to this run's own directory -- `config.workspace_dir` is
    `ARTIFACTS_ROOT/<exec_id>/workspace`, so the parent is `ARTIFACTS_ROOT/<exec_id>`, never
    ARTIFACTS_ROOT itself. That is the only thing separating this from a cross-run scan, which would
    pull other tickets' clones into this ticket's coverage set.

    `show-ref --verify refs/heads/<branch>`, NOT `rev-parse --verify <branch>`: executed, `rev-parse`
    also matches a TAG of the same name (rc=0) while `show-ref` excludes it (rc=1). Both correctly
    exclude remote-tracking-only refs. Both DO match a stale local branch, which is accepted --
    over-inclusion produces a loud stop, while the alternative (does HEAD carry the branch) would
    miss a repo the coder pushed and then checked out elsewhere.
    """
    branch = (state.get("branch") or "").strip()
    if not branch:
        return []
    workspace = config.workspace_dir(state["execution_id"])
    candidates = list(quality.repo_dirs(state.get("worktree_dir", ""), workspace))
    try:
        # `.git` filter first: the run dir holds ~46 entries of which ~6 are repos, so this saves
        # ~40 subprocesses. Same filter quality.repo_dirs._add applies.
        candidates += [d for d in sorted(workspace.parent.iterdir())
                       if d.is_dir() and (d / ".git").exists()]
    except OSError:
        pass
    out: list[Path] = []
    seen: set[str] = set()
    for d in candidates:
        key = str(d.resolve()) if d.exists() else str(d)
        if key in seen:
            continue
        seen.add(key)
        try:
            rc = subprocess.run(
                ["git", "-C", str(d), "show-ref", "--verify", "--quiet",
                 f"refs/heads/{branch}"], capture_output=True, timeout=15).returncode
        except (subprocess.TimeoutExpired, OSError):
            continue
        if rc == 0:
            out.append(d)
    return out


def _review_coverage(state: OceanState,
                     reviewed_dirs: list[Path]) -> tuple[list[str], list[str], list[str], str]:
    """(branch_repos, covered, gap, unverified) — ONE disk read, four values.

    `required` is the DISK-derived branch carriers UNIONED with `_service_slugs(state)` -- the exact
    list `open_pr` acts on. Anything less breaks the invariant this gate exists for ("every repo that
    gets a PR was reviewed"): executed, with the coder omitting `repo`, `open_pr` still opens PRs on
    both slugs while a `service_repo`-only `required` is empty, so BOTH would ship unreviewed.

    NO BRANCH => gap is EMPTY, unconditionally. On a failed first coder pass (branch="", repo="")
    `_service_slugs` falls through to the over-scoped `target_repos`, so a non-empty gap here would
    both mislabel the run (the real reason is review_budget_exhausted_no_diff) and shrink the coder's
    retry budget from MAX_REVIEW_ITERATIONS to this gate's cap of 1. With no branch nothing can ship,
    so the gate has nothing to protect -- the same reasoning as open_pr's own guard.

    COULD-NOT-RUN FAILS OPEN, LOUDLY: if the reviewed dir yields no slug (no origin, git missing),
    `unverified` is set and the gap is forced empty. Sibling precedent, stated at graph.py:330:
    "clean, or could-not-run (fails OPEN, loudly)". Failing closed would turn a broken git into a
    zero-PR halt on every run -- an infra fault wearing the costume of an unreviewed repo.
    """
    branch_slugs: list[str] = []
    for d in _branch_repos(state):
        slug = _origin_slug(d)
        if slug and slug not in branch_slugs:
            branch_slugs.append(slug)

    covered: list[str] = []
    for d in reviewed_dirs:
        slug = _origin_slug(d)
        if slug and slug not in covered:
            covered.append(slug)

    if not (state.get("branch") or "").strip():
        return branch_slugs, covered, [], "no branch recorded — coverage not derived"
    if not covered:
        return (branch_slugs, covered, [],
                "the reviewed checkout reported no origin slug — coverage not derived")

    # Case-insensitive: gitops.repo_slug does not lowercase, so an origin/report case mismatch would
    # otherwise be a permanent gap no bounce could clear.
    required = {s.lower(): s for s in branch_slugs}
    for s in _service_slugs(state):
        if s:
            required.setdefault(s.lower(), s)
    covered_lc = {s.lower() for s in covered}
    gap = [orig for lc, orig in sorted(required.items()) if lc not in covered_lc]
    return branch_slugs, covered, gap, ""


def _coverage_budget(state: OceanState, gap: list) -> tuple[int, bool]:
    """(attempts, stopped) for a coverage pass. Pure, so the budget is testable without a graph.

    Extracted rather than inlined in `harsh_reviewer` because a mutation sweep showed the inline
    version was unreachable from any test: every routing test set `review_coverage_stopped` by hand,
    so reverting the cap (`stopped = False`) and dropping the knob check BOTH left the suite green.
    A budget nothing exercises is the inertness class this gate exists to prevent, reproduced inside
    the gate. Same shape as `_real_service_gap` / `_eval_gate` above.

    `>` not `>=`: MAX counts BOUNCES and `attempts` has already been incremented for this pass, so
    `>=` would make the first gap a stop and the rework edge unreachable -- the defect
    MAX_QUALITY_GATE_ATTEMPTS' own comment records.

    The knob is read HERE as well as in the router so that knob-off records the gap without either
    stopping or bouncing.
    """
    attempts = state.get("review_coverage_attempts", 0) + (1 if gap else 0)
    stopped = bool(gap) and attempts > config.MAX_COVERAGE_ATTEMPTS and config.MULTI_REPO_REVIEW_GATE
    return attempts, stopped


# ------------------------------------------------------------------ Station 0
async def researcher(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 0, "start")
    jira.transition(state["ticket_id"], "In Progress")   # best-effort; no-op without a Jira token
    # Finding 3: identify the run BEFORE dispatching, so this station's own tool failures are
    # capturable too. The domain bucket isn't known until the verdict comes back (it's what the
    # researcher produces), so the capture hook stays disarmed for this one call and is re-armed with
    # the bucket below — a judge review flagged that Station 0, the heaviest MCP consumer, was the
    # only station whose failures were structurally invisible. Carrying a lesson keyed by an unknown
    # bucket would not be recallable anyway; what this buys is that ticket/exec identity is never the
    # reason a capture is dropped.
    agents.set_run_context(ticket_id=state["ticket_id"], execution_id=state["execution_id"])
    v: schemas.ResearchVerdict = await agents.run_agent(
        agent_md="research.md",   # ocean-coding-agent worker (fk-aideveloper single source)
        node="researcher",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Research {state['ticket_id']} and classify the route "
            f"(coding vs rca vs sop vs loft vs ff_onboarding). Record the language-scoped "
            f"build_env for each target repo (ruby=docker, java/go=native).\n\n{_summary(state)}"
        ),
        verdict_model=schemas.ResearchVerdict,
    )
    # G2: re-sync each target repo's sibling local checkout to the default-branch tip ONCE here,
    # before the analysis stations (SME/dep-resolver/reachability) read it — so they all analyze
    # current code instead of a stale base (the runs showed 4 stations each telling the coder to
    # re-sync while nobody actually did). Deterministic, best-effort, never blocks the run.
    sync_status = [gitops.sync_local_checkout(gitops.repo_slug(r.get("repo", "")))
                   for r in (v.target_repos or []) if r.get("repo")]
    # Finding 3 (architecture review, "Give Pipeline Memory"): recall recurring failure patterns
    # this SAME domain has hit on prior tickets (written by stop_run's record_failure call) and fold
    # them into _summary() so every downstream station (SME, dep-resolver, qa_scenarios, coder,
    # harsh_reviewer, sit_run) sees them automatically -- without this, ticket 200 in a domain starts
    # exactly as blind as ticket 1 did, however many times the SAME failure has already recurred.
    # Finding 3: from here on, every worker's TOOL failures are captured into the lesson store keyed
    # by this domain (agents._capture_tool_failure, a real PostToolUseFailure hook). Must be set before
    # any downstream station runs, and can only be set once the bucket is actually known.
    agents.set_run_context(domain_bucket=v.domain_bucket, ticket_id=state["ticket_id"],
                           execution_id=state["execution_id"])
    recurring = lessons.recall_lessons(v.domain_bucket) if v.domain_bucket else []
    telemetry.station_event(state["execution_id"], 0, "end", route=v.route,
                            domain_bucket=v.domain_bucket, recurring_lessons=len(recurring),
                            checkout_sync="; ".join(sync_status) or "no target repo to sync")
    return {"route": v.route, "research_packet": _load_json(v.packet_path),
            "target_repos": v.target_repos, "domain_bucket": v.domain_bucket,
            "recurring_lessons": recurring}


# ------------------------------------------------------------------ Station 0.5 — ocean SME consult
_SME_BY_BUCKET = {
    "callback_notification": "sme-callback-notification.md",
    "load_creation": "sme-load-creation.md",
    "ocean_tracking_milestones": "sme-ocean-milestones.md",
    "ocean_data_quality": "sme-ocean-data-quality.md",
    "jt_data_quality": "sme-jt-data-quality.md",
    "event_processing_failure": "sme-event-processing-failure.md",
}

# BOOT CHECK. The bucket vocabulary lives in schemas.DOMAIN_BUCKETS (the ResearchVerdict validator
# canonicalizes against it); this map turns each bucket into the SME file to dispatch. A bucket in
# one and not the other fails SILENTLY at runtime — `_SME_BY_BUCKET.get()` misses, no SME is
# consulted, and the run continues with a `skip` event nobody is watching. That is the same fork
# class as the preflight list and the SME/RCA drift, so it fails at IMPORT instead: an assertion
# here is noticed by the first test run and by `ocean-pipeline --print-graph`, long before a ticket
# is routed to a bucket that dispatches nothing.
assert set(_SME_BY_BUCKET) == set(schemas.DOMAIN_BUCKETS), (
    "domain buckets disagree — schemas.DOMAIN_BUCKETS and nodes._SME_BY_BUCKET must match. "
    f"only in schemas: {sorted(set(schemas.DOMAIN_BUCKETS) - set(_SME_BY_BUCKET))}; "
    f"only in the SME map: {sorted(set(_SME_BY_BUCKET) - set(schemas.DOMAIN_BUCKETS))}")


async def sme_consult(state: OceanState) -> dict:
    """Graph-owned SME dispatch: the graph (not the researcher) picks the ocean domain SME by
    domain_bucket and consults it for ownership/reuse guidance. A no-op when no bucket applies."""
    bucket = state.get("domain_bucket") or ""
    exec_id = state["execution_id"]
    sme_md = _SME_BY_BUCKET.get(bucket)
    if not sme_md:
        telemetry.station_event(exec_id, 0.5, "skip", domain_bucket=bucket or "(none)")
        return {"sme_findings": {}}
    telemetry.station_event(exec_id, 0.5, "start", domain_bucket=bucket)
    v: schemas.SmeVerdict = await agents.run_agent(
        agent_md=sme_md,   # fk-aideveloper SME (referenced expert knowledge; resolves via fallback)
        node="sme_consult",
        ticket_id=state["ticket_id"],
        execution_id=exec_id,
        task_prompt=(
            f"Static-architecture question for {state['ticket_id']} (domain: {bucket}). Which "
            f"repo/file/mechanism owns the change this ticket needs, and what should the coder reuse "
            f"or extend? Answer from your curated knowledge; fall back to grep / the code graph only "
            f"where uncovered. Do NOT write code or open anything.\n\n"
            f"Research summary:\n{_brief(state.get('research_packet'))}\n\n{_summary(state)}"
        ),
        verdict_model=schemas.SmeVerdict,
    )
    telemetry.station_event(exec_id, 0.5, "end", findings=len(v.findings))
    return {"sme_findings": {"summary": v.summary, "findings": v.findings}}


# ------------------------------------------------------------------ Station 0.6 — pre-warm Ruby image
def _kill_proc_group(proc) -> None:
    """SIGKILL the subprocess AND its whole process group (B2). ruby_image_cache spawns `docker build`
    as a grandchild; a plain proc.kill() reaps only the Python wrapper, leaving the build ORPHANED and
    running (~1hr observed in EXE-6fca4a71 — wasted CPU + a duplicate rebuild by prep_container, B3).
    Requires the child to be spawned with start_new_session=True so it leads its own group."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.kill()
    except (ProcessLookupError, OSError):
        pass


async def _clone_for_prewarm(repo: str, worktree: Path) -> bool:
    """I8: clone a repo's FIRST-EVER local checkout so prep_image gets a chance to pre-warm it,
    instead of silently skipping (leaving the coder to eat a fully cold build alone later, with no
    earlier opportunity to warm it). Best-effort and bounded — `gh repo clone` reuses whatever `gh`
    auth is already set up elsewhere in this codebase (no manual token/URL construction needed).
    True on success (worktree now has a `.git` dir AND `gh` reported a clean exit); False on any
    failure — caller treats that exactly like today's existing "no checkout" skip, never raises.
    A failed clone that got as far as creating `.git` (git does this before fetching any refs/
    objects, so an auth/network failure partway through still leaves a `.git` skeleton behind) is
    torn down on the way out -- otherwise the NEXT run's `worktree.exists() and not .git` guard
    above would never trip (the broken `.git` is already there), the next attempt would try to
    clone into a non-empty directory and fail immediately, and the broken checkout would persist
    forever, silently reused as "warm" by every downstream station keyed on the same worktree path
    (sync_local_checkout's researcher/SME/dep-resolver/reachability callers included)."""
    if worktree.exists() and not (worktree / ".git").exists():
        return False   # exists but isn't a git repo -- don't clone into/clobber an unknown directory
    worktree.parent.mkdir(parents=True, exist_ok=True)
    try:
        proc = await asyncio.create_subprocess_exec(
            "gh", "repo", "clone", repo, str(worktree),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            start_new_session=True)
        try:
            await asyncio.wait_for(proc.communicate(), timeout=config.CLONE_PREWARM_TIMEOUT)
        except asyncio.TimeoutError:
            _kill_proc_group(proc)
            shutil.rmtree(worktree, ignore_errors=True)
            return False
    except OSError:
        shutil.rmtree(worktree, ignore_errors=True)
        return False
    if proc.returncode == 0 and (worktree / ".git").exists():
        return True
    shutil.rmtree(worktree, ignore_errors=True)
    return False


async def prep_image(state: OceanState) -> dict:
    """#2 latency: pre-build/cache the Ruby ocean Docker image CONCURRENTLY with sme_consult/
    dep_resolver, so it is hot before the coder (the ~32m station) and reachability need it -- moving
    the image build OFF the coder's critical path. Runs only on the PARALLEL_ANALYSIS path (the graph
    fans it out beside sme/dep and joins at reachability_gate). Best-effort and non-blocking: no
    Docker, no local checkout, a non-Ruby repo, a missing tool, a build error, or a timeout all no-op
    cleanly and the coder just builds normally. Keyed by Gemfile.lock+Dockerfile hash inside
    ruby_image_cache, so a warm cache returns fast."""
    exec_id = state["execution_id"]
    telemetry.station_event(exec_id, 0.6, "start")
    # E1 (run-monitoring-findings.md): this node does its own subprocess/Docker work directly and
    # never calls run_agent/run_skill (the only other place that prints a "▶" header), so without
    # this the monitor's last-seen station label goes stale for the ENTIRE image-build duration
    # (observed 2+ stations / 4-7 min behind reality during a concurrent build).
    ui.station_start("prep_image")
    target_repos = state.get("target_repos") or []
    # Build-slot capacity gate (only for Ruby/Docker tickets — a Go/Java-only ticket never even
    # attempts this): non-blocking single try, matching this function's own already-fallback-safe
    # posture. No slot/headroom -> skip the WHOLE prewarm cleanly (not a partial run) — the coder just
    # builds normally, exactly like every other "no-op" path this function already has.
    if _any_docker_repo(target_repos) and not await asyncio.to_thread(_try_acquire_build_slot, exec_id, target_repos):
        telemetry.station_event(exec_id, 0.6, "skip", reason="no build capacity (slot/headroom)")
        return {}
    try:
        tool = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-qa-agent" / "tools" / "ruby_image_cache.py"
        results: list[str] = []
        for r in target_repos:
            repo = r.get("repo", "")
            name = repo.split("/")[-1]
            is_ruby = "docker" in (r.get("build_env") or "").lower() or (r.get("language") or "").lower() == "ruby"
            worktree = config.PROJECTS_ROOT / name
            if not repo or not is_ruby:
                continue
            if not tool.exists():
                results.append(f"{name}: skip (image-cache tool not found)"); continue
            if not (worktree / ".git").exists():
                # I8 (run-monitoring-findings-06af6088.md): a repo's FIRST-EVER touch on this
                # machine has no checkout yet, so this used to just skip -- meaning prep_image
                # structurally could NEVER pre-warm a first-time repo, and the coder later ate a
                # fully cold, auth-requiring, >10-min build alone (the exact D6-violation/
                # background-and-yield incident this finding traces). `sync_local_checkout`
                # (gitops.py) deliberately does NOT clone a missing repo -- "the coder clones fresh
                # into its own workspace" is its own documented contract, used by the read-only
                # analysis stations (researcher/SME/dep-resolver/reachability) where a clone isn't
                # this control plane's job. prep_image is different: its ENTIRE purpose is doing
                # Docker work off the coder's critical path, so cloning here (once, best-effort,
                # bounded) directly serves that purpose instead of leaving the coder to eat the
                # full cold-checkout-then-cold-build cost with no earlier chance to warm it.
                if not await _clone_for_prewarm(repo, worktree):
                    results.append(f"{name}: skip (no local checkout, clone failed/unavailable)"); continue
            try:
                env, has_token = _image_cache_env()
                proc = await asyncio.create_subprocess_exec(
                    *_image_cache_argv(tool, name, worktree, has_token), env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True)   # B2: own process group so a timeout kills the docker build too
                try:
                    out, _ = await asyncio.wait_for(proc.communicate(), timeout=config.IMAGE_PREWARM_TIMEOUT)
                except asyncio.TimeoutError:
                    _kill_proc_group(proc)   # B2: kill the whole group, not just the Python wrapper
                    results.append(f"{name}: prewarm timed out (coder will build)"); continue
                except BaseException:
                    # start_new_session=True moved this child OUT of the pipeline's own process
                    # group (B2's fix for the timeout case), so a Ctrl-C/kill of the pipeline itself
                    # (CancelledError here) no longer reaches it via the parent's group — it would
                    # otherwise be orphaned exactly like the timeout case this same fix targets.
                    _kill_proc_group(proc)
                    raise
                tail = ((out or b"").decode(errors="replace").strip().splitlines() or [""])[-1]
                results.append(f"{name}: {'ready' if proc.returncode == 0 else 'build failed'} ({tail[:80]})")
            except OSError as e:
                results.append(f"{name}: prewarm error ({e})")
        telemetry.station_event(exec_id, 0.6, "end", prewarm="; ".join(results) or "no ruby target to prewarm")
        return {}
    finally:
        _release_build_slot(exec_id)


# ------------------------------------------------------------------ Station 3.5 — persistent container
async def prep_container(state: OceanState) -> dict:
    """Latency #1: start ONE booted container from the pre-warmed image so the coder/reviewer/SIT
    stations reuse it (docker cp + docker exec) instead of each doing its own `docker run` +
    image/env re-derivation (D4/D5/P1). Best-effort and fallback-safe: not Ruby, no Docker, no image,
    or a start failure all leave container_ready=False and the stations use their own recipe."""
    exec_id = state["execution_id"]
    telemetry.station_event(exec_id, 3.5, "start")
    ui.station_start("prep_container")   # E1 -- same reasoning as prep_image above
    ruby = next((r for r in (state.get("target_repos") or [])
                 if "docker" in (r.get("build_env") or "").lower() or (r.get("language") or "").lower() == "ruby"),
                None)
    tool = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-qa-agent" / "tools" / "ruby_image_cache.py"
    if not ruby:
        telemetry.station_event(exec_id, 3.5, "skip", reason="no ruby target repo")
        return {"container_ready": False}
    name = (ruby.get("repo") or "").split("/")[-1]
    worktree = config.PROJECTS_ROOT / name
    if not shutil.which("docker") or not tool.exists() or not (worktree / ".git").exists():
        telemetry.station_event(exec_id, 3.5, "skip", reason="docker / image-cache tool / checkout absent")
        return {"container_ready": False}
    # Build-slot capacity gate: non-blocking single try (this function is already best-effort/
    # fallback-safe — coder/harsh_reviewer/reachability_gate all have their own recipe for
    # container_ready=False). Deliberately NOT released here on success: this hold spans the whole
    # PERSISTENT_CONTAINER lifetime (coder -> harsh_reviewer -> sit_run all REUSE this same container
    # via _container_directive) and is released only by teardown_container — a container sitting idle
    # between stages is still consuming real Docker memory the whole time, so the capacity hold must
    # reflect that, not just the moment this function itself is running.
    if not await asyncio.to_thread(_try_acquire_build_slot, exec_id, state.get("target_repos")):
        telemetry.station_event(exec_id, 3.5, "skip", reason="no build capacity (slot/headroom)")
        return {"container_ready": False}
    container = f"ocean-{name}-{exec_id}"
    try:
        # Resolve (cache-hit if prep_image already built it) the pre-warmed image tag.
        env, has_token = _image_cache_env()
        proc = await asyncio.create_subprocess_exec(
            *_image_cache_argv(tool, name, worktree, has_token), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True)   # B2: own process group so a timeout kills the docker build too
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=config.IMAGE_PREWARM_TIMEOUT)
        except asyncio.TimeoutError:
            _kill_proc_group(proc)   # B2: kill the whole group, not just the Python wrapper
            telemetry.station_event(exec_id, 3.5, "skip", reason="image resolve timed out")
            _release_build_slot(exec_id)   # nothing came up — don't hold capacity for it
            return {"container_ready": False}
        except BaseException:
            # Same reasoning as prep_image: start_new_session=True took this child out of the
            # pipeline's own process group, so a Ctrl-C/kill of the pipeline (CancelledError here)
            # no longer reaches it via the parent's group — orphaned unless killed explicitly.
            _kill_proc_group(proc)
            _release_build_slot(exec_id)   # aborting before the container ever started — free the hold
            raise
        tag = (((out or b"").decode(errors="replace").strip().splitlines() or [""])[-1]) if proc.returncode == 0 else ""
        if not tag:
            telemetry.station_event(exec_id, 3.5, "skip", reason="image tag unresolved")
            _release_build_slot(exec_id)
            return {"container_ready": False}
        await _docker(["rm", "-f", container])           # clear any stale same-named container
        started = await _docker(["run", "-d", "--name", container, "--entrypoint", "sleep", tag, "infinity"])
        ready = started == 0
        telemetry.station_event(exec_id, 3.5, "end",
                                container=container if ready else f"start failed (docker exit {started})")
        if not ready:
            _release_build_slot(exec_id)   # start failed — nothing to hold capacity for
        return {"container_name": container if ready else "", "container_ready": ready}
    except OSError as e:
        telemetry.station_event(exec_id, 3.5, "skip", reason=f"prep error ({e})")
        _release_build_slot(exec_id)
        return {"container_ready": False}


async def teardown_container(state: OceanState) -> dict:
    """Remove the run-scoped Docker resources at run end (both terminal paths route through here):
    the persistent test container (#1) and the warm SIT infra project (#6). Best-effort; a no-op for
    whatever wasn't created. Passes state straight through — it's a cleanup node, not a gate."""
    exec_id = state["execution_id"]
    removed = []
    name = state.get("container_name") or ""
    if config.PERSISTENT_CONTAINER and name:
        await _docker(["rm", "-f", name])
        removed.append(name)
    if config.WARM_SIT_INFRA:
        project = config.sit_infra_project(exec_id)
        # remove the compose project AND any stray containers labeled/named for it (best-effort both ways)
        await _docker(["compose", "-p", project, "down", "-v", "--remove-orphans"], timeout=180)
        removed.append(project)
    # Release the SIT concurrency slot (manual-findings #18) so a queued run can proceed. Safe/no-op if
    # this run never acquired one; the stack is being torn down above, so the slot is genuinely free now.
    _release_sit_slot(exec_id)
    # Release the build-slot hold prep_container acquired (spans coder/harsh_reviewer/sit_run's reuse
    # of the same persistent container) — safe/no-op if this run never acquired one.
    _release_build_slot(exec_id)
    if removed:
        telemetry.station_event(exec_id, 3.6, "end", removed="; ".join(removed))
    return {}


# ------------------------------------------------------------------ Station 1
async def dep_resolver(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 1, "start")
    v: schemas.DependencyVerdict = await agents.run_agent(
        agent_md="dep-resolve.md",   # ocean-coding-agent worker (fk-aideveloper single source)
        node="dep_resolver",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Resolve dependencies/blockers for {state['ticket_id']}. Self-solve where possible; "
            f"flag only true blockers.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.DependencyVerdict,
    )
    telemetry.station_event(state["execution_id"], 1, "end", blocking=v.blocking,
                            blocking_questions=len(v.blocking_open_questions))
    # MM-14816 (G20): thread the resolver's placeholder work into STATE so it survives to the
    # reachability worker (verifies it) and the coder — a report-file-only record reaches nobody
    # (both downstream workers are banned from cross-station file reads). See schemas.DependencyVerdict.
    return {"dependency_report": {"report_path": v.report_path, "notes": v.notes},
            "dependency_blocking": v.blocking,
            "resolved_placeholders": v.resolved_placeholders,
            "unresolved_placeholders": v.unresolved_placeholders,
            "blocking_open_questions": v.blocking_open_questions}


# ------------------------------------------------------------------ Station 1.5
async def reachability_gate(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 1.5, "start")
    # MM-14816 (G20): if dep_resolver surfaced a genuine product/UX decision that BLOCKS an AC, stop
    # BEFORE any coding — short-circuit at ENTRY (skip this gate's own verification, the GAN, and the
    # coder). The graph's after_reachability conditional routes to stop_run's blocked branch (which
    # posts the questions to Jira + emits the oas-autodev block marker). Raising it here, not after a
    # partial diff exists, is the whole point ("resolve/ask before coding").
    _blocking_qs = [q for q in (state.get("blocking_open_questions") or []) if str(q).strip()]
    if _blocking_qs:
        telemetry.station_event(state["execution_id"], 1.5, "blocked_short_circuit",
                                blocking_questions=len(_blocking_qs))
        return {"reachability_report": {}, "reachability_blocking": True}
    # NOTE: an earlier "skip the gate when dep_resolver made zero reachability claims"
    # optimization (has_reachability_claims) was REMOVED (MM-14816) -- its skip signal covered only
    # 4 of the gate's 5 verification categories (it omitted "build-new-mechanism" self-solves and
    # "blocked" classifications), so it could skip a legitimate check; the saving fired only on rare
    # claim-less tickets. The gate is a safety check -- always run it. On a genuinely claim-less
    # ticket it verifies nothing and passes cheaply, which is the correct, fail-safe behavior.
    # Same build-slot gate as coder/harsh_reviewer — defensive here specifically: reachability's own
    # Docker usage (a fallback ruby_image_cache.py call in reachability.md) isn't confirmed wired to
    # _container_directive today, so this errs toward protecting it; can be removed later if a
    # transcript audit shows reachability never actually touches Docker in practice.
    own_build_slot = _any_docker_repo(state.get("target_repos")) and not state.get("container_ready")
    if own_build_slot:
        slot = await _acquire_build_slot(state["execution_id"], state.get("target_repos"))
        telemetry.station_event(state["execution_id"], 1.5, "build_slot", build_slot=slot)
    try:
        v: schemas.ReachabilityVerdict = await agents.run_agent(
            agent_md="reachability.md",   # ocean-coding-agent worker (fk-aideveloper single source)
            node="reachability_gate",
            ticket_id=state["ticket_id"],
            execution_id=state["execution_id"],
            task_prompt=(
                f"Execution-verify every ALREADY_MET / self-solve / blocked / cross-repo claim for "
                f"{state['ticket_id']}. Emit the binding reachability-report.json.\n\n{_summary(state)}"
                # MM-14816 (G20): the resolver filled these [ENGINEER TO FILL]/blank values from
                # code/logs/comments. VERIFY each against its cited source (a resolved cross-repo/runtime
                # value is a cross-repo claim — same job) and write the VERIFIED set into the report so the
                # coder receives it; DOWNGRADE any that don't verify to unresolved_placeholders (never pass
                # an unverified value as fact). See reachability.md's placeholder-verification section.
                + (f"\n\nRESOLVED PLACEHOLDERS to verify against their cited source, then emit the verified "
                   f"set into the report (downgrade unverifiable ones):\n"
                   f"{_brief(state.get('resolved_placeholders'), limit=8000)}"
                   if state.get("resolved_placeholders") else "")
            ),
            verdict_model=schemas.ReachabilityVerdict,
        )
    finally:
        if own_build_slot:
            _release_build_slot(state["execution_id"])
    telemetry.station_event(state["execution_id"], 1.5, "end", blocking=v.blocking)
    return {"reachability_report": _load_json(v.report_path), "reachability_blocking": v.blocking}


# ------------------------------------------------------------ 1.55 — blocked-open-questions human gate
async def blocked_review_gate(state: OceanState) -> dict:
    """MM-14816 (G20): the resolver surfaced AC-blocking product/UX open questions. Posting them to the
    customer Jira ticket is an OUTWARD-FACING action, so it is gated behind a HUMAN decision in the
    Monitor UI (always ask — no auto override), NOT fired automatically. 3-way, resumed via
    `--blocked <answer|post|reject> [--note ...]`:
      • answer → the human already knows the answers (in --note): fold them in, clear the block, and
        CONTINUE (loop back to reachability, then coding). No Jira post.
      • post   → the human doesn't know: approve posting the questions to Jira for product/UX (stop_run
        posts + emits the oas-autodev marker → parks AWAITING_INPUT → resumes when answered).
      • reject → skip posting but CONTINUE anyway (human-authorized proceed-despite): the questions ride
        forward as caveats to the coder + PR body (coder stays C10/C11-bound; nothing invented silently).
    Reached only when reachability short-circuited on a non-empty, stripped blocking set."""
    exec_id = state["execution_id"]
    questions = [q for q in (state.get("blocking_open_questions") or []) if str(q).strip()]
    if not questions:                      # defensive — routed here only when non-empty
        return {"blocked_decision": "reject"}
    from langgraph.types import interrupt
    raw = interrupt({
        "action": "blocked_open_questions",
        "ticket_id": state["ticket_id"],
        "questions": questions,
        "prompt": ("This ticket has open product/UX questions that block implementation. Review them, "
                   "then resume with ONE of:\n"
                   "  --blocked answer --note '<your answers>'   (you know them → pipeline continues)\n"
                   "  --blocked post                             (post to Jira for product/UX)\n"
                   "  --blocked reject                           (skip posting; continue, questions flagged)"),
    })
    decision = (raw.get("decision") if isinstance(raw, dict) else str(raw)) or "reject"
    note = raw.get("note", "") if isinstance(raw, dict) else ""
    telemetry.station_event(exec_id, 1.55, "decision", blocked_decision=decision)
    if decision == "post":
        # keep blocking_open_questions set → stop_run's blocked branch posts + returns final_status=blocked
        return {"blocked_decision": "post"}
    if decision == "answer":
        # human supplied answers → clear the block, carry the answers to the coder, loop back to reachability
        return {"blocked_decision": "answer", "blocked_answers": note,
                "open_question_caveats": [], "blocking_open_questions": []}
    # reject → continue with the questions as visible caveats (no post), clear the block so the graph proceeds
    return {"blocked_decision": "reject", "blocked_answers": "",
            "open_question_caveats": questions, "blocking_open_questions": []}


# ------------------------------------------------------------------ 1.6 — GAN-hardened test scenarios (MM-14738)
def _gan_verdict(partial: dict) -> str:
    """I7 (run-monitoring-findings-06af6088.md): the GAN verdict contract is unenforced prose, and
    two real runs in the SAME batch persisted it in different shapes — one top-level
    (`{"qa_gan_verdict": "REJECT"}`), one nested (`{"qa_gan": {"qa_gan_verdict": "REJECT"}}`, no
    top-level key at all). A bare `partial.get("qa_gan_verdict", "")` silently returns "" for the
    nested shape, dropping a genuine REJECT + open HIGH gap into state as if it never happened.
    Tolerate both shapes; top-level wins if somehow both are present (it's the documented/primary
    contract). A malformed `qa_gan` (e.g. a bare string instead of the nested-object shape) must
    fall back to "" rather than raise -- an agent emitting a third, unanticipated shape should
    degrade the verdict, not crash the node."""
    top = partial.get("qa_gan_verdict")
    if top:
        return top
    nested = partial.get("qa_gan")
    return nested.get("qa_gan_verdict", "") if isinstance(nested, dict) else ""


# D1 STEP 1 (gan-decision.md, judge-approved v5). The GAN's residual HIGH gaps never reached the
# coder, though nodes.py calls the scenarios "the fixed target coder must satisfy" -- confirmed by
# four independent traces. `_gan_verdict` above aliases the verdict STRING only, so a gap reader is
# a PREREQUISITE for that wiring, not a later step.
#
# SIX key names appear across the eight real artifacts and only FOUR are residual-gap lists. Reading
# the other two would hand the coder items the GAN panel explicitly REJECTED, which is worse than
# reading nothing:
#   * `residual_open_items_for_downstream` (MM-14060) -- LOW/EXECUTABILITY/ADVISORY/OBSERVABILITY,
#     zero HIGH entries.
#   * `documented_gaps_and_deferrals` (MM-14132) -- NO severity field at all; statuses are DEFERRED,
#     OUT OF SCOPE, MOVE TO UNIT LEVEL, and one entry records the panel ruling the underlying bug a
#     FALSE POSITIVE.
# Both are excluded BY NAME here, and the severity filter below is a second, independent guard:
# neither key can produce a HIGH entry, so either mechanism alone would suffice. That redundancy is
# deliberate -- this is the one reader whose failure mode is "coder implements a rejected finding".
_GAN_GAP_KEYS = ("residual_high_gaps", "qa_gan_residual_gaps", "remaining_gaps")
_GAN_GAP_KEYS_EXCLUDED = ("residual_open_items_for_downstream", "documented_gaps_and_deferrals")
# Inner fields drift across artifacts too: summary is `summary` (14312, 14381) or `gap` (14457,
# 14475), and the verification field is `verified` / `reverified` / `status` / `drafted_fix`.
_GAN_SUMMARY_FIELDS = ("summary", "gap", "title", "description")


def _gan_gaps(partial: dict) -> list[dict]:
    """The GAN's residual HIGH gaps, normalized to [{severity, summary}]. Never raises.

    HIGH ONLY. MEDIUM/LOW/ADVISORY are dropped: the payload is measured at 1-2 HIGH entries per run
    (MM-14312 1, MM-14381 1 (+2 MED), MM-14457 2, MM-14475 2), and widening it turns a short,
    code-actionable list into noise the coder will skim.

    `gan_rounds[].real_gaps` is read; `real_gaps_fixed` is NOT -- rounds 1-2 of MM-14381 use that
    second key for gaps the GAN ALREADY FIXED. A prefix or substring match on "real_gaps" would
    catch it and hand the coder work that is already done, which is why the round reader tests the
    key name for EQUALITY.
    """
    out: list[dict] = []
    seen: set[str] = set()

    def _add(items) -> None:
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            sev = str(it.get("severity") or it.get("priority") or "").strip().strip("*_ ").upper()
            if sev != "HIGH":
                continue
            summary = ""
            for f in _GAN_SUMMARY_FIELDS:
                if it.get(f):
                    summary = str(it[f]).strip()
                    break
            if not summary or summary in seen:
                continue
            seen.add(summary)
            # Only severity + summary. The artifacts wrap each entry in test metadata
            # (`drafted_fix: "S15 ..."`, `verified`, `reverified`, `status`) that is about the GAN's
            # own bookkeeping, not about the code -- passing it on would read to the coder as an
            # instruction to touch the test.
            out.append({"severity": "HIGH", "summary": summary})

    if not isinstance(partial, dict):
        return []
    for key in _GAN_GAP_KEYS:
        _add(partial.get(key))
    nested = partial.get("qa_gan")
    if isinstance(nested, dict):
        for key in _GAN_GAP_KEYS:
            _add(nested.get(key))
    for rnd in partial.get("gan_rounds") or []:
        # EQUALITY, never a prefix: `real_gaps_fixed` is a different concept (already fixed).
        if isinstance(rnd, dict):
            _add(rnd.get("real_gaps"))
    return out


async def qa_scenarios(state: OceanState) -> dict:
    """Design + GAN-harden the SIT test scenarios BEFORE any code exists (skill Station-independent —
    calls `ocean-qa-agent` directly, not `ocean-automation-testing`, since that skill's own contract
    is post-code/post-PR; see ocean-qa-agent SKILL.md Steps 2e/5d + `--scenarios-only`). TDD-style: the
    hardened scenarios become the fixed target `coder` must satisfy, and `sit_author` later writes the
    actual pytest from them (post-review) rather than designing fresh. A code_fault rework re-enters at
    `coder` directly and never re-runs this node — the scenarios don't change because the code did."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 1.6, "start")
    scenarios_path = config.qa_scenarios_path(tid)
    scenarios_path.parent.mkdir(parents=True, exist_ok=True)
    if scenarios_path.exists():
        scenarios_path.unlink()  # fresh attempt -- don't let a stale prior-attempt file fool the
                                  # verdict_path retry-guard below into thinking THIS attempt already
                                  # succeeded (same pattern as sit_resolve's automation_verdict_path).
    # MM-14793 (GAN A1): clear any stale per-round GAN progress sidecar left by a PRIOR, UNRELATED
    # run for this ticket, so Step 5d resumes only against THIS run's rounds. But an unconditional
    # unlink here also destroys a genuinely resumable sidecar: a real abort+retry (e.g. EXE-a43959ff's
    # spend-limit abort mid-round-3) re-enters this SAME node on a FRESH process (`--resume EXE-...`)
    # with the SAME execution_id -- from this node's perspective that's indistinguishable from any
    # other "entry into qa_scenarios" unless the sidecar itself says whose run it belongs to (I3,
    # run-monitoring-findings.md: the driver had no positive signal to trust its OWN sidecar, so it
    # safely defaulted to fresh and threw away ~2 completed GAN rounds). Fix: only unlink when the
    # sidecar's stored execution_id does NOT match this run's -- a same-run sidecar (this run's own
    # aborted attempt) is left in place for Step 5d's resume check to find and trust.
    gan_progress_path = scenarios_path.with_name(scenarios_path.stem + "-gan-progress.json")
    if gan_progress_path.exists():
        try:
            sidecar_data = json.loads(gan_progress_path.read_text())
            # SKILL.md's Step 5d instruction specifies a single JSON OBJECT (not a bare array) --
            # but this is LLM-followed prose, not an enforced schema, so a non-compliant agent could
            # still write something else (e.g. a bare list). Guard the .get() call itself rather than
            # assume the shape, so an unexpected type degrades to "no match" instead of an AttributeError
            # crashing the node on exactly the abort+retry re-entry this fix targets.
            sidecar_exec_id = sidecar_data.get("execution_id") if isinstance(sidecar_data, dict) else None
        except (json.JSONDecodeError, OSError):
            sidecar_exec_id = None   # corrupt/partial (a crash mid-write) -- treat as no match, same
                                     # safe-default-to-fresh behavior as the old unconditional unlink
        if sidecar_exec_id != exec_id:
            gan_progress_path.unlink()
    # S2 (run-monitoring-findings.md): serialize the GAN loop machine-wide (config.MAX_CONCURRENT_GAN)
    # so a batch's tickets don't all fan out 4 sub-agents/round at once and contend for local CPU/LLM
    # concurrency (and the shared TestRail API downstream). Mandatory/waiting acquisition -- this node
    # must run, it just may queue briefly first (same posture as coder/harsh_reviewer's build-slot wait).
    gan_slot = await _acquire_gan_slot(exec_id)
    telemetry.station_event(exec_id, 1.6, "gan_slot", gan_slot=gan_slot)
    try:
        await agents.run_skill(
            skill_name="ocean-qa-agent",
            node="qa_scenarios",
            ticket_id=tid,
            task_prompt=(
                f"Run /ocean-qa-agent {tid} --scenarios-only --no-review, HEADLESS. This run's "
                f"execution_id is {exec_id} -- stamp it into the Step 5d per-round GAN progress sidecar "
                f"(see SKILL.md's sidecar write instruction) so a resumed process can verify it's "
                f"resuming ITS OWN aborted run, not trusting a stale one (I3, MM-14793). No code or PR "
                f"exists yet for this ticket -- design the test scenarios from the ticket's ACs ALONE (Steps "
                f"1-2d, 5, 5b, 5d -- Step 2e is disabled, skip it), GAN-harden them (Step 5d test-case "
                f"GAN), and persist the hardened scenario list + qa_gan_verdict to {scenarios_path}. "
                # D1 Step 2: the ceiling that bounds Step 5d's evidence-gated extension.
                # The skill may go past round 3 ONLY while HIGH is strictly decreasing and
                # both scores clear 90%; a plateau stops regardless. Also ask for the
                # stop_reason, which is what makes "why did it end" visible downstream.
                f"max_gan_rounds={config.MAX_GAN_ROUNDS} (the ceiling for Step 5d's "
                f"evidence-gated extension). Persist `stop_reason` (converged | "
                f"scores_below_threshold | plateau | round_ceiling) as a TOP-LEVEL key too. "
                f"**Phase 1 is AC-ONLY — read NO code and NO automation-testing files** (MM-14132/"
                f"EXE-44302b71): do NOT run Step 3/3b (PR/diff search — none exists yet) AND do NOT run "
                f"Step 4/4a (sibling-test / helper-file reading) — the sibling-test conventions are applied "
                f"later in sit_author when the pytest is actually WRITTEN (`--use-scenarios` runs Step "
                f"3/3b/4/4a for real then). Do NOT write a pytest file (Step 7) here either.\n\n{_summary(state)}\n\n"
                f"Reachability report (for AC/behavior context):\n{_brief(state.get('reachability_report'), limit=8000)}\n\n"
                f"Ocean SME ownership/reuse guidance:\n{_brief(state.get('sme_findings'))}"
            ),
            verdict_path=scenarios_path,  # EXE-2755f777: a transient post-success error must not blindly
                                           # redrive this whole (expensive, multi-step) invocation from scratch
        )
    finally:
        _release_gan_slot(exec_id)
    partial = _load_json(str(scenarios_path))
    gan_verdict = _gan_verdict(partial)
    telemetry.station_event(exec_id, 1.6, "end", qa_gan_verdict=gan_verdict)
    gan_gaps = _gan_gaps(partial)
    if gan_gaps:
        ui.milestone(f"GAN left {len(gan_gaps)} residual HIGH gap(s) — passing them to the coder")
    telemetry.station_event(exec_id, 1.6, "gan_gaps", count=len(gan_gaps))
    return {"qa_scenarios_path": str(scenarios_path),
            "qa_gan_verdict": gan_verdict,
            "qa_gan_residual_gaps": gan_gaps,
            "qa_gan_stop_reason": str(partial.get("stop_reason") or ""),
            "qa_gan_phase0_gaps": partial.get("qa_gan_phase0_gaps", [])}


# ------------------------------------------------------------------ Station 4
async def coder(state: OceanState) -> dict:
    iteration = state.get("review_iteration", 0)
    attempt = state.get("coding_attempts", 0)
    telemetry.station_event(state["execution_id"], 4, "start",
                            review_iteration=iteration, coding_attempt=attempt)

    rework = ""
    rca_findings = state.get("rca_findings", [])
    if rca_findings:
        rework += (f"\nThis change originates from an RCA investigation that concluded a fix is "
                   f"needed. Implement the fix per the RCA brief:\n{json.dumps(rca_findings, indent=2)}\n")
    review_findings = state.get("review_findings", [])
    if review_findings:
        rework += (f"\nAddress these Station 5 review findings from the prior pass:\n"
                   f"{json.dumps(review_findings, indent=2)}\n")
    sit_findings = state.get("sit_findings", [])
    if sit_findings:
        rework += (f"\nAddress these Station 6 SIT code-fault findings (real defects a passing "
                   f"SIT would catch):\n{json.dumps(sit_findings, indent=2)}\n")
    # Station 4.5. THE load-bearing half of the quality gate: without this the gate bounces the run
    # back here and the coder re-runs BLIND, reproduces the same file, and the attempt budget turns a
    # recoverable syntax error into a failed run.
    quality_findings = state.get("quality_gate_findings", [])
    if quality_findings and any(schemas.is_blocking_finding(f) for f in quality_findings):
        rework += (
            f"\nSTATIC QUALITY GATE — BLOCKING. A deterministic, non-LLM check ran plain `ruby -c` / "
            f"`gofmt -l -e` / a Python or YAML parse over the files you changed and REJECTED them. "
            f"These are mechanical facts, not review opinions — do NOT re-litigate them, and do NOT "
            f"redesign or re-implement anything. Fix EXACTLY these files, re-run the same command "
            f"yourself to confirm it now passes, commit, push, and STOP. Entries marked MINOR are "
            f"advisory and do not block:\n{json.dumps(quality_findings, indent=2)}\n")
    # D1 Step 1: the GAN's residual HIGH gaps. NOT a rework block -- these are available on the
    # FIRST coding pass, because qa_scenarios runs pre-code (right after reachability_gate) and the
    # scenarios are, in this file's own words, "the fixed target coder must satisfy". They never
    # reached the coder until now.
    gan_gaps = state.get("qa_gan_residual_gaps") or []
    if gan_gaps:
        rework += (
            f"\nGAN RESIDUAL HIGH GAPS ({len(gan_gaps)}). An adversarial panel hardened this "
            f"ticket's test scenarios BEFORE any code existed and these HIGH-severity gaps survived "
            f"its rounds -- they describe code behaviour the SIT will target. Address them in the "
            f"implementation. Note some may be hypothetical implementation choices rather than "
            f"observed defects (Phase 1 is AC-only by construction): if one does not apply to the "
            f"code you actually wrote, say so explicitly in your verdict notes rather than "
            f"inventing a change to satisfy it:\n{json.dumps(gan_gaps, indent=2)}\n")

    # B1. A coverage bounce without this is a blind re-run. Names the mismatch concretely, because
    # the usual cause is the coder reporting one repo while its branch lives in another.
    if state.get("review_coverage_gap"):
        rework += (
            f"\nMULTI-REPO REVIEW COVERAGE GAP. Your branch exists in "
            f"{state.get('review_branch_repos')}, but the adversarial review only ran in "
            f"{state.get('review_repos_covered')}, so {state.get('review_coverage_gap')} would "
            f"receive a pull request that nothing ever reviewed. Either your reported `repo_dir` is "
            f"not the repo your branch is on, or you changed more repos than you reported. Report "
            f"EVERY repo you pushed to in `repo`, comma-separated, and set `repo_dir` to the tree "
            f"you actually changed.\n")

    # D6. Same load-bearing role as the quality-gate block above: an ENFORCED accuracy failure
    # bounces the run back here, and without the judge's own issues[] the coder re-runs blind and
    # burns the attempt budget reproducing the same diff. Injected only when `eval_gap` is set, which
    # `_eval_gate` populates only under EVAL_ENFORCE — so with the knob off this is never added.
    if state.get("eval_gap"):
        failed = [e for e in (state.get("node_evaluations") or [])
                  if isinstance(e, dict) and schemas.eval_verdict(e.get("verdict")) == "FAIL"]
        rework += (
            f"\nINDEPENDENT ACCURACY EVALUATION — FAILED. A separate judge (a different model, which "
            f"did not write this code) scored your output against the ticket's acceptance criteria "
            f"and the binding reachability report, and found it materially wrong or incomplete. "
            f"{state['eval_gap']}. Address each issue below concretely — do NOT argue with the "
            f"judgment and do NOT redesign beyond what the issues name. If you believe an issue is "
            f"mistaken, say so explicitly in your verdict notes rather than silently ignoring "
            f"it:\n{json.dumps(failed, indent=2)}\n")

    # The graph owns the clone location: the coder clones into a per-run workspace and works
    # there, so the reviewer and any rework pass run against the SAME tree (reuse on re-entry).
    workspace = config.workspace_dir(state["execution_id"])

    # Resume-awareness (manual-findings #16): if a PRIOR attempt was killed mid-run, run_agent left a
    # coder.partial.json breadcrumb with its committed git state. Tell the coder to CONTINUE that branch
    # instead of starting over — so a kill mid-coder no longer discards the committed work.
    partial = config.artifacts_dir(state["execution_id"]) / "coder.partial.json"
    resume_hint = ""
    if partial.exists():
        resume_hint = (
            f"\n\nRESUME — a PRIOR coder attempt was interrupted (killed/timed out) before it finished or "
            f"wrote a verdict. Its captured git state:\n{_brief(_load_json(str(partial)))}\n"
            f"Do NOT start over. `git fetch` + checkout the ticket branch, run `git log --oneline "
            f"<base>..HEAD` to see what that attempt already committed, VERIFY it against the ticket, then "
            f"implement ONLY what is still missing, re-push, and STOP.\n"
        )

    # Build-slot capacity gate: the worker below runs Ruby `bundle install`/rspec IN DOCKER for a
    # Docker-run repo (AGENT_GUARDRAILS) — invisible to a Python-level check unless the slot is held
    # by THIS parent process before the worker is even spawned (the worker's own direct calls to
    # ruby_image_cache.py are then already covered, since it can only run while this hold exists).
    # Skip entirely when prep_container already holds a hold spanning this whole persistent-container
    # phase (container_ready), or when no target repo is Docker/Ruby at all (Go/Java tickets never wait).
    own_build_slot = _any_docker_repo(state.get("target_repos")) and not state.get("container_ready")
    if own_build_slot:
        slot = await _acquire_build_slot(state["execution_id"], state.get("target_repos"))
        telemetry.station_event(state["execution_id"], 4, "build_slot", build_slot=slot)
    try:
        v: schemas.CoderVerdict = await agents.run_agent(
            agent_md="code.md",   # ocean-coding-agent worker (fk-aideveloper single source)
            node="coder",
            ticket_id=state["ticket_id"],
            execution_id=state["execution_id"],
            cwd=workspace,
            task_prompt=(
                f"Decompose and implement {state['ticket_id']} per FK North Star. isbu: commit + PUSH "
                f"the branch and STOP (no PR — the graph opens it).{rework}{resume_hint}\n\n"
                f"WORKSPACE: clone the target repo into {workspace} and do all work there. If the clone "
                f"already exists (a rework pass re-enters here), reuse it — `git fetch` + checkout the "
                f"ticket branch — do NOT re-clone. Report its absolute path as repo_dir.\n\n"
                f"Binding reachability report (build what it says is NOT_YET_BUILT; do not re-litigate "
                # MM-14816 (G20): use the UNTRUNCATED binding slice (verdicts + overrides + advisories +
                # the reachability-VERIFIED resolved_placeholders the coder must USE) — the old
                # `_brief(..., 12000)` truncated binding content on a real run (R2), which would drop the
                # resolved values. Full audit/provenance stays on disk if the coder wants more.
                f"its verdicts; USE any resolved_placeholders values verbatim, do not re-invent them):\n"
                f"{_reachability_for_coder(state.get('reachability_report') or {})}\n\n"
                # MM-14816 (G20): a human answered the ticket's open questions at the Monitor-UI gate —
                # authoritative, use them as given.
                + (f"HUMAN-PROVIDED ANSWERS to the ticket's open questions (authoritative — build to "
                   f"these):\n{state['blocked_answers']}\n\n" if state.get("blocked_answers") else "")
                # reject path: proceed, but these are UNANSWERED — flag them, don't invent around them.
                + (f"UNANSWERED OPEN QUESTIONS (a human chose to proceed without answers): implement "
                   f"best-effort, do NOT invent a consumer-facing value to resolve one (C10/C11), and "
                   f"CALL THESE OUT in the PR body so reviewers see the gaps:\n"
                   f"{_brief(state.get('open_question_caveats'), limit=4000)}\n\n"
                   if state.get("open_question_caveats") else "")
                + f"Ocean SME ownership/reuse guidance:\n{_brief(state.get('sme_findings'))}\n\n"
                f"Research summary:\n{_brief(state.get('research_packet'))}\n\n"
                f"{_summary(state)}"
                f"{_container_directive(state, str(workspace))}"
            ),
            verdict_model=schemas.CoderVerdict,
        )
    finally:
        if own_build_slot:
            _release_build_slot(state["execution_id"])
    telemetry.station_event(state["execution_id"], 4, "end")
    # A fresh code pass supersedes prior SIT findings; clear them once addressed.
    # Persist WHICH repo the coder pushed to + WHERE the clone lives + the PR title/body it
    # proposed, so the reviewer/rework run in the same tree and open_pr opens deterministically
    # (preserve prior values if a rework pass leaves them blank).
    out = {"branch": v.branch,
           "files_changed": v.files_changed, "sit_findings": [],
           "service_repo": v.repo or state.get("service_repo", ""),
           "worktree_dir": v.repo_dir or state.get("worktree_dir", ""),
           "pr_title": v.pr_title or state.get("pr_title", ""),
           "pr_body": v.pr_body or state.get("pr_body", "")}
    # D5: independent accuracy evaluation. node-evaluator.md calls the coder rubric "the
    # highest-value check", and this is the only node wired to it today. No-ops entirely unless
    # NODE_EVAL is on, and routes nothing unless EVAL_ENFORCE is also on. Runs on `{**state, **out}`
    # so the judge sees the tree THIS pass just wrote (`worktree_dir` is set in `out`, and on a
    # first pass the pre-call state has none) -- otherwise the judge would be pointed at the control
    # plane's own checkout and could not read the diff it is scoring.
    out.update(await _eval_node(
        {**state, **out}, "coder",
        job=(f"Implement {state['ticket_id']} per FK North Star: honor the binding reachability "
             f"report, reuse the intended mechanism, commit and push the branch. No PR."),
        output=(f"branch={v.branch} repo={v.repo} repo_dir={v.repo_dir} "
                f"files_changed={v.files_changed}\npr_title={v.pr_title}\npr_body={v.pr_body}"),
    ))
    return out


# ------------------------------------------------------------------ Station 4.5 (plain code, no agent)
async def quality_gate(state: OceanState) -> dict:
    """Deterministic zero-config static checks over the coder's CHANGED files, BEFORE the LLM review.

    Closes the one path in the graph with no mechanical check at all: `coder` -> `harsh_reviewer` (an
    LLM) -> `open_pr`. The junit parse (F1) is downstream at Station 6, so until now nothing ever
    looked at a diff before a draft PR opened. This is a SYNTAX gate, not a quality gate — see
    quality.py for why the obvious "run the repo's linter" design is unavailable (no ocean repo has one).

    WHY IT RUNS BEFORE THE REVIEWER, not after. `harsh_reviewer` OVERWRITES `review_findings`
    (its own return) and `prep_rework` CLEARS it. A gate that wrote findings there before the reviewer
    would be wiped one node later — a textbook inert gate. Running first, with its own state keys, also
    means a syntax error costs seconds instead of a full adversarial review.

    NO BUILD SLOT, deliberately — the omission looks like an oversight next to `coder`/`harsh_reviewer`,
    so: the Ruby path `docker exec`s into an ALREADY-RUNNING container (marginal Docker-VM memory ~0,
    which is what the slot's headroom check bounds), `prep_container` already holds a slot for that
    container's whole lifetime, and `_acquire_build_slot` waits up to BUILD_SLOT_WAIT_SECONDS (1800) —
    a 30-minute queue in front of a 2-second gate. If a future revision ever makes this node START a
    container, it must adopt the canonical acquire/`finally`-release pattern the coder uses.

    Fails OPEN on could-not-run (loudly, and carried to the terminal), CLOSED on an actual finding.
    """
    exec_id = state["execution_id"]
    telemetry.station_event(exec_id, 4.5, "start")
    # Plain-code nodes must print their own header — run_agent/run_skill do it for agent nodes, so
    # without this the monitor's last-seen station label stays on "Coding" for the gate's duration.
    ui.station_start("quality_gate")

    dirs = quality.repo_dirs(state.get("worktree_dir") or "", config.workspace_dir(exec_id))
    container = (state.get("container_name") or "") if (
        config.PERSISTENT_CONTAINER and state.get("container_ready")) else ""

    try:
        findings, unverified, checked, derived, uncovered = await asyncio.to_thread(
            quality.run_all, dirs, container,
            timeout=config.QUALITY_GATE_CMD_TIMEOUT,
            cap=config.QUALITY_GATE_MAX_FILES,
            java_on=config.QUALITY_GATE_JAVA)
    except Exception as e:  # noqa: BLE001
        # The CLASS, not one instance of it. A non-UTF-8 path once raised UnicodeDecodeError straight
        # out through run_all, to_thread and this node, turning a run that would have completed into
        # `[FAILED] UnicodeDecodeError` — failing CLOSED on infrastructure, which this node's own
        # contract forbids. That specific trigger is fixed; this makes the guarantee unconditional,
        # so no future checker can reintroduce it.
        findings, unverified, checked, derived, uncovered = \
            [], f"the gate itself raised {type(e).__name__}: {e}", 0, 0, 0

    # THE anti-inertness invariant, and it is keyed on CHECKED — not on derived. A judge replayed this
    # design against a real completed run (eta-worker/MM-14312) and it came out inert while reading
    # clean: 8 files derived, 0 checked, no findings, route proceed. `derived == 0` never fires there.
    if derived == 0:
        # `derived == 0` is NOT a clean pass — it is "we found nothing to look at", and a judge
        # evaded the invariant through this door three separate ways (a non-ASCII changed filename,
        # a worktree left on the base branch, a single-branch clone whose base_ref resolves to the
        # ticket branch). Every one produced zero milestones, empty `unverified`, and a UI reading
        # "0 file(s) clean". This module's own docstring names an empty changed-file list as the #1
        # inertness hazard, so it cannot also be the one shape that stays silent.
        unverified = (f"derived NO changed files for this run"
                      + (f" — {unverified}" if unverified else ""))
    elif checked == 0:
        unverified = (f"derived {derived} changed file(s) but CHECKED 0 of them"
                      + (f" — {unverified}" if unverified else ""))

    if unverified:
        # LOUD. A gate that quietly disables itself is worse than no gate, because the run still looks
        # gated (the lesson from _check_rca_report's four silent infra-failure modes).
        ui.milestone(f"QUALITY GATE DID NOT FULLY RUN — {unverified}. "
                     f"{checked} of {derived} changed file(s) were actually checked.")
        telemetry.station_event(exec_id, 4.5, "skip", reason=unverified[:300],
                                checked=checked, derived=derived)

    blocking = [f for f in findings if schemas.is_blocking_finding(f)]
    attempts = state.get("quality_gate_attempts", 0) + (1 if blocking else 0)
    # Mirrors after_quality_gate's own "stop" condition. The router is a pure function and cannot
    # write state, so the node computes it here — this boolean is what lets stop_run label the run
    # without keying on `attempts >= MAX`, which stays true for the rest of the run and would
    # mislabel any later, unrelated stop.
    # A CLEAN pass returns the budget. Without this, `quality_gate_attempts` was a global run counter
    # rather than a per-defect one: `harsh_reviewer --rework--> coder` bypasses `prep_rework`, so a
    # brand-new syntax error introduced on a LATER review round inherited the spent budget and went
    # straight to `stop_run` — terminating a healthy run over a defect the coder was never handed
    # even once (judge review). Config calls this a BOUNCE budget; this makes it one.
    attempts = attempts if blocking else 0
    stopped = bool(blocking) and attempts > config.MAX_QUALITY_GATE_ATTEMPTS
    if blocking:
        for f in blocking[:4]:
            ui.milestone(f"Quality gate: {f.get('file', '?')} — {f.get('summary', '?')}")
        if stopped:
            ui.milestone(f"Quality gate budget spent after {attempts} attempt(s) — stopping. The "
                         f"changed files still do not parse; this needs an engineer. "
                         f"(Set OCEAN_PIPELINE_QUALITY_GATE=0 to disable this gate entirely.)")

    telemetry.station_event(exec_id, 4.5, "end", files_checked=checked, files_derived=derived,
                            files_uncovered=uncovered, findings=len(findings),
                            blocking=len(blocking), attempt=attempts)
    # Every key is returned on EVERY pass: nothing else clears `quality_gate_findings` (prep_rework
    # clears review_findings, not this), so a "write only what changed" shape would re-inject stale
    # findings into every later coder prompt.
    return {"quality_gate_findings": findings,
            "quality_gate_unverified": unverified,
            "quality_gate_checked_files": checked,
            "quality_gate_attempts": attempts,
            "quality_gate_stopped": stopped}


# ------------------------------------------------------------------ Station 5
async def harsh_reviewer(state: OceanState) -> dict:
    iteration = state.get("review_iteration", 0)
    telemetry.station_event(state["execution_id"], 5, "start", review_iteration=iteration)
    # Review in the SAME clone the coder pushed from, so `git diff` + independent test
    # re-execution see the real tree (falls back to the default cwd if unset).
    wt = state.get("worktree_dir") or ""
    # Same build-slot gate as coder — skipped when prep_container's hold already covers this phase
    # (container_ready) or the ticket has no Docker/Ruby repo at all.
    own_build_slot = _any_docker_repo(state.get("target_repos")) and not state.get("container_ready")
    if own_build_slot:
        slot = await _acquire_build_slot(state["execution_id"], state.get("target_repos"))
        telemetry.station_event(state["execution_id"], 5, "build_slot", build_slot=slot)
    try:
        v: schemas.ReviewVerdict = await agents.run_agent(
            agent_md="review.md",   # ocean-coding-agent worker (fk-aideveloper single source)
            node="harsh_reviewer",
            ticket_id=state["ticket_id"],
            execution_id=state["execution_id"],
            cwd=Path(wt) if wt else None,
            task_prompt=(
                f"Adversarially review the pushed committed diff on branch {state.get('branch')} for "
                f"{state['ticket_id']} (review round {iteration + 1}) in the clone at "
                f"{wt or '(the current directory)'}. Get the diff with `git diff <base>...HEAD`. "
                f"Classify every finding CRITICAL/MAJOR/MINOR. APPROVE only at zero CRITICAL and zero "
                f"MAJOR.\n\n"
                f"Research summary (for AC context):\n{_brief(state.get('research_packet'))}\n\n"
                f"{_summary(state)}"
                f"{_container_directive(state, wt)}"
            ),
            verdict_model=schemas.ReviewVerdict,
            model=config.JUDGE_MODEL,   # F7: judge on a different model than the coder (self-preference bias)
        )
    finally:
        if own_build_slot:
            _release_build_slot(state["execution_id"])
    telemetry.station_event(state["execution_id"], 5, "end", verdict=v.verdict)
    # B1: multi-repo review coverage. `harsh_reviewer` has ONE cwd, so `covered` is ALWAYS a
    # singleton -- do NOT pass repo_dirs() here or the gate over-claims coverage and never fires.
    wt = state.get("worktree_dir") or ""
    reviewed_dirs = [Path(wt)] if wt else [config.FK_AIDEVELOPER_DIR]
    branch_repos, covered, gap, unverified = _review_coverage(state, reviewed_dirs)
    attempts, stopped = _coverage_budget(state, gap)
    if unverified:
        ui.milestone(f"REVIEW COVERAGE NOT DERIVED — {unverified}. Proceeding (fails OPEN).")
        telemetry.station_event(state["execution_id"], 5, "skip", reason=unverified[:300])
    elif gap:
        ui.milestone(f"REVIEW COVERAGE GAP — reviewed {covered}, but {gap} carry this branch "
                     f"and would receive a PR unreviewed.")
        telemetry.station_event(state["execution_id"], 5, "coverage_gap", gap=",".join(gap),
                                covered=",".join(covered), attempt=attempts)
    # ALL six keys on EVERY pass (quality_gate's discipline): a "write only what changed" shape
    # re-injects a stale gap into every later router decision and coder prompt.
    return {"review_verdict": v.verdict, "review_findings": v.findings,
            "review_iteration": iteration + 1,
            "review_branch_repos": branch_repos, "review_repos_covered": covered,
            "review_coverage_gap": gap, "review_coverage_unverified": unverified,
            "review_coverage_attempts": attempts, "review_coverage_stopped": stopped}


# ------------------------------------------------------------------ 3.87 open PR (plain code, idempotent)
async def open_pr(state: OceanState) -> dict:
    """Open the DRAFT service PR(s) — deterministic gh, run by the graph, NOT an agent. MULTI-REPO
    aware: the coder's single branch may span 1..N changed repos, so open one draft PR PER repo.
    Idempotent per repo: reuses an existing PR for the branch (the code_fault loop re-enters here)."""
    slugs, branch = _service_slugs(state), state.get("branch")
    if not slugs or not branch:
        raise gitops.GitOpError(
            f"cannot open PR: no repo slugs ({slugs!r}) or branch ({branch!r}) — the coder "
            f"must report `repo`/`target_repos` and `branch`."
        )
    pr_numbers = dict(state.get("pr_numbers") or {})
    # Back-compat: a pre-fix single-repo rework re-enters with only pr_number set.
    if not pr_numbers and state.get("pr_number") and len(slugs) == 1:
        pr_numbers[slugs[0]] = state["pr_number"]
    if all(s in pr_numbers for s in slugs):
        return {}  # every repo's PR already open (rework/resume re-enters here)
    telemetry.station_event(state["execution_id"], 3.87, "start")
    title = state.get("pr_title") or f"{state['ticket_id']}: automated pipeline change"
    body_text = state.get("pr_body") or f"Automated change for {state['ticket_id']} (FK Ocean pipeline)."
    # FourKites org policy: every PR body must start with `Ticket: <TICKET-ID>` on its own line.
    # Enforced here (not left to the coder's free-text pr_body) so it's guaranteed regardless of
    # what the coder proposed or whether the fallback text above fired.
    ticket_line = f"Ticket: {state['ticket_id']}"
    body = body_text if body_text.startswith(ticket_line) else f"{ticket_line}\n\n{body_text}"
    # D1 Step 3 (bullet 1): surface the GAN's residual HIGH gaps to the human who reviews this PR.
    # GATED ON THE GAPS BEING NON-EMPTY, never on `qa_gan_verdict` -- 4 of 5 real artifacts are
    # REJECT and the verdict vocabulary itself drifts (`APPROVE_WITH_FIXES` vs `APPROVE WITH
    # FIXES`), so gating on the verdict would put this section on nearly every PR and train
    # reviewers to skip it. APPENDS ONLY, after the Ticket: line logic: with no gaps the body is
    # byte-identical to what it was before this existed.
    gan_gaps = state.get("qa_gan_residual_gaps") or []
    if gan_gaps:
        lines = "\n".join(f"- {g.get('summary', '')}" for g in gan_gaps if isinstance(g, dict))
        body += (f"\n\n---\n**Residual test-coverage gaps ({len(gan_gaps)})** — an adversarial "
                 f"panel hardened this ticket's test scenarios before the code was written and "
                 f"these HIGH-severity gaps survived its rounds. They were given to the coder; "
                 f"please confirm they are addressed or consciously accepted:\n{lines}\n")
    for slug in slugs:                       # one draft PR per changed repo (open_draft_pr is idempotent)
        if slug not in pr_numbers:
            pr_numbers[slug] = gitops.open_draft_pr(slug, branch, title, body)
    primary = pr_numbers[slugs[0]]
    telemetry.station_event(state["execution_id"], 3.87, "end",
                            pr_number=primary, pr_numbers=pr_numbers)
    return {"pr_number": primary, "pr_numbers": pr_numbers}


# ============================ Station 6 — local SIT, decomposed into graph nodes ============================
# LangGraph owns the Station-6 sequence: sit_resolve -> sit_run -> sit_triage, driving the
# ocean-automation-testing skill one `--only <phase>` at a time (state flows through the skill's own
# memory/tickets/<TICKET>-automation-testing.json). The graph branches at the two real decision points:
# after resolve (onboard an unsupported repo) and after triage (pass / code_fault / could_not_verify).
#
# NOTE on numbering vs. execution order (MM-14738): these "Station 6.x" labels are historical, not
# execution order. `sit_author` (6.1) no longer designs scenarios from scratch here — `qa_scenarios`
# (Station 1.6, defined above with `coder`) already GAN-hardened them pre-code, right after
# reachability_gate; `sit_author` runs post-review as before and just writes the pytest from that
# artifact. Actual execution order: reachability_gate -> qa_scenarios -> coder -> harsh_reviewer ->
# open_pr -> sit_resolve -> sit_author -> qa_review_gate -> sit_run[+testrail] -> sit_triage.

# ------------------------------------------------------------------ Station 6a — resolve (+ gate)
async def sit_resolve(state: OceanState) -> dict:
    """Resolve the narrowest changed repo + existing SIT (skill Station 0, `--only resolve`). Because
    the control plane OWNS onboarding, this reports-only on an unsupported repo (needs_onboarding) so
    the graph can branch to learn_repo BEFORE any authoring/execution. Clears the prior verdict first."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.0, "start", coding_attempt=state.get("coding_attempts", 0))
    verdict_path = config.automation_verdict_path(tid)
    verdict_path.parent.mkdir(parents=True, exist_ok=True)
    if verdict_path.exists():
        verdict_path.unlink()  # fresh Station-6 attempt (drop a prior loop's verdict)
    # This node is shared by the main graph (which always knows pr_number by now — open_pr already
    # ran) and qa_batch's subgraph (which enters HERE with no coder/open_pr step, so pr_number is
    # never set). Embedding "Service PR #None" in the prompt was misleading in the qa_batch case;
    # phrase truthfully for each instead of asserting a number that doesn't exist.
    pr_context = (
        f"Service PR #{state['pr_number']}; Station 5 APPROVED so Station 0.5's gate auto-passes. "
        if state.get("pr_number") else
        "No PR number given — resolve the existing service PR for this ticket yourself "
        "(qa-batch mode: there is no pending review, so Station 0.5's gate is not relevant here). "
    )
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_resolve",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 0 (resolve) ONLY for {tid} (`--only resolve`), "
            f"HEADLESS. {pr_context}"
            f"Resolve the NARROWEST changed ocean repo set, the existing SIT (reuse-aware), "
            f"the domain bucket, and pr_number; persist them to {verdict_path}.\n"
            f"CONTROL-PLANE ONBOARDING (MM-14621): the Aquaman control plane OWNS repo onboarding. If the "
            f"changed repo is an ocean/isbu service NOT in your supported local set, do NOT self-clone, "
            f"profile, or commit — set needs_onboarding=true + onboard_repo=<repo> in {verdict_path} and "
            f"STOP. Do NOT author or run the SIT here.\n\n{_summary(state)}"
        ),
        verdict_path=verdict_path,  # EXE-2755f777: unlinked fresh above, so its existence here is a
                                     # trustworthy signal that THIS attempt (not a stale prior one) succeeded
    )
    partial = _load_json(str(verdict_path))
    needs = bool(partial.get("needs_onboarding"))
    telemetry.station_event(exec_id, 6.0, "end", needs_onboarding=needs)
    return {"needs_onboarding": needs, "onboard_repo": partial.get("onboard_repo", "")}


# ------------------------------------------------------------------ Station 6b — author (draft + STOP)
async def sit_author(state: OceanState) -> dict:
    """Write the SIT pytest (skill Station 1) from the GAN-hardened scenarios `qa_scenarios` already
    produced pre-code, and STOP — no TestRail cases, no run. A human reviews the draft at
    qa_review_gate before anything executes or gets committed.

    On the FIRST pass (no reviewer note yet): pass `--use-scenarios` through to ocean-qa-agent so it
    finds the real diff/helpers (Step 3/3b/4/4a) and writes the pytest from the pre-hardened scenarios
    — it does NOT redesign them. On a 'changes' loop-back: the reviewer already looked at the concrete
    draft and their feedback supersedes the earlier automated hardening, so this pass runs a full
    design+write instead (no `--use-scenarios`) — same as before MM-14738 — letting the note reshape
    the scenarios themselves, not just the pytest mechanics."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    it = state.get("qa_review_iteration", 0)
    telemetry.station_event(exec_id, 6.1, "start", qa_review_iteration=it)
    note = state.get("qa_note") or ""
    scenarios_path = state.get("qa_scenarios_path") or ""
    if note:
        # Revise pass: human feedback on the concrete draft outranks the pre-hardened scenarios --
        # redesign + rewrite in one shot, exactly as ocean-qa-agent did before this ticket.
        author_directive = (
            f"Draft the invariant-compliant SIT scenarios + the pytest file via ocean-qa-agent with "
            f"`--no-review --skip-testrail`: AUTHOR/UPDATE the test file (reuse only if it still "
            f"covers the current diff) and record the scenarios + the test path, then STOP.\n"
            f"The reviewer requested CHANGES to the prior draft — revise the scenarios/test to "
            f"address this feedback:\n{note}\n"
        )
    else:
        # First pass: the scenarios are already GAN-hardened (qa_scenarios ran pre-code) -- do not
        # redesign them, just find the real diff and write the pytest from them.
        author_directive = (
            f"Write the pytest file via ocean-qa-agent with `--use-scenarios {scenarios_path} "
            f"--no-review --skip-testrail`: it will find the real diff/helpers (Step 3/3b/4/4a) and "
            f"write the file from the ALREADY GAN-hardened scenarios at that path -- do NOT let it "
            f"redesign the scenarios. Record the test path, then STOP.\n"
        )
    # qat-handoff Phase 1.5: invalidate the PREVIOUS pass's 9b verdict before re-authoring.
    # A 9b verdict grades the test AGAINST THE PRODUCT CODE, and on the code_fault rework loop
    # (sit_triage -> prep_rework -> coder -> ... -> sit_author) the product code has changed, so the
    # old verdict is stale BY CONSTRUCTION. Fail-closed covers "missing"; it does not cover "stale",
    # and a re-authoring pass that fails to write a new one would otherwise leave a later reader
    # consuming the previous attempt's verdict. Same pattern sit_resolve already uses for the
    # automation verdict (EXE-2755f777), and for the same reason: existence must mean THIS pass.
    review_json = config.FK_AIDEVELOPER_DIR / "memory" / "tickets" / f"{tid}-qa-authoring-review.json"
    try:
        review_json.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass   # best-effort; an un-deletable stale file is caught by the corroboration below
    # Phase 1.2: byte offset BEFORE the skill runs, so the judge scan covers this invocation only.
    _9b_offset = _log_offset(exec_id, "sit_author")

    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_author",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 1 (author) ONLY for {tid} (`--only author`), HEADLESS. "
            f"Station 0 (resolve) already ran — do NOT re-resolve. {author_directive}"
            f"Do NOT create TestRail cases and do NOT execute the SIT. "
            # qat-handoff Phase 1.1. This used to end '— a human reviews this draft next', and the
            # skill took it LITERALLY: two of three real artifacts record the Step-9b judge panel
            # as deliberately deferred to that human ("No fresh-agent judge panel spawned").
            # Nothing suppressed it technically -- no tools restriction, no deny hook, no
            # max_turns -- it was this sentence. Note the fix is NECESSARY, NOT SUFFICIENT: the
            # graph really does edge sit_author -> qa_review_gate, so an agent inferring "a human
            # reviews next" is reasoning correctly about the world. The weight is carried by the
            # disk corroboration below, which enforces at READ time.
            f"Step 9b (the fresh adversarial judge panel) is MANDATORY on this pass and must NOT "
            f"be deferred to the human review gate: the gate reviews your OUTPUT, it does not "
            f"perform 9b. Dispatch each 9b judge with STEP9B-JUDGE in its description.\n\n"
            f"{_summary(state)}"
        ),
    )
    partial = _load_json(str(config.automation_verdict_path(tid)))
    # Phase 1.2: corroborate the 9b judge panel from DISK, never from the field the same agent wrote.
    judge_rounds_raw = _judge_rounds_observed(exec_id, "sit_author", _9b_offset)
    review = _load_json(str(review_json))
    claimed = bool(review.get("judge_spawned")) if isinstance(review, dict) else False
    # Unclamped, and > the documented ceiling FAILS CLOSED rather than presenting as a healthy 2:
    # a count above the ceiling means the sentinel is colliding with something else, and a collision
    # is not evidence the judge ran.
    judge_rounds = _corroborated_judge_rounds(judge_rounds_raw)
    if claimed and judge_rounds == 0:
        ui.milestone("Step 9b claims judge_spawned=true but the station log shows NO matching "
                     "dispatch this pass — treating it as NOT spawned.")
    telemetry.station_event(exec_id, 6.1, "9b_corroboration", claimed=claimed,
                            observed_raw=judge_rounds_raw, observed=judge_rounds)
    telemetry.station_event(exec_id, 6.1, "end")
    # Finding 2e: accept every key real verdicts have used for this (a judge review found `test_path`
    # absent or differently-named in most of 12 real runs) -- `sit_test_path` is the third observed one.
    authored_path = (partial.get("test_path") or partial.get("existing_test_path")
                     or partial.get("sit_test_path") or partial.get("target_test_path") or "")
    # Fingerprint the AUTHORED test now, so sit_triage can tell DETERMINISTICALLY whether sit_run
    # rewrote it mid-station (the test_fault -> fix -> re-run path). Without this the ready-flip gate
    # depends entirely on the agent volunteering `test_edited`, and the automatic latch cannot fire on
    # a PASS (failure_class is "" on a pass by contract, so there is no `test_fault` to latch from).
    authored_sha = _file_sha(authored_path)
    # Phase 2.1: the census, computed from the FILE -- never from the review JSON, whose counts AND
    # whose `test_file` path are both agent-written (pointing it at a cleaner file would pass the
    # floor on something that is not the authored test).
    try:
        census = quality.sit_method_census(Path(authored_path).read_text(errors="replace")) \
            if authored_path else {"parsed": False, "total": 0, "skip_guarded": 0}
    except OSError:
        census = {"parsed": False, "total": 0, "skip_guarded": 0}
    genuine, genuine_why = _genuine_passed(review, judge_rounds, census)
    if not genuine and str(review.get("qa_authoring_review_verdict") or "").upper() == "PASSED":
        ui.milestone(f"Step 9b says PASSED but it is NOT a genuine pass — {genuine_why}")
    telemetry.station_event(exec_id, 6.1, "9b_genuine", genuine=genuine, why=genuine_why[:200],
                            methods=census.get("total"), skip_guarded=census.get("skip_guarded"))
    if authored_sha:
        _snapshot_authored_test(exec_id, authored_path)   # content, so a later diff is real
    if not authored_sha:
        # NEVER fail silently here: an empty hash disables the 2e latch entirely, which is exactly how
        # this mechanism was found inert the first time. Say so, loudly, in the run log and telemetry.
        ui.milestone(f"Finding 2e: could not fingerprint the authored SIT file "
                     f"({authored_path or 'no test path in the verdict'}) — the deterministic "
                     f"test-edit check is DISABLED for this run; it falls back to the skill's own "
                     f"`test_edited` report")
        telemetry.station_event(exec_id, 6.1, "test_sha_unavailable", raw_path=(authored_path or "")[:120])
    return {"qa_test_path": authored_path,
            "qa_test_sha": authored_sha,
            "qa_judge_rounds_observed": judge_rounds,
            "qa_genuine_passed": genuine,
            "qa_genuine_passed_why": genuine_why,
            "qa_method_census": census,
            "qa_review_iteration": it + 1, "qa_note": ""}


# ------------------------------------------------------------------ Station 6b.5 — QA review gate (human 3-way)
async def qa_review_gate(state: OceanState) -> dict:
    """Human review of the drafted SIT — the same 3-way choice ocean-qa-agent offers interactively
    (approve-with-TestRail / approve-without-TestRail / changes), surfaced at the graph level so it
    works headless. Default ON (interrupt + wait). QA_REVIEW_AUTO skips the pause and auto-approves
    (with TestRail only if QA_TESTRAIL is set).

    MM-14738: this is also where qa_scenarios' GAN verdict + Phase-0 spec gaps finally surface --
    qa_scenarios ran headless (no human watching) and sit_author never re-inspects them, so if either
    is dropped here they're dropped for good. Both the interrupt payload (human path) and the auto
    telemetry (headless path, so it's at least in the run log) carry them. (Phase 0 / Step 2e is
    currently DISABLED in ocean-qa-agent/SKILL.md -- qa_gan_phase0_gaps is always [] until it's
    re-enabled there; this plumbing is left in place so re-enabling needs no code change here.)"""
    exec_id = state["execution_id"]
    gan_verdict = state.get("qa_gan_verdict", "")
    phase0_gaps = state.get("qa_gan_phase0_gaps", [])
    if config.QA_REVIEW_AUTO:
        decision = "approve_testrail" if config.QA_TESTRAIL else "approve_no_testrail"
        telemetry.station_event(exec_id, 6.15, "auto", qa_decision=decision,
                                qa_gan_verdict=gan_verdict, qa_gan_phase0_gap_count=len(phase0_gaps))
        return {"qa_decision": decision, "qa_note": ""}
    from langgraph.types import interrupt
    raw = interrupt({
        "action": "qa_review",
        "ticket_id": state["ticket_id"],
        "test_path": state.get("qa_test_path"),
        "qa_gan_verdict": gan_verdict,      # APPROVE | APPROVE WITH FIXES | REJECT (Step 5d)
        "qa_gan_phase0_gaps": phase0_gaps,  # HIGH spec gaps from Step 2e -- always [] while Step 2e
                                            # is disabled in ocean-qa-agent/SKILL.md (MM-14738)
        # The REMAINING budget, stated. Without it the human cannot tell that "request changes" is
        # about to be ignored: at 0 remaining, after_qa_review (graph.py:189) silently falls through
        # to `return "sit_run"` and runs the draft anyway. Asking for a decision while concealing
        # that the decision may not be honoured is the part that made this dangerous.
        "changes_remaining": max(0, config.MAX_QA_REVIEW_ITERATIONS - state.get("qa_review_iteration", 0)),
        "prompt": ("Review the drafted SIT scenarios + sample test, then resume with ONE of: "
                   "`--qa approve-testrail` | `--qa approve-no-testrail` | "
                   "`--qa changes --note '<feedback>'`."
                   + ("" if config.MAX_QA_REVIEW_ITERATIONS - state.get("qa_review_iteration", 0) > 0
                      else "  !! The `changes` budget is EXHAUSTED: requesting changes will NOT "
                           "re-author the test — the run proceeds to sit_run with the current draft. "
                           "Stop the run instead if that is not what you want.")),
    })
    decision = raw.get("decision") if isinstance(raw, dict) else str(raw)
    note = raw.get("note", "") if isinstance(raw, dict) else ""
    telemetry.station_event(exec_id, 6.15, "decision", qa_decision=decision)
    return {"qa_decision": decision, "qa_note": note}


def _sit_junit_path(exec_id: str):
    """The run-scoped junit BOTH sit_run (writer) and sit_triage (reader) agree on: an ABSOLUTE path in
    the run's own artifacts dir. The old `reports/junit_${EXEC}.xml` was CHECKOUT-RELATIVE — the filename
    matched but the directory did not (sit_run ran in its clone e.g. /tmp/ta-<tkt>, sit_triage in another
    checkout), so triage found no junit and the skill SILENTLY fell back to a PRIOR run's junit → a false
    could_not_verify on a genuinely PASSING run (EXE-f749212a: authored+ran auto_populate 6/6, triage
    scored the stale vessel_inference file + EXE-6fca4a71's 0/6). An absolute artifacts-dir path removes
    the ambiguity and survives the clone teardown."""
    return config.artifacts_dir(exec_id) / f"junit_{exec_id}.xml"


def _recover_sit_junit(exec_id: str, junit_path) -> None:
    """Judge-A hardening: if the sit_run agent wrote only the skill's own `reports/junit_<exec>.xml`
    (relative) instead of honoring the absolute `--junitxml`, recover it DETERMINISTICALLY from the
    KNOWN exec-scoped copies the skill produces — its durable `memory/tickets` preserve and the SIT
    checkout's `reports/`. The filenames are EXEC-SCOPED, so this can NEVER pick up a prior run's junit
    (the whole point of the run-scoped name) — it only closes the "agent used the relative path" gap so a
    genuinely-passing run isn't turned into a loud (but still wrong) could_not_verify (EXE-f749212a).

    EXCLUDES `*junit_batch_*` (S4/I12, run-monitoring-findings-06af6088.md): those are the PER-FILE
    partial junits ocean-automation-testing's Step 6 batching recipe writes before merging them all
    into the real run junit via `merge_junit.py`. Without this exclusion, this glob's `*{exec_id}*.xml`
    pattern also matches a single batch file — if the agent's own merge step were ever skipped, this
    recovery would silently copy in JUST ONE batch (sorted first, e.g. `junit_batch_1_...`) as if it
    were the complete run, truncating evidence exactly the way the batching fix exists to prevent, via
    a different code path. A batch file left behind with no merged output present is a real merge-step
    failure -- surface that as missing evidence, don't paper over it with a partial substitute."""
    if junit_path.exists() and junit_path.stat().st_size > 0:
        return
    pat = f"*{exec_id}*.xml"
    candidates = []
    try:
        candidates += sorted((Path(str(config.FK_AIDEVELOPER_DIR)) / "memory" / "tickets").glob(pat))
    except OSError:
        pass
    try:  # SIT clones live at /tmp/ta-*/system_integration_test/reports/
        candidates += sorted(Path("/tmp").glob(f"ta-*/system_integration_test/reports/{pat}"))
    except OSError:
        pass
    try:
        candidates += sorted((Path.home() / "Documents/projects/test-automation/system_integration_test/reports").glob(pat))
    except OSError:
        pass
    for c in candidates:
        if "junit_batch_" in c.name:
            continue
        try:
            if c.is_file() and c.stat().st_size > 0:
                shutil.copyfile(str(c), str(junit_path))
                return
        except OSError:
            continue


def _resolve_test_file(raw: str) -> str:
    """Turn whatever the SIT skill recorded as its test path into a real absolute path, or "".

    Finding 2e (second judge round): the deterministic test-edit latch was inert on 0/12 REAL verdicts
    because it assumed `test_path` was absolute. Observed shapes across those verdicts:
      * a repo-qualified LABEL — "cloudqwest/test-automation :: system_integration_test/test_cases/..."
      * repo-relative        — "system_integration_test/test_cases/services/v1/ocean/..."
      * checkout-relative    — "test_cases/services/v1/ocean/..."
      * absent, or under a differently-named key (handled by the caller)
    Rather than demand the skill change shape (which wouldn't fix already-running variants), resolve
    all of them here. Tries, in order: the path as given; the part after a "::" label; the same
    candidates under the local test-automation checkout, with and without its `system_integration_test`
    prefix. Returns "" only when nothing on disk matches — which the caller reports loudly instead of
    silently disabling the check."""
    cand = (raw or "").strip()
    if not cand:
        return ""
    # Strip a repo qualifier, in either observed punctuation: "owner/repo :: path" or "owner/repo:path".
    # The single-colon form appears on non-test-automation repos (e.g. a Java test in eta-worker), so
    # remember which repo it named and search THAT checkout too, not just test-automation.
    repo_hint = ""
    if "::" in cand:
        left, cand = cand.split("::", 1)
        repo_hint, cand = left.strip(), cand.strip()
    elif ":" in cand and not cand.startswith("/") and not re.match(r"^[A-Za-z]:[\\/]", cand):
        left, right = cand.split(":", 1)
        if "/" in left:                    # looks like owner/repo, not a stray colon in a filename
            repo_hint, cand = left.strip(), right.strip()
    roots = []
    if repo_hint:
        roots.append(config.PROJECTS_ROOT / repo_hint.split("/")[-1])
    roots.append(config.PROJECTS_ROOT / "test-automation")
    tries = [Path(cand)]
    for r in roots:
        tries += [r / cand, r / "system_integration_test" / cand]
        # A path may already carry the system_integration_test/ prefix; also try it stripped.
        if cand.startswith("system_integration_test/"):
            tries.append(r / cand[len("system_integration_test/"):])
    for p in tries:
        try:
            if p.is_file():
                return str(p.resolve())
        except OSError:
            continue
    return ""


def _test_snapshot_path(exec_id: str) -> Path:
    return config.artifacts_dir(exec_id) / "authored_test_snapshot.txt"


def _snapshot_authored_test(exec_id: str, raw_path: str) -> bool:
    """Finding 2e: save the authored test's CONTENT (not just its hash) so that when the deterministic
    latch fires, `_diff_against_snapshot` can hand the human an actual diff. A judge review found the
    hash alone left the reviewer acknowledging an edit they could not see. Best-effort; returns whether
    a snapshot was written."""
    resolved = _resolve_test_file(raw_path)
    if not resolved:
        return False
    try:
        shutil.copyfile(resolved, _test_snapshot_path(exec_id))
        return True
    except OSError:
        return False


def _diff_against_snapshot(exec_id: str, raw_path: str, max_chars: int = 4000) -> str:
    """Unified diff of the authored snapshot vs the test file as it stands now, or "" if either side is
    unavailable. Used only when a test edit was detected, to give the human the actual change."""
    resolved = _resolve_test_file(raw_path)
    snap = _test_snapshot_path(exec_id)
    if not resolved or not snap.exists():
        return ""
    try:
        import difflib
        before = snap.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        after = Path(resolved).read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        d = "".join(difflib.unified_diff(before, after, fromfile="as-authored", tofile="at-triage"))
        return d[:max_chars]
    except OSError:
        return ""


def _file_sha(path: str) -> str:
    """Short content fingerprint of a file, or "" if unreadable/absent. Used by Finding 2e to detect a
    mid-run test rewrite deterministically instead of trusting a self-reported flag. Accepts any of the
    path shapes _resolve_test_file handles."""
    try:
        import hashlib
        resolved = _resolve_test_file(path)
        return hashlib.sha256(Path(resolved).read_bytes()).hexdigest()[:16] if resolved else ""
    except OSError:
        return ""


def _read_unmocked_paths(exec_id: str) -> list[str]:
    """Finding 2a: read the mock's OWN deterministic audit of every catch-all (unmocked) path it
    served, written by ocean_mock_helper.py's `_record_unmocked` to
    <artifacts>/<exec_id>/unmocked_paths.json (its default location, derived from the same
    OCEAN_PIPELINE_ARTIFACTS/OCEAN_PIPELINE_EXEC_ID the pipeline exports).

    Why this exists rather than trusting the verdict's own `unmocked_paths_hit`: a judge review found
    the first cut of this fix asked the SIT agent to grep the mock's stdout for a marker and
    self-report the result -- so an agent that simply skipped the grep produced an empty list, the
    rung cap never fired, and a run that silently degraded through the catch-all was indistinguishable
    from a clean one. Reading the file here makes the signal the control plane's own, not a
    self-report. Best-effort: a missing/corrupt file returns [] (the mock may predate this feature or
    have been launched without the audit path), which simply falls back to the verdict's own field."""
    try:
        p = config.artifacts_dir(exec_id) / "unmocked_paths.json"
        if not p.exists():
            return []
        data = json.loads(p.read_text(encoding="utf-8"))
        return [str(x) for x in data] if isinstance(data, list) else []
    except Exception:  # noqa: BLE001 — an audit-file hiccup must never break triage
        return []


def _sut_write_activity(exec_id: str) -> tuple:
    """Finding 2 (②-a): the mock's own count of provenance-tagged writes, as (sut, setup, available).

    `_read_unmocked_paths` above is ABSENCE-BLIND, and a review made that concrete: a SUT that errors
    before ever touching the receiving service makes no mock call at all, so the unmocked audit stays
    empty and its cap is structurally incapable of firing. The test then reads back the value its own
    setup seeded, passes honestly, and a self-reported `fidelity_rung: 2` + `ran_on: "local"` clears
    `_real_service_gap` on the agent's word alone and flips the PR ready.

    A COUNT of SUT-origin writes is the missing presence signal for exactly that case: in
    local-mock-first mode the SUT's outbound writes land in the mock, so zero of them means the SUT
    produced no observable effect this run. `available` is False when the mock wrote no audit (older
    mock, or launched without the path) — that is "we could not check", which must stay distinct from
    "we checked and the SUT was idle"."""
    try:
        p = config.artifacts_dir(exec_id) / "sut_activity.json"
        if not p.exists():
            return 0, 0, False
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return 0, 0, False
        # `unknown` counts mutating requests that carried no ?origin= marker — most mock routes don't
        # take one. They are folded into the SUT side ON PURPOSE: an untagged write may or may not be
        # the SUT's, and the only conclusion drawn from this number is the negative one ("nothing but
        # setup happened"), so an ambiguous write must PREVENT that conclusion, never support it.
        sut = int(data.get("sut") or 0) + int(data.get("unknown") or 0)
        return sut, int(data.get("setup") or 0), True
    except Exception:  # noqa: BLE001 — an audit-file hiccup must never break triage
        return 0, 0, False


def _junit_pass_fail(junit_path) -> tuple:
    """F1 (Aquaman architecture review): the SIT pass/fail is MACHINE-READABLE (junit XML) — parse it
    DETERMINISTICALLY here instead of trusting the LLM triage to read it as prose. Returns
    (result, detail) where result is 'passed' | 'failed' | 'could_not_verify' — NEVER None.
    'passed' iff the run is verifiably COMPLETE, at least one test actually EXECUTED (tests minus
    skipped > 0), and there are zero failures and zero errors.

    It used to return None on any XML it couldn't aggregate, and the caller then fell back to the
    LLM's self-reported `automation_result` — which defeated the entire point of F1. Reproduced in
    review against the real sit_triage, LLM self-reporting "passed":
        malformed (truncated) XML  -> 'passed', routed to flip_ready
        well-formed, no <testsuite> -> 'passed', routed to flip_ready
        junk text "ALL TESTS PASSED" -> 'passed', routed to flip_ready
    ...and silently: no override telemetry, no milestone. The EVIDENCE GUARD upstream only checks the
    file is non-empty, never that it parses, so unreadable evidence WAS treated as good evidence.
    Unreadable evidence is now `could_not_verify`, which is what "we did not verify this" means.

    COMPLETENESS: a junit merged from per-file batches can be missing whole batches — a batch killed
    by its wrapper timeout writes no file at all, so the merge silently presents the survivors' totals
    as the run's totals and a killed test file grades PASSED (the green-by-omission hole; reproduced
    end-to-end). merge_junit.py now stamps `completeness` on the root, and anything other than
    "complete" is could_not_verify here — absence of evidence is not evidence of a pass.

    Aggregates across both the <testsuites> wrapper and bare <testsuite> shapes."""
    import xml.etree.ElementTree as ET
    try:
        root = ET.parse(str(junit_path)).getroot()
    except Exception as e:  # noqa: BLE001 — never crash triage; unreadable evidence is not a pass
        return "could_not_verify", f"junit unparseable: {type(e).__name__}: {e}"[:160]
    suites = [root] if root.tag == "testsuite" else root.findall(".//testsuite")
    if not suites:
        return "could_not_verify", "no <testsuite> element found"

    # Completeness, before anything is counted. Absent attribute on a NON-merged junit is the normal
    # single-pytest shape and carries no completeness claim to check. Absent on a merged one means it
    # came from a merge_junit predating this marker — treat that as unverified rather than complete.
    completeness = root.get("completeness") or ""
    if not completeness and any((s.get("name") or "") == "merged" for s in suites):
        completeness = "unverified"
    if completeness and completeness != "complete":
        got, exp = root.get("merged_batches") or "?", root.get("expected_batches") or "?"
        return "could_not_verify", (
            f"junit completeness={completeness} ({got} of {exp} batches merged) — batches that were "
            f"killed before writing a junit are ABSENT, not failing, so the totals here describe only "
            f"the survivors")

    tests = failures = errors = skipped = 0
    for s in suites:
        def _i(attr):
            try:
                return int(s.get(attr, 0) or 0)
            except (TypeError, ValueError):
                return 0
        tests += _i("tests"); failures += _i("failures"); errors += _i("errors"); skipped += _i("skipped")
    # P2 hardening (judge follow-up): some junit writers populate ONLY testcase-level <failure>/<error>
    # (and testcases) with a suite-level failures="0" — trusting the attrs alone would read that as a
    # clean PASS. Cross-check the element-level counts and take the worst case, so a testcase-only
    # failure can never flip a real failure to passed. (Double-counting can only inflate a non-zero,
    # never mask one, so max() is safe for the boolean.)
    tc_total = len(root.findall(".//testcase"))
    tc_bad = len(root.findall(".//testcase/failure")) + len(root.findall(".//testcase/error"))
    tc_skipped = len(root.findall(".//testcase/skipped"))
    tests = max(tests, tc_total)
    skipped = max(skipped, tc_skipped)
    bad = max(failures + errors, tc_bad)
    executed = tests - skipped
    detail = f"tests={tests} failures={failures} errors={errors} skipped={skipped} tc_bad={tc_bad}"
    if executed <= 0:
        return "failed", f"nothing executed ({detail})"   # 0 tests, or all skipped, is never a PASS
    return ("passed" if bad == 0 else "failed"), detail


# ------------------------------------------------------------------ Station 6c — execute the approved SIT
async def sit_run(state: OceanState) -> dict:
    """Execute the approved SIT local + mock-first (skill Station 2). Authoring + human review already
    happened; this only runs the changed repo locally, mocks the rest, and captures per-test results."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.2, "start")

    # Resource pre-flight (ocean-qa-agent-ac-driven-plan.md Workstream 3.4): fail fast, deterministically,
    # BEFORE spending an entire agent invocation on a Docker bring-up that's going to OOM (MM-13437's
    # full-chain attempt ground for ~40 min before hitting this exact documented ceiling).
    # Exclude THIS run's own containers (named with the exec_id — warm SIT stack + persistent container)
    # from the over-commit check, so a WARM_SIT_INFRA retry isn't blocked by its own reused stack (review #4).
    reason = _docker_preflight_reason(state.get("target_repos"), exclude_name_substr=exec_id)
    if reason:
        # Fail fast via TYPED STATE — do NOT hand-write a marker into the skill's own verdict file.
        # (That Python-into-skill-file mutation was the multi-writer fragility G3 removes: sit_triage
        # now reads `preflight_failed` from state, not a `_preflight_short_circuit` file marker, so no
        # cross-phase reliance on a hand-merged JSON. The skill's verdict file is written only by the
        # skill.) failure_class is environment_failure, not could_not_verify -- see
        # _docker_preflight_reason's docstring for why this specific case never auto-retries.
        telemetry.station_event(exec_id, 6.2, "end", automation_result="failed",
                                failure_class="environment_failure", preflight="insufficient_resources")
        return {"preflight_failed": True, "preflight_reason": reason}

    # SIT-stage concurrency slot (manual-findings #18): WAIT here for a free slot instead of standing up
    # a heavy stack that would over-commit Docker alongside other runs. Held until the run ends (released
    # in cli.py::_execute's finally — guaranteed on success/failure/crash). Interruptible + idempotent.
    slot = await _acquire_sit_slot(exec_id)
    telemetry.station_event(exec_id, 6.2, "slot", sit_slot=slot)

    # The authoritative junit for THIS run — absolute, so it lands in the run's artifacts dir regardless
    # of which clone pytest executes in, and sit_triage reads the SAME file (no checkout-relative drift,
    # no fallback to a prior run — EXE-f749212a). Unlink stale first so its presence is a per-run signal.
    junit_path = _sit_junit_path(exec_id)
    if junit_path.exists():
        junit_path.unlink()

    # Retry-aware prompt: prep_env_retry bumped env_retry_attempts before re-entering here. Without
    # this, a retry would just re-run identical steps and fail identically -- the point of a retry is
    # remediation (rebuild a stale image, bring infra up fresh), not repetition.
    env_retry_attempt = state.get("env_retry_attempts", 0)
    retry_note = ""
    if env_retry_attempt > 0:
        prior_evidence = state.get("sit_report", {}).get("evidence", "")
        retry_note = (
            f"\n\nRETRY {env_retry_attempt}/{config.MAX_ENV_RETRY_ATTEMPTS} after an environment_failure "
            f"on the previous attempt (a harness/infra issue, NOT a code defect). Prior evidence:\n"
            f"{prior_evidence}\n"
            f"Before re-running, attempt remediation for the SPECIFIC issue above -- e.g. if a Ruby "
            f"worker image looks stale/corrupt, rebuild it with `tools/ruby_image_cache.py --repo <repo> "
            f"--worktree <path> --force-rebuild`; if infra (LocalStack/Kafka/Redis) didn't come up "
            f"cleanly last time, tear it down and bring it up fresh; if a mock/network dependency was "
            f"unreachable, verify it's actually running before proceeding. Do not just re-run identical "
            f"steps expecting a different result -- fix the specific thing that broke, or this retry "
            f"will fail the same way and exhaust the budget for nothing."
        )

    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_run",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 2 (execute) ONLY for {tid} (`--only run`), HEADLESS. "
            f"The SIT was authored + human-approved already — do NOT re-author. EXECUTE it local + "
            f"mock-first: run ONLY the changed repo locally per its language-scoped build_env "
            f"(ruby=docker) and mock the rest (ocean_mock_helper + route_local); never present a "
            f"native-host Ruby run as passed. "
            f"TEST-DATA (FIX A2-1): BEFORE pytest, materialize this ticket's authored-scenario payload "
            f"templates (the seq1/seq4 local test-data the SIT loads via get_pay_load_from_json_file / "
            f"get_pay_load_from_db, keyed by the test's TestRail-case-id placeholders) as LOCAL json files, "
            f"synthesized from the approved scenarios — do NOT depend on TestRail case creation (this headless "
            f"run may be approve-no-testrail, where no TestRail cases exist). The templates MUST exist before "
            f"collection/setup or every test errors at setup (could_not_run). "
            f"This run's authoritative junit is the ABSOLUTE path {junit_path} — Station 3 (sit_triage) "
            f"reads back exactly THAT file; it must exist and be non-empty when this step ends. "
            f"DO NOT run every changed test file in ONE pytest invocation inside this single turn "
            f"(S4/I12, run-monitoring-findings-06af6088.md): a single full-chain test can sit in a "
            f"`poller.poll(10,...)` for up to 10 min or a `custom_sleep_time(900)` wait, and chaining "
            f"several files into one Bash call risks the OUTER turn/tool timeout killing the WHOLE "
            f"pytest process (exit 143) BEFORE `--junitxml` is ever written (it's only written on a "
            f"normal exit) — losing every file's results, including ones that already passed, and "
            f"forcing a blind full re-run (confirmed live: MM-14381 lost its entire junit this way). "
            f"Instead follow ocean-automation-testing/SKILL.md's Step 6 'suite-level time budget' "
            f"recipe verbatim: loop ONE bounded `timeout 600 pytest --timeout=300 -m \"not needs_env\" "
            f"<single file>` per changed test file, each into its OWN `reports/junit_batch_N_<exec_id>.xml` "
            f"(never abort the loop on one batch's failure/timeout — every later file still deserves its "
            f"turn), then merge all produced batch files into {junit_path} with "
            f"`tools/merge_junit.py --out {junit_path} --glob 'reports/junit_batch_*_<exec_id>.xml'` as the "
            f"LAST step. If this turn is killed mid-loop, whatever batch files already exist on disk are "
            f"still there for a follow-up merge — do not discard them. `--timeout=300` per test still "
            f"applies inside each batch (MM-14132/EXE-aea95d39 issue #6b); `pytest-timeout` counts fixture/"
            f"setup time too, so if a setup-heavy FIRST item (Rails boot + seed + a legit `poll(5,...)`) "
            f"trips 300s, raise it to ~480s — never remove it. Do NOT "
            f"run Station 3 (report/verdict) — the graph's sit_triage node does that next."
            f"{retry_note}"
            f"\n\n{_summary(state)}"
            f"{_container_directive(state, state.get('worktree_dir', ''))}"
            f"{_sit_infra_directive(state)}"
        ),
    )
    _recover_sit_junit(exec_id, junit_path)  # judge-A: if the agent wrote only reports/, recover the exec-scoped copy
    junit_present = junit_path.exists() and junit_path.stat().st_size > 0
    telemetry.station_event(exec_id, 6.2, "end", junit_present=junit_present)
    # Publish the exact junit path + whether THIS run produced it, so sit_triage scores this run's
    # evidence (or fails loud) instead of re-discovering a file and falling back to a prior run.
    return {"sit_junit_path": str(junit_path), "sit_junit_present": junit_present}


# ------------------------------------------------------------------ Station 6c' — TestRail cases (parallel)
async def sit_testrail(state: OceanState) -> dict:
    """Create the TestRail cases for the approved SIT (Project 22 / Suite 197). Runs IN PARALLEL with
    sit_run — TestRail's API is slow + rate-limited, so it must not block the functional gate. Returns
    the case map via STATE (a dedicated file, not the shared verdict json) to avoid a write race with
    the concurrent sit_run/sit_triage. sit_triage substitutes these real case IDs into the committed
    test file's TCNOTADDED{N} placeholders (MM-14738)."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.3, "start")
    tr_dir = config.artifacts_dir(exec_id)
    # create_testrail_cases.py always names its own output "<TICKET>_testrail_result.json" under
    # whatever --outdir it's given -- there is no flag to target an arbitrary filename, so tr_path
    # must match that convention exactly (not an arbitrary name) or nothing writes here at all.
    tr_path = tr_dir / f"{tid}_testrail_result.json"
    if tr_path.exists():
        tr_path.unlink()
    # rows.json (and thus the case_map keys sit_triage substitutes) MUST be sourced from the COMMITTED
    # TEST FILE's own TCNOTADDED{N} markers, NOT the persisted qa_scenarios list: a qa_review_gate
    # "changes" loop can REDESIGN the scenarios after qa_scenarios ran (sit_author rewrites them into the
    # file, never back to qa_scenarios_path), so that list goes STALE — a row set built from it would
    # misalign with the file's placeholders and tag test methods with the WRONG case ids at substitution.
    # The file is always 1:1 with itself; the scenarios list is at best supplementary wording.
    test_path = state.get("qa_test_path") or "(the committed ticket SIT file)"
    scenarios_hint = state.get("qa_scenarios_path") or ""
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_testrail",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 1b (testrail) ONLY for {tid} (`--only testrail`): "
            f"create the TestRail cases for the SIT already authored + approved via ocean-qa-agent "
            f"(Project 22 / Suite 197 — its Step 6, tools/create_testrail_cases.py). Do ONLY TestRail "
            f"case creation for the EXISTING authored test at {test_path} — do "
            f"NOT re-author, execute, or open any PR. rows.json does NOT already exist -- sit_author "
            f"always authors with --skip-testrail (this TestRail decision is made later, at the review "
            f"gate), so ocean-qa-agent's Step 6a never ran. Build it yourself first, per Station 1b's "
            f"own instructions, sourcing the case set (ONE row per TCNOTADDED{{N}}) AUTHORITATIVELY from "
            f"the COMMITTED TEST FILE at {test_path} — its TCNOTADDED{{N}} parametrize markers, AC MAP "
            f"comment block, and per-method AC-citing docstrings — so every row maps 1:1 to a real "
            f"placeholder in the file. Do NOT drive the placeholder set from the persisted qa_scenarios "
            f"list: a qa_review_gate 'changes' loop may have REDESIGNED the scenarios after qa_scenarios "
            f"ran, leaving it STALE — a mismatched row set tags methods with the WRONG case ids when "
            f"sit_triage substitutes."
            + (f" Use the GAN-hardened scenarios at {scenarios_hint} ONLY as supplementary detail for "
               f"richer GIVEN/WHEN/THEN wording, never as the placeholder source." if scenarios_hint else "")
            + " "
            f"DECOUPLING (FIX A2-1): the LOCAL seq payload templates "
            f"/ test-data are materialized by sit_run (Station 2), NOT here — this node touches ONLY the "
            f"TestRail API, so the local SIT never depends on TestRail case creation to resolve its payloads. "
            f"This runs in parallel with the local SIT run, so "
            f"touch ONLY TestRail (respect its rate limits). "
            f"CREDS (MM-14132/EXE-aea95d39): create_testrail_cases.py needs TestRail Basic-Auth creds (for "
            f"the fourkitesqa instance) — it accepts `test_rail_email`/`test_rail_password` (Jenkins) OR "
            f"`TESTRAIL_EMAIL`/`TESTRAIL_API_KEY` (a developer's ~/.zshrc). These are USUALLY already "
            f"inherited from the shell that launched the pipeline — check `printenv TESTRAIL_EMAIL` (name "
            f"only; NEVER echo the value) and just run the tool if it's set. ONLY if genuinely absent, run "
            f"the tool via `zsh -ic '<full create_testrail_cases.py command>'` in a SINGLE command so "
            f"~/.zshrc's exports load in their native zsh and reach the child process (a bash `source "
            f"~/.zshrc` is unreliable — wrong interpreter, interactive guards, and env is lost across "
            f"separate Bash calls). Without creds it aborts pre-flight and creates ZERO cases (all "
            f"TCNOTADDED kept). Run "
            f"ocean-qa-agent/tools/create_testrail_cases.py with --outdir {tr_dir} -- its own naming "
            f"convention writes exactly {tr_path} there (section_ids + case_map + failed, per "
            f"ocean-qa-agent/SKILL.md Step 6). Do NOT rename, move, or write to a different "
            f"filename.\n\n{_summary(state)}"
        ),
        verdict_path=tr_path,  # EXE-2755f777: unlinked fresh above, exclusively owned by this node
    )
    case_map: dict[str, int] = {}
    if tr_path.exists():
        try:
            payload = json.loads(tr_path.read_text())
        except (ValueError, OSError):
            payload = {}
        # Per-entry resilient, matching create_testrail_cases.py's own per-row-isolation contract:
        # one malformed entry (e.g. a null value from a row the script itself recorded into `failed`)
        # must not discard every OTHER entry's already-created, perfectly valid case id.
        for k, v in (payload.get("case_map") or {}).items():
            try:
                case_map[str(k)] = int(v)
            except (ValueError, TypeError):
                continue
    telemetry.station_event(exec_id, 6.3, "end", testrail_case_count=len(case_map))
    return {"qa_testrail_case_map": case_map}


# ------------------------------------------------------------------ Station 6c — report + triage + verdict
async def sit_triage(state: OceanState) -> dict:
    """Parse junit, triage the run, write the canonical verdict, and (on PASS) open the test-automation
    draft PR (skill Station 3, `--only report`). This node reads that verdict; the graph branches on it.
    A missing verdict file is treated as could_not_verify as a backstop."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.4, "start")
    verdict_path = config.automation_verdict_path(tid)
    # sit_run's resource-preflight short-circuit (Workstream 3.4) is carried in TYPED STATE
    # (`preflight_failed`), not a marker Python wrote into the skill's verdict file — when set,
    # pytest never ran (no reports/junit.xml to parse), so skip the redundant, expensive Station-3
    # skill call and emit the environment_failure verdict directly from state. Not could_not_verify --
    # this IS a harness/infra limit, just a non-retriable one (after_sit_triage checks preflight_failed
    # separately and never routes this specific case to the environment_failure retry).
    if state.get("preflight_failed"):
        # No run_skill call on this path -- do NOT unlink verdict_path here, or it destroys
        # sit_resolve/sit_author's still-relevant forensic record (resolved repo set, domain bucket,
        # pr_number, test_path) for zero benefit (the retry-guard this would serve is never exercised
        # since run_skill is never invoked below).
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="environment_failure", preflight_short_circuit=True)
        return {"automation_result": "failed", "failure_class": "environment_failure",
                "execution_mode": "local-mock-first",
                "test_automation_pr_url": "", "sit_findings": [],
                "needs_onboarding": False, "onboard_repo": "",
                "sit_report": {"tests": [], "changed_repos": [], "dependencies": [],
                               "evidence": state.get("preflight_reason", ""), "testrail_case_count": 0,
                               "ac_coverage": []}}
    # EVIDENCE GUARD (EXE-f749212a): score ONLY this run's junit. If sit_run produced none, refuse to
    # triage — do NOT let the skill re-discover a file and silently fall back to a PRIOR run's junit and
    # emit a confident could_not_verify on a run whose real result is unknown. Fail loud + specific.
    junit_path = Path(state.get("sit_junit_path") or str(_sit_junit_path(exec_id)))
    junit_present = state.get("sit_junit_present")
    if junit_present is None:  # resilient if sit_run's state key didn't propagate — check disk
        junit_present = junit_path.exists() and junit_path.stat().st_size > 0
    if not junit_present:
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="could_not_verify", sit_junit_missing=True)
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "needs_onboarding": False, "sit_findings": [],
                "sit_report": {"tests": [], "changed_repos": [], "dependencies": [],
                               "evidence": f"current-run junit absent: {junit_path}", "ac_coverage": []},
                "final_outcome": (f"SIT_JUNIT_MISSING: this run produced no junit at {junit_path}; refused "
                                  f"to score a prior run's evidence")}
    if verdict_path.exists():
        # Fresh Station-3 attempt, right before the one call that's actually about to run: this
        # node's own contract fully OVERWRITES the file with the final AutomationVerdict schema
        # (never merges with sit_resolve/sit_author's earlier, different-shaped writes to the same
        # path) — so unlinking here is safe, and makes its existence afterward a trustworthy
        # per-attempt signal for the verdict_path retry-guard below (EXE-2755f777).
        verdict_path.unlink()

    # Finding 2e: compare the test file's fingerprint against the one sit_author recorded, BEFORE this
    # node does its own TCNOTADDED substitution below (otherwise our own rewrite would look like an
    # agent edit). A mismatch means sit_run rewrote the test mid-station -- the test_fault -> fix ->
    # re-run path -- which is the thing that needs a human acknowledgement. Deterministic: it does not
    # depend on the agent reporting anything. Only meaningful when both hashes are known; an unknown
    # hash on either side yields False rather than a false accusation.
    _authored_sha = state.get("qa_test_sha") or ""
    _current_sha = _file_sha(state.get("qa_test_path") or "")
    detected_test_edit = bool(_authored_sha and _current_sha and _authored_sha != _current_sha)
    detected_diff = ""
    if detected_test_edit:
        # Produce the ACTUAL diff from the authoring snapshot, so the human asked to acknowledge this
        # edit can see it even when the skill reported no test_diff of its own.
        detected_diff = _diff_against_snapshot(exec_id, state.get("qa_test_path") or "")
        ui.milestone("Finding 2e: the SIT file changed between authoring and triage — a test edit "
                     "happened during the run; the ready-flip will require a human acknowledgement")
        telemetry.station_event(exec_id, 6.4, "test_edit_detected",
                                authored=_authored_sha, current=_current_sha,
                                diff_bytes=len(detected_diff))

    # MM-14738: fan-in point -- both sit_run (already executed against the placeholder-keyed file)
    # and sit_testrail (created the real cases) are guaranteed done by here. Plain-code substitution,
    # no agent: replace TCNOTADDED{N} with its real case id BEFORE the commit-skill call below, so
    # cloudqwest/test-automation never receives a placeholder for a case that actually exists.
    case_map = state.get("qa_testrail_case_map") or {}
    test_path_str = _resolve_test_file(state.get("qa_test_path") or "")
    _post_subst_sha = ""
    if case_map and test_path_str:
        test_file = Path(test_path_str)
        if test_file.exists():
            content = test_file.read_text()
            # Longest-placeholder-first: "TCNOTADDED1" is a PREFIX of "TCNOTADDED10"/"TCNOTADDED11" --
            # replacing the short one first corrupts the long one ("TCNOTADDED10" -> "<id1>0", a
            # fabricated case id silently committed). Sorting by length descending guarantees the
            # 2-digit placeholders are gone before any 1-digit placeholder's replace call can touch them.
            for placeholder in sorted(case_map, key=len, reverse=True):
                content = content.replace(placeholder, str(case_map[placeholder]))
            test_file.write_text(content)
            # Finding 2e: re-baseline the fingerprint to OUR OWN post-substitution content. Without
            # this, an environment_failure retry (sit_triage -> prep_env_retry -> sit_run -> sit_triage,
            # which does NOT re-run sit_author) would compare the second pass against the stale
            # pre-substitution hash and report a spurious test edit on every retry.
            _post_subst_sha = _file_sha(test_path_str)

    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_triage",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 3 (report) ONLY for {tid} (`--only report`): parse "
            f"EXACTLY this junit — THIS run's authoritative evidence — for the per-test pass/fail:\n"
            f"  {junit_path}\n"
            f"Do NOT search for, guess, or fall back to any OTHER junit (a different exec-id, a bare "
            f"junit.xml, a checkout `reports/` copy, or a prior run): scoring another run's junit "
            f"false-verdicted a PASSING run in EXE-f749212a. If {junit_path} is absent/empty, report "
            f"could_not_verify with reason 'current-run junit missing' — never substitute another file. "
            f"TRIAGE any failure — test_fault "
            f"(fix + re-run, capped) vs code_fault (real defect -> findings_for_coder) vs could_not_verify. "
            f"On PASS, commit the SIT into cloudqwest/test-automation on an {tid}/… branch, open a DRAFT PR "
            f"(reuse an existing {tid} test-automation PR — do not duplicate), and set test_automation_pr_url. "
            f"Write the verdict object to {verdict_path} exactly per SKILL.md Station 3. Do NOT flip the "
            f"service PR, merge, or deploy.\n\n{_summary(state)}"
        ),
        verdict_path=verdict_path,  # EXE-2755f777: unlinked fresh above
        model=config.JUDGE_MODEL,   # F7: triage/classification on a different model than the coder
    )
    if not verdict_path.exists():
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="could_not_verify")
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "needs_onboarding": False, "sit_report": {}, "sit_findings": [],
                "final_outcome": "sit skill wrote no verdict"}

    # A1-1 (master guard): the skill writes AutomationVerdict from SKILL.md PROSE, not an injected schema,
    # so its SHAPE can drift (EXE-968500e9: `changed_repos` as strings crashed the whole run here). The
    # model's field_validators coerce the known slips, but ANY residual schema violation must DOWNGRADE to
    # could_not_verify — never crash a run that actually executed the SIT. A present-but-invalid verdict is
    # treated exactly like a missing one.
    # Was `fidelity_rung` EMITTED, or merely absent? Both currently arrive as 0 (the field defaults
    # to 0 and `_coerce_fidelity_rung` maps anything unparseable to 0), and `after_sit_triage` stops
    # on rung 0 — so "the skill never wrote the field" and "this run was genuinely trivial-green"
    # produce an identical stop with an identical message. Those are completely different problems:
    # one is a skill not honouring its own contract (SKILL.md:689 marks the field REQUIRED on every
    # PASS, and 0 of 18 recorded verdicts carry it), the other is a real fidelity result about this
    # ticket. Read the raw key BEFORE validation, where the difference still exists.
    try:
        _raw_verdict = json.loads(verdict_path.read_text())
        _rung_emitted = isinstance(_raw_verdict, dict) and "fidelity_rung" in _raw_verdict
    except Exception:  # noqa: BLE001 — the real parse below owns the error path
        _rung_emitted = False
    try:
        v = schemas.AutomationVerdict.model_validate_json(verdict_path.read_text())
    except Exception as e:  # noqa: BLE001 — malformed/schema-invalid verdict → non-fatal could_not_verify
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="could_not_verify", verdict_parse_error=type(e).__name__)
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "needs_onboarding": False, "sit_report": {}, "sit_findings": [],
                "final_outcome": f"sit verdict failed schema validation: {type(e).__name__}: {e}"[:300]}
    # F1 (architecture review): DETERMINISTIC pass/fail from THIS run's junit is authoritative — the LLM
    # verdict is kept only for the CLASSIFICATION (failure_class, findings). If the two disagree, the junit
    # wins; that gap is exactly the verifier-self-report / reward-hacking risk F1 targets.
    #   det=passed  -> passed, no failure_class (the tests objectively passed; any LLM "failure" is a false negative)
    #   det=failed  -> failed, KEEP the LLM's failure_class (its job: code_fault vs test_fault vs env); if the
    #                  LLM thought it PASSED and gave no class, we can't classify a failure it misread -> could_not_verify
    #   det=could_not_verify -> the junit is unreadable or incomplete. NEVER falls back to the LLM: that
    #                  fallback (the old det=None path) let a malformed junit plus a self-reported
    #                  "passed" flip the PR ready with no human touch, silently, which is precisely the
    #                  reward-hacking surface F1 exists to close.
    # F4 (architecture review): sit_run executes under bypassPermissions in a writable worktree, so the
    # grading junit is a reward-hacking surface. Deterministic grading (F1) already reads it from the
    # run's artifacts dir (outside the worktree) AFTER sit_run's turn ends — but also FINGERPRINT the
    # exact bytes we score into telemetry so the graded evidence is auditable / tamper-evident.
    try:
        import hashlib
        _junit_sha = hashlib.sha256(junit_path.read_bytes()).hexdigest()[:16] if junit_path.exists() else ""
    except OSError:
        _junit_sha = ""
    det_result, det_detail = _junit_pass_fail(junit_path)
    automation_result = v.automation_result
    failure_class = v.failure_class
    if det_result is not None:
        automation_result = det_result
        if det_result == "passed":
            failure_class = ""
        elif det_result == "could_not_verify":
            # Unreadable/incomplete evidence. Do NOT keep an LLM failure_class here: classifying a
            # fault (code_fault/test_fault) from evidence we could not read would send the code_fault
            # loop chasing a defect nobody has established exists.
            failure_class = "could_not_verify"
            ui.milestone(f"F1: junit could not be verified ({det_detail}) -- grading could_not_verify, "
                         f"NOT falling back to the triage agent's self-report "
                         f"('{v.automation_result}'). Re-run the SIT to produce readable evidence.")
            telemetry.station_event(exec_id, 6.4, "det_unverifiable",
                                    llm_result=v.automation_result, detail=det_detail)
        else:  # det says failed
            failure_class = v.failure_class or "could_not_verify"
        if det_result != v.automation_result and det_result != "could_not_verify":
            ui.milestone(f"F1: junit is authoritative -> {det_result} ({det_detail}); LLM triage said "
                         f"'{v.automation_result}' -> trusting the deterministic junit parse")
            telemetry.station_event(exec_id, 6.4, "det_override",
                                    det_result=det_result, llm_result=v.automation_result, detail=det_detail)
            # P1 (judge follow-up): if the junit says PASSED but the LLM thought it FAILED, the skill
            # never ran its on-PASS side-effect (commit the SIT + open the test-automation draft PR) --
            # so this now-passing run has NO test-automation PR. Flag it loudly for manual follow-up
            # rather than let a green run silently ship without its SIT PR.
            if det_result == "passed" and not v.test_automation_pr_url:
                ui.milestone("F1/P1: junit PASSED but the triage opened no test-automation PR (it "
                             "believed the run failed) -- the SIT passed WITHOUT a PR; needs a manual "
                             "PR or a re-run to produce one")
                telemetry.station_event(exec_id, 6.4, "pass_without_test_pr", detail=det_detail)
    # Finding 2a (judge follow-up): the DISK audit the mock wrote itself is authoritative over the
    # verdict's self-reported list — same principle as F1's junit-over-LLM rule above. Union rather
    # than replace, so a path the agent reported but the mock's audit missed (e.g. a second mock
    # launched without the audit path) still counts. Re-apply the rung cap here because the schema's
    # own `_cap_rung_on_unmocked_hit` only saw the SELF-REPORTED list at validation time.
    unmocked_paths = sorted(set(v.unmocked_paths_hit) | set(_read_unmocked_paths(exec_id)))
    fidelity_rung = v.fidelity_rung
    if unmocked_paths and fidelity_rung > 1:
        if not v.unmocked_paths_hit:
            ui.milestone(f"Finding 2a: the mock's own audit recorded {len(unmocked_paths)} unmocked "
                         f"path(s) the triage verdict did not report — capping fidelity to Rung 1")
            telemetry.station_event(exec_id, 6.4, "unmocked_selfreport_gap",
                                    disk=len(unmocked_paths), reported=0)
        fidelity_rung = 1
    # Finding 2 (②-a): the unmocked audit above can only cap on PRESENCE of a degraded call, so the
    # "SUT never ran at all" case slipped through it entirely. Require POSITIVE corroboration for a
    # Rung-2 claim in mock-first mode: the mock must have observed at least one SUT-origin write. Zero
    # observed writes means the assertions held against the test's own seed, which is Rung 0 by
    # definition (trivial green) — and after_sit_triage routes Rung 0 to stop_run, so this needs a
    # human instead of flipping the PR.
    sut_writes, setup_writes, activity_known = _sut_write_activity(exec_id)
    rung_corroboration = ""
    if v.execution_mode == "local-mock-first" and fidelity_rung >= 2:
        if not activity_known:
            # Could not check — NOT the same as checked-and-idle. This changes NO routing and is read
            # by no consumer today: it is a ui.milestone + a telemetry event, with the state key kept
            # so a future consumer (report line, gate reason) has it without re-plumbing. Making it
            # block would halt every run whose mock predates the audit, on no evidence of a defect.
            # Two earlier versions of this comment overclaimed (human_gate surfaces it; the final
            # report carries it) — neither is true, so it is stated plainly here.
            rung_corroboration = ("the mock wrote no SUT-activity audit, so this run's Rung-2 claim "
                                  "rests on the triage agent's own word")
            ui.milestone(f"Finding 2: Rung {fidelity_rung} is UNCORROBORATED — {rung_corroboration}. "
                         f"(An older mock, or one launched without --sut-activity-log.)")
            telemetry.station_event(exec_id, 6.4, "rung_uncorroborated", reason="no sut_activity.json")
        elif sut_writes == 0:
            ui.milestone(f"Finding 2: the triage claims fidelity Rung {fidelity_rung}, but the mock "
                         f"observed ZERO SUT-origin writes ({setup_writes} setup write(s)) — the "
                         f"assertions held against the test's own seed. Capping to Rung 0 "
                         f"(trivial green); this needs a human, not a ready-flip.")
            telemetry.station_event(exec_id, 6.4, "rung_capped_no_sut_activity",
                                    claimed=fidelity_rung, setup_writes=setup_writes)
            fidelity_rung = 0
    # A PASS with no rung at all is a CONTRACT failure by the skill, not a fidelity result about
    # this ticket. Say which one it is — the stop that follows looks identical either way.
    if automation_result == "passed" and not _rung_emitted:
        ui.milestone(
            "the SIT verdict carries NO `fidelity_rung` — SKILL.md marks it REQUIRED on every PASS. "
            "This run is treated as Rung 0 (needs a human, no ready-flip), but that is the SKILL not "
            "reporting fidelity, NOT a measured trivial-green. Fix the emission in "
            "ocean-automation-testing before reading anything into the rung.")
        telemetry.station_event(exec_id, 6.4, "rung_not_emitted", automation_result=automation_result)
    telemetry.station_event(exec_id, 6.4, "end", automation_result=automation_result,
                            failure_class=failure_class, execution_mode=v.execution_mode,
                            needs_onboarding=v.needs_onboarding, graded_junit_sha=_junit_sha,  # F4: audit fingerprint
                            fidelity_rung=fidelity_rung, ref_load_used=v.ref_load_used,  # Finding 2c
                            unmocked_paths_hit=len(unmocked_paths))  # Finding 2a: count only (telemetry is flat kwargs)
    return {
        "automation_result": automation_result,
        "failure_class": failure_class,
        "execution_mode": v.execution_mode,
        "fidelity_rung": fidelity_rung,
        "rung_emitted": _rung_emitted,   # False => the 0 above is an ABSENT field, not a measurement
        "rung_corroboration": rung_corroboration,
        "ref_load_used": v.ref_load_used,
        "test_automation_pr_url": v.test_automation_pr_url,
        "sit_findings": v.findings_for_coder,
        "needs_onboarding": v.needs_onboarding,
        "onboard_repo": v.onboard_repo,
        # Finding 2e: carry the post-substitution baseline forward so an env-retry's second triage
        # compares against what WE wrote, not the pre-substitution authoring hash (else every retry
        # reports a spurious test edit). Unset when we didn't substitute — keep the original baseline.
        **({"qa_test_sha": _post_subst_sha} if _post_subst_sha else {}),
        "sit_report": {
            "unmocked_paths_hit": unmocked_paths,  # Finding 2a: full list lives in the report, not flat state
            # Finding 2e: a green that followed a test edit must be visible to the ready-flip gate
            # (_test_edit_ack_reason) and to the human it pauses for. OR-ed with the deterministic
            # hash comparison above so an agent that never sets the flag (the common case on a PASS,
            # where failure_class is "" by contract and the automatic latch cannot fire) still trips
            # the acknowledgement.
            "test_edited": bool(v.test_edited or detected_test_edit),
            # Prefer whatever the skill supplied (it can scope the diff to the meaningful hunks);
            # fall back to the diff WE computed from the authoring snapshot, so a detected-but-
            # unreported edit still shows the human what actually changed.
            "test_diff": v.test_diff or detected_diff,
            "ac_before": v.ac_before,
            "ac_after": v.ac_after,
            "tests": [t.model_dump() for t in v.tests],
            "changed_repos": [c.model_dump() for c in v.changed_repos],
            "dependencies": [d.model_dump() for d in v.dependencies],
            "evidence": v.evidence,
            # MM-14738: sit_testrail returns qa_testrail_case_map (via state), not a run id -- no
            # add_run call exists anywhere in either skill, so "testrail_run_id" never reflected a
            # real TestRail run; this is the actually-meaningful signal (0 when TestRail was skipped
            # or every case create failed).
            "testrail_case_count": len(state.get("qa_testrail_case_map") or {}),
            # AC traceability (additive, optional — [] if the skill hasn't started emitting it yet).
            "ac_coverage": v.ac_coverage,
        },
    }


# ------------------------------------------------------------------ graph-owned repo onboarding
async def learn_repo(state: OceanState) -> dict:
    """Onboard an ocean repo Station 6 reported as unsupported — the CONTROL PLANE owns this.

    Station 6 (headless) is told NOT to self-onboard: when it meets an unknown ocean repo it reports
    needs_onboarding + onboard_repo and stops. This node then invokes the skill's learn-a-repo MECHANIC
    (`local_service_execution.md` Steps N1-N5) in an explicit, authorized onboarding pass — confirm it's
    an ocean/isbu service, clone it if absent, profile it, and persist+commit the learned profile back
    into the skill references DELIBERATELY (a tracked graph step, not a hidden side effect of a test run).
    Then it loops back to sit_resolve to re-resolve now that the repo is supported. Capped by
    MAX_ONBOARD_ATTEMPTS so a repo that still reports unsupported after profiling ends as could_not_verify.
    """
    repo = state.get("onboard_repo") or ""
    attempt = state.get("onboard_attempts", 0) + 1
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 5.95, "learn_repo_start", onboard_repo=repo, attempt=attempt)
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="learn_repo",
        ticket_id=tid,
        task_prompt=(
            f"AUTHORIZED ONBOARDING PASS for the Aquaman control plane — this is NOT a test run.\n"
            f"Repo to learn: {repo!r} (Station 6 reported it unsupported for {tid}).\n"
            f"Follow 'Onboarding an unsupported repo (learn a new repo)' in "
            f"skills/ocean-qa-agent/references/local_service_execution.md (Steps N1-N5): first confirm it "
            f"is an ocean/isbu service — if it is NOT (a shared/platform lib or another team's repo), do "
            f"NOT onboard it; write nothing and report it as out-of-scope. Otherwise auto-detect it under "
            f"~/Documents/projects/ or clone cloudqwest/{repo} if absent, PROFILE it into the standard "
            f"per-repo local-run anatomy, and WRITE the profile back per Step N4's two-file split: the "
            f"canonical REGISTRY ROW (repo/lang/role/run-cmd/port/health/boot-gates + the language-scoped "
            f"build-rule bucket) into skills/_shared/ocean-knowledge/ocean-repos.md, and the OPERATIONAL "
            f"profile (infra/mock bullets, boot recipe, gotchas, api_base_urls row) into "
            f"local_service_execution.md (+ this skill's Key-ocean-repos list + team-repo-manifest.yml). "
            f"Do NOT duplicate the registry columns across both files. You ARE authorized to "
            f"persist+commit the learned profile in the fk-aideveloper checkout (message "
            f"'{tid}: learn {repo} local-run profile') — the control plane invoked you specifically to "
            f"onboard. Do NOT run the SIT, open a service PR, or flip anything.\n\n{_summary(state)}"
        ),
    )
    telemetry.station_event(exec_id, 5.95, "learn_repo_end", onboard_repo=repo, attempt=attempt)
    # Record what was learned and clear the request so the Station 6 re-run starts clean.
    return {"onboard_attempts": attempt, "repo_onboarded": repo,
            "needs_onboarding": False, "onboard_repo": ""}


# ------------------------------------------------------------------ code_fault rework prep
async def prep_rework(state: OceanState) -> dict:
    """On a Station-6 code_fault, re-enter the FULL loop (coder -> Station 5 -> Station 6).
    Bump the shared coding-attempts budget, reset the per-attempt review counter, and
    clear stale review findings (SIT findings are carried in sit_findings for the coder).
    Also reset env_retry_attempts: the SIT run that follows this code fix is a FRESH attempt that
    deserves its own environment-retry budget, not one already exhausted by an earlier, unrelated
    SIT run before this code fault was even diagnosed."""
    attempt = state.get("coding_attempts", 0) + 1
    telemetry.station_event(state["execution_id"], 5.9, "code_fault_rework", coding_attempt=attempt)
    # `quality_gate_attempts` resets HERE AND NOWHERE ELSE. A Station-6 code fault is a fresh coding
    # attempt and deserves its own gate budget, exactly like review_iteration and env_retry_attempts
    # above. Do NOT also reset it in `coder` or on after_review's rework edge: `harsh_reviewer
    # --rework--> coder` goes DIRECTLY (prep_rework serves only the code_fault loop), so resetting on
    # that path makes the coder <-> quality_gate cycle genuinely unbounded.
    # `qa_review_iteration` resets here for the same reason. It is written in exactly ONE place
    # (qa_review_gate) and was reset NOWHERE, making it monotonic for the whole run — so two code
    # faults exhausted MAX_QA_REVIEW_ITERATIONS, and after that a human clicking "request changes"
    # was IGNORED: `after_qa_review` (graph.py:189) falls through to `return "sit_run"` and executes
    # the draft the human just rejected. Silently — no telemetry marks the fall-through.
    # `eval_attempts` resets here for exactly the reason `quality_gate_attempts` does, one line up: a
    # Station-6 code fault is a FRESH coding attempt and deserves its own accuracy budget. Do NOT
    # also reset it on after_quality_gate's rework edge — that edge goes DIRECTLY to `coder`, so
    # resetting there makes the coder <-> eval cycle unbounded. `eval_unverified` deliberately does
    # NOT reset (matching quality_gate_unverified): an infra fault that survives a rework should
    # still be visible at the end of the run.
    return {"coding_attempts": attempt, "review_iteration": 0, "review_findings": [],
            "env_retry_attempts": 0, "quality_gate_attempts": 0,
            "quality_gate_findings": [], "quality_gate_stopped": False,
            "qa_review_iteration": 0,
            "eval_attempts": 0, "eval_gap": "", "eval_stopped": False,
            # B1: three of six. `review_coverage_unverified` deliberately does NOT reset, matching
            # quality_gate_unverified — an infra fault that survives a rework stays visible at the end.
            "review_coverage_attempts": 0, "review_coverage_gap": [],
            "review_coverage_stopped": False}


# ------------------------------------------------------------------ environment_failure retry prep
async def prep_env_retry(state: OceanState) -> dict:
    """On a Station-6 AGENT-DIAGNOSED environment_failure (harness/infra broke -- NOT the deterministic
    resource-insufficient preflight short-circuit, which after_sit_triage routes straight to stop_run,
    never here), retry Station 2 (sit_run) ONLY -- not the full coder/review loop, since this isn't a
    code problem. Bump the env-retry budget so sit_run's own task_prompt knows to include remediation
    instructions (rebuild a stale image, bring infra up fresh) instead of blindly repeating the same
    steps. Clears preflight_failed/preflight_reason in case either was set on the SAME attempt for an
    unrelated reason, so a stale flag can't leak into the retry."""
    attempt = state.get("env_retry_attempts", 0) + 1
    telemetry.station_event(state["execution_id"], 6.45, "environment_failure_retry", env_retry_attempt=attempt)
    return {"env_retry_attempts": attempt, "preflight_failed": False, "preflight_reason": ""}


# ------------------------------------------------------------------ human approval gate (optional)
# ------------------------------------------------------------------ D5/D6: node accuracy evaluation
async def _eval_node(state: OceanState, node: str, job: str, output: str) -> dict:
    """Score one node's output with the independent evaluator, and return the state keys.

    ADVISORY BY CONSTRUCTION. This never raises and never routes: it returns state, and only
    `_eval_gate` (below) turns a FAIL into anything, and only when EVAL_ENFORCE is on. An accuracy
    judge that can break a run it was added to observe is worse than no judge -- and this one spends
    a full SDK session, which is exactly the kind of thing that fails for transport reasons.

    Skips silently when NODE_EVAL is off or `node` is not in EVAL_NODES, returning {} so callers can
    `state.update(...)` unconditionally without a second flag check at every call site.

    Runs SYNCHRONOUSLY inside the calling station, before that station prints its own outcome line.
    monitor/app.py:411-425 depends on that ordering: it skips the `eval_<node>` header outright and
    deliberately does not touch `current_label`, so the enclosing station's header stays
    authoritative for any [PAUSED] line that follows.
    """
    if not config.NODE_EVAL or node not in config.EVAL_NODES:
        return {}
    exec_id = state["execution_id"]
    try:
        ev = await agents.evaluate_node(
            node=node,
            ticket_id=state["ticket_id"],
            execution_id=exec_id,
            node_job=job,
            node_output=output,
            cwd=Path(state["worktree_dir"]) if state.get("worktree_dir") else None,
        )
    except Exception as e:  # noqa: BLE001 -- advisory: a judge failure must never fail the station
        telemetry.station_event(exec_id, 4.6, "eval_could_not_run", eval_node=node,
                                error=f"{type(e).__name__}: {e}")
        ui.milestone(f"[eval] {node}: could not run ({type(e).__name__}) -- advisory, continuing")
        return {"eval_unverified": f"{node}: evaluator could not run ({type(e).__name__})"}

    record = {"node": node, "accuracy": ev.accuracy, "verdict": ev.verdict,
              "dimensions": ev.dimensions.model_dump(), "issues": ev.issues,
              "rationale": ev.rationale}
    # A judge that ran but returned an unreadable verdict is NOT a pass. Recorded separately so
    # `_eval_gate` can tell "could not measure" from "measured and failed" -- these must never share
    # a value, which is why `accuracy` and the three dimensions default to None rather than 0.
    unverified = "" if ev.verdict else f"{node}: evaluator returned no readable verdict"
    score = "not reported" if ev.accuracy is None else str(ev.accuracy)
    ui.milestone(f"[eval] {node}: accuracy={score} verdict={ev.verdict or 'UNREADABLE'}")
    telemetry.station_event(exec_id, 4.6, "eval", eval_node=node,
                            accuracy=ev.accuracy, verdict=ev.verdict, issues=len(ev.issues or []))

    evaluations = (state.get("node_evaluations") or []) + [record]
    # The budget is computed HERE, not in the router: LangGraph routers return a string and cannot
    # write state, so a router-side counter would never persist. `_eval_gate` reads the accumulated
    # list, so it must be given the list that includes THIS pass.
    gap = _eval_gate({**state, "node_evaluations": evaluations})
    attempts = state.get("eval_attempts", 0) + (1 if gap else 0)
    stopped = bool(gap) and attempts > config.MAX_EVAL_ATTEMPTS
    # ALL five keys on EVERY pass. `quality_gate` (nodes.py:1315-1317) learned this the hard way: a
    # "write only what changed" shape leaves a stale gap from an earlier pass in state, which then
    # re-injects itself into every later router decision and coder prompt.
    return {"node_evaluations": evaluations, "eval_unverified": unverified,
            "eval_gap": gap, "eval_attempts": attempts, "eval_stopped": stopped}


def _eval_gate(state: OceanState) -> str:
    """Returns "" when nothing should be gated on accuracy, else a short human-readable reason.

    Same shape as `_real_service_gap` above: a pure function of state, so the routing decision is
    testable without running a graph.

    Gates ONLY on an explicit FAIL, and ONLY when EVAL_ENFORCE is on -- node-evaluator.md's own
    policy line. Three cases deliberately do NOT gate:

      * EVAL_ENFORCE off -- the evaluation is still recorded and surfaced, it just routes nothing.
        This is what makes NODE_EVAL safe to switch on: it cannot, by itself, change where a run goes.
      * WARN -- the spec defines it as "minor gaps, usable". Gating on WARN would collapse the
        three-value enum into a two-value one and make the middle rung unreachable.
      * an UNREADABLE verdict ("") -- fails OPEN, matching the sibling precedent at graph.py:330
        ("clean, or could-not-run (fails OPEN, loudly)"). A transport failure or a malformed judge
        reply is an infra fault, and failing closed on it would convert every judge outage into a
        halted pipeline. `eval_unverified` carries it so it reads as unmeasured, never as clean.
    """
    if not config.EVAL_ENFORCE:
        return ""
    for ev in state.get("node_evaluations") or []:
        if not isinstance(ev, dict):
            continue
        if schemas.eval_verdict(ev.get("verdict")) == "FAIL":
            score = ev.get("accuracy")
            got = "not reported" if score is None else score
            return (f"the independent accuracy evaluator FAILED the `{ev.get('node')}` node "
                    f"(accuracy={got}) — see its issues[] for the specific defects")
    return ""


def _real_service_gap(state: OceanState) -> str:
    """Finding 2d: "require a real-service run before a PR goes to review". Returns "" when this run
    genuinely verified the change against real services, else a short human-readable reason why it
    did not.

    HONEST LIMIT (a judge review corrected an earlier overclaim in this docstring): every input below
    is written by the triage skill itself. Nothing independently verifies that a changed repo really
    executed, so a verdict asserting `fidelity_rung: 2` + `ran_on: "local"` clears this gate on its own
    word. What the gate does buy is that the claim must be SPECIFIC and INTERNALLY CONSISTENT — silence
    (no changed_repos at all), an unrecognized mode, a self-declared mock, or a short-circuited rung all
    fail it. The one fully-independent backstop nearby is the mock's own unmocked-path audit, which caps
    the rung from OUTSIDE the verdict (see _read_unmocked_paths).

    The inputs:

      * `fidelity_rung < 2` — Rung 2 is defined (local_service_execution.md) as "the SUT runs the
        reviewed logic end-to-end against real local receiving service(s); a regression in that logic
        would fail the test". Rung 1 means the SUT reached the service but the reviewed branch was
        short-circuited — real signal, but NOT a verification of this change. Rung 0 never reaches
        here (after_sit_triage stops it outright).
      * an unrecognized `execution_mode` — see schemas._coerce_execution_mode.
      * a CHANGED repo that did not run for real. `ChangedRepo.ran_on`'s own contract is that a changed
        repo is always run real ("a mocked repo belongs in dependencies[]") — this VERIFIES that
        contract instead of assuming it, which is the whole point of a gate.

    Note qat-fallback is NOT a gap: QAT is a real service. The finding's concern is a mock-only run
    reaching review, not which real environment was used."""
    rung = state.get("fidelity_rung", 0)
    if not isinstance(rung, int) or rung < 2:
        return (f"fidelity Rung {rung} (<2) — the reviewed logic was not exercised end-to-end "
                f"against real services, so this run did not verify the change itself")
    mode = state.get("execution_mode", "")
    if mode not in ("local-mock-first", "qat-fallback"):
        return f"execution_mode {mode!r} is not a recognized mode — cannot tell how this run executed"
    changed = (state.get("sit_report") or {}).get("changed_repos") or []
    # An EMPTY changed_repos is not a pass — it's an absence of evidence. A judge review found the
    # "did any changed repo run for real?" check below was vacuously true on an empty list, so a
    # verdict that simply omitted the field (its default is [], and _coerce_list_fields maps null ->
    # []) sailed through the very gate meant to require a real-service run. Fail-safe: no recorded
    # execution of the code under review == cannot prove a real-service run.
    if not changed:
        return ("no changed_repos recorded — nothing states that the code under review actually ran, "
                "so a real-service run cannot be verified")
    # A non-dict entry (e.g. `changed_repos: ["ocean-worker"]`, a REAL observed drift shape per
    # EXE-968500e9) states no ran_on at all, so it cannot evidence a real run -- count it, rather than
    # skipping it into a vacuous pass.
    not_real = [(c.get("repo") or "?") if isinstance(c, dict) else str(c) for c in changed
                if not isinstance(c, dict) or c.get("ran_on") not in ("local", "real-local")]
    if not_real:
        return (f"changed repo(s) did not run for real: {', '.join(not_real)} — the code under review "
                f"never actually executed")
    return ""


def _test_edit_ack_reason(state: OceanState) -> str:
    """Finding 2e: "require a human acknowledgement" when a green run followed a test edit. Returns ""
    when no edit happened. Three sources feed `sit_report["test_edited"]`, with HONEST coverage limits
    on each (a judge review corrected an earlier "can't be bypassed" claim here):
      1. the skill reporting it explicitly — omittable;
      2. the automatic latch off a raw `failure_class: "test_fault"`
         (schemas._capture_test_edit_signal) — cannot fire on a PASS, where failure_class is "" by
         contract, which is exactly the case that matters most;
      3. sit_triage's deterministic hash comparison against the authored file — needs no cooperation,
         but only works when the recorded test path RESOLVES on this machine (see _resolve_test_file);
         it is skipped, loudly, when it does not.
    So on a PASS with an omitted flag AND an unresolvable path, the acknowledgement IS skipped. Source 3
    is the one worth strengthening (a named absolute `test_path` in the verdict contract).
    Also flags an AC SHRINK (the test cites fewer acceptance criteria after the edit than before) —
    the specific abuse shape the review worried about: making a failing test pass by narrowing what
    it claims to verify."""
    rep = state.get("sit_report") or {}
    if not rep.get("test_edited"):
        return ""
    before, after = rep.get("ac_before") or [], rep.get("ac_after") or []
    dropped = [a for a in before if a not in after] if isinstance(before, list) and isinstance(after, list) else []
    detail = f"; AC coverage SHRANK — dropped {', '.join(map(str, dropped))}" if dropped else ""
    return (f"this pass followed a test edit (test_fault -> fix -> re-run), so the green reflects a "
            f"REWRITTEN test, not the original one{detail}")


async def human_gate(state: OceanState) -> dict:
    """Optional human approval before the ready-flip (OCEAN_PIPELINE_REQUIRE_APPROVAL). Default OFF
    -> pass-through (auto-flip on green). When ON, interrupt() pauses the run until an engineer
    resumes with a decision (`ocean-pipeline --resume <exe> --approve|--reject`). Either way the
    pipeline still never merges or deploys — that boundary is unchanged.

    Finding 2d: the pass-through is now CONDITIONAL. A green run that cannot demonstrate it exercised
    the change against real services (see _real_service_gap) always pauses for a human, even with
    REQUIRE_APPROVAL off — "require a real-service run before a PR goes to review". This is the one
    case where the default-off setting is overridden, and the reason is stated in the pause message so
    the human isn't asked to approve something with no context (Finding 11)."""
    # Two independent reasons a green run must still be seen by a human, regardless of
    # REQUIRE_APPROVAL: it never exercised the change against real services (2d), or its green came
    # from a test that was rewritten mid-run (2e). Both are reported, not just the first.
    gap = _real_service_gap(state)
    ack = _test_edit_ack_reason(state)
    blockers = [b for b in (gap, ack) if b]
    if not config.REQUIRE_APPROVAL and not blockers:
        return {}
    for b in blockers:
        ui.milestone(f"holding the ready-flip for human approval — {b}")
    if gap:
        telemetry.station_event(state["execution_id"], 6.45, "real_service_gap", reason=gap[:120])
    if ack:
        telemetry.station_event(state["execution_id"], 6.45, "test_edit_ack_required", reason=ack[:120])
    from langgraph.types import interrupt
    decision = interrupt({
        "action": "flip_service_pr_ready",
        "ticket_id": state["ticket_id"],
        "pr_number": state.get("pr_number"),
        "test_automation_pr_url": state.get("test_automation_pr_url"),
        # Finding 11: hand the human the evidence, not just a yes/no question.
        "fidelity_rung": state.get("fidelity_rung", 0),
        "execution_mode": state.get("execution_mode", ""),
        "real_service_gap": gap,
        "tests": ((state.get("sit_report") or {}).get("tests") or [])[:10],
        "changed_repos": (state.get("sit_report") or {}).get("changed_repos") or [],
        "unmocked_paths_hit": ((state.get("sit_report") or {}).get("unmocked_paths_hit") or [])[:10],
        "evidence": (state.get("sit_report") or {}).get("evidence", ""),
        # Finding 2e: the human acknowledging a post-edit pass needs to SEE the edit.
        "test_edit_ack_reason": ack,
        "test_edited": bool((state.get("sit_report") or {}).get("test_edited")),
        "test_diff": ((state.get("sit_report") or {}).get("test_diff") or "")[:4000],
        "ac_before": (state.get("sit_report") or {}).get("ac_before") or [],
        "ac_after": (state.get("sit_report") or {}).get("ac_after") or [],
        "prompt": (
            ("SIT passed, but a human must look before this goes to review:\n"
             + "\n".join(f"  - {b}" for b in blockers) + "\nApprove flipping the service PR to "
             "ready-for-review anyway? "
             if blockers else
             "SIT passed. Approve flipping the service PR to ready-for-review? ")
            + "Resume with --approve or --reject."
        ),
    })
    return {"approval_decision": str(decision)}


# ------------------------------------------------------------------ ready-flip (plain code, on GREEN)
async def flip_ready(state: OceanState) -> dict:
    """On PASS: cross-link the test-automation PR into the service PR and flip the service PR to
    ready-for-review — deterministic gh, run by the graph, NOT an agent. Also moves the ticket to
    In Review + posts a PR-link comment (best-effort Jira). This is the intended AUTOMATED terminal
    action; the human boundary is merge/deploy, which the pipeline never performs."""
    telemetry.station_event(state["execution_id"], 6.5, "start")
    # D4. The mechanical secret scan, BEFORE any gh or Jira call. This is the last automated action
    # in the pipeline and the one that asks a human to look at the diff, so it is the right place
    # for the check -- and the fk-aideveloper gitleaks hook provably does NOT cover this path (see
    # quality.secret_scan's docstring: Aquaman builds SDK hooks in-process and never loads that
    # repo's settings). An upstream LLM security gate does exist (review.md:104-105 makes security
    # an unconditional CRITICAL); this is the deterministic half, not a duplicate of it.
    secret_findings: list = []
    secret_unverified = ""
    if config.SECRET_SCAN:
        dirs = quality.repo_dirs(state.get("worktree_dir", ""),
                                 config.workspace_dir(state["execution_id"]))
        try:
            secret_findings, secret_unverified, scanned = quality.secret_scan(
                dirs, config.SECRET_SCAN_TIMEOUT)
        except Exception as e:  # noqa: BLE001 -- an exploding scanner must not silently pass the flip
            secret_findings, secret_unverified, scanned = [], f"scanner crashed: {type(e).__name__}", 0
        telemetry.station_event(state["execution_id"], 6.5, "secret_scan",
                                repos=len(dirs), scanned=scanned, findings=len(secret_findings),
                                unverified=secret_unverified)
        if secret_unverified:
            # Fails OPEN, loudly -- an absent or broken gitleaks must not halt every ready-flip. The
            # reason is carried to the terminal so it can never read as "scanned and clean".
            ui.milestone(f"[secret-scan] NOT fully scanned: {secret_unverified}")
        if secret_findings:
            # Fails CLOSED on evidence. Do NOT flip, do NOT move the ticket to In Review: flipping a
            # PR ready is the request for human eyes, and a possible live credential should be
            # rotated before that audience widens, not after.
            ui.milestone(f"[secret-scan] BLOCKED the ready-flip: "
                         f"{len(secret_findings)} possible secret(s) in the diff")
            telemetry.station_event(state["execution_id"], 6.5, "stop",
                                    ready_flipped=False, findings=len(secret_findings))
            return {"ready_flipped": False, "final_status": "failed",
                    "secret_findings": secret_findings,
                    "secret_scan_unverified": secret_unverified,
                    "final_outcome": (
                        f"secret_scan_blocked: {len(secret_findings)} possible secret(s) in the "
                        f"diff on branch {state.get('branch')!r}; PR(s) left DRAFT and the ticket "
                        f"was not moved to In Review — rotate the credential, then re-run")}

    # Multi-repo: flip EVERY changed repo's PR ready (fall back to pr_number for a pre-fix single repo).
    pr_numbers = dict(state.get("pr_numbers") or {})
    if not pr_numbers and state.get("pr_number"):
        slugs = _service_slugs(state)
        if slugs:
            pr_numbers = {slugs[0]: state["pr_number"]}
    for slug, pr in pr_numbers.items():
        if slug and pr:
            gitops.cross_link_and_ready(slug, int(pr), state.get("test_automation_pr_url") or "")
    tid = state["ticket_id"]
    jira.transition(tid, "In Review")
    prs_str = ", ".join(f"#{n}" for n in pr_numbers.values()) or "(none)"
    pr_line = f"service PR(s) {prs_str}" + (f" · test PR {state['test_automation_pr_url']}"
                                            if state.get("test_automation_pr_url") else "")
    jira.comment(tid, f"🤖 Aquaman: SIT passed; {pr_line} flipped to ready-for-review. "
                      f"Merge/deploy remain with the engineer.")
    telemetry.station_event(state["execution_id"], 6.5, "end", ready_flipped=bool(pr_numbers))
    return {"ready_flipped": True, "final_status": "completed",
            "secret_findings": [], "secret_scan_unverified": secret_unverified,
            "final_outcome": f"sit_passed; service PR(s) {prs_str} ready-for-review"}


# ------------------------------------------------------------------ stop (rejected / failed / could_not_verify / budget exhausted)
async def stop_run(state: OceanState) -> dict:
    # MM-14816 (G20): AC-blocking product/UX decision the resolver couldn't ground — raised BEFORE any
    # coding (reachability short-circuits here). Post the questions to Jira ONCE (idempotent across
    # oas-autodev resumes — P3) and return final_status="blocked" so cli emits the oas-autodev block
    # marker (→ AWAITING_INPUT, surfaced for product/UX, auto-resumes when answered). This branch is
    # first: it's a pre-coding stop, distinct from every review/SIT/approval reason below.
    questions = [q for q in (state.get("blocking_open_questions") or []) if str(q).strip()]
    # Only the blocked_review_gate POST decision reaches here with the block still set (answer/reject
    # clear it + continue). The blocked_decision=="post" guard makes that explicit.
    if questions and state.get("blocked_decision") == "post":
        tid = state["ticket_id"]
        qlist = "\n".join(f"  {i}. {q}" for i, q in enumerate(questions, 1))
        jira.comment_once(
            tid,
            body=(f"🤖 Aquaman [BLOCKED — open questions]: this ticket has open questions that need a "
                  f"product/UX decision before implementation can start. Please answer in a comment and "
                  f"the pipeline will resume automatically:\n{qlist}"),
            marker="Aquaman [BLOCKED — open questions]",   # stable per-ticket marker → no resume re-spam
        )
        telemetry.station_event(state["execution_id"], 1.5, "stop", reason="blocked_open_questions",
                                blocking_questions=len(questions))
        return {"final_status": "blocked", "ready_flipped": False,
                "final_outcome": "blocked_open_questions: posted to Jira for product/UX; "
                                 "awaiting answers before coding"}
    if str(state.get("rca_approval_decision", "")).lower().startswith("reject"):
        # Human rejected the RCA at its review gate — before any coding started.
        telemetry.station_event(state["execution_id"], 0.15, "stop", reason="rca_rejected_by_engineer")
        return {"final_status": "failed",
                "final_outcome": "human rejected the RCA report at the review gate; no action taken"}
    if str(state.get("approval_decision", "")).lower().startswith("reject"):
        # Human rejected the ready-flip at the approval gate — not a SIT failure.
        telemetry.station_event(state["execution_id"], 6.5, "stop", reason="rejected_by_engineer")
        return {"final_status": "failed", "ready_flipped": False,
                "final_outcome": f"human rejected the ready-flip; service PR "
                                 f"#{state.get('pr_number')} left draft"}
    fc = state.get("failure_class", "")
    branch_set = bool(state.get("branch"))
    pr_opened = bool(state.get("pr_number"))
    # after_review's own "stop" cases land here too, BEFORE open_pr ever ran (no PR yet) -- two shapes,
    # BOTH of which stopped at Station 5 (review), not Station 6 (SIT), which never ran:
    #   - budget exhausted with NO diff (coder wrote nothing to approve), OR
    #   - F3/C1: budget exhausted WITH a diff whose FINAL review still holds unresolved CRITICAL/MAJOR
    #     -- a real branch the reviewer rejected. A `sit_failed` label (station 6) here would misdirect
    #     the human triaging the run to a SIT stage that didn't run; record the review-stage truth.
    review_rejected = state.get("review_verdict") not in (None, "", "APPROVE")
    # THIRD copy of the severity comparison, now routed through the one canonicalizer F2 and F3 share.
    # It gates nothing (after_review already decided to stop by the time we get here), but a
    # non-canonical severity — "P0", " CRITICAL " — made it disagree with the gate that just fired,
    # and everything downstream inherited that disagreement: the run was labelled `sit_failed`
    # against Station 6 (which never ran), the PR note named the wrong stage, and
    # `lessons.record_failure` wrote the wrong action_sig into the cross-ticket store, so the bad
    # signature is recalled into every future ticket in the domain. Duplication of this exact
    # comparison is the defect class this batch set out to remove; leaving one copy behind would have
    # kept it alive in the failure MEMORY rather than the gate.
    has_blocking = any(schemas.is_blocking_finding(f) for f in (state.get("review_findings") or []))
    review_stop_no_diff = review_rejected and not branch_set
    review_stop_blocking = review_rejected and branch_set and not pr_opened and has_blocking
    # Finding 2c (architecture review): after_sit_triage routes a Rung-0 "trivial green" PASS here
    # (never a genuine failure) -- give it its own reason/label rather than falling through to the
    # generic "sit_failed" (misleading: automation_result really was "passed", just untrustworthy).
    trivial_green = (state.get("automation_result") == "passed"
                      and state.get("fidelity_rung", 0) == 0)
    if state.get("quality_gate_stopped"):
        # Station 4.5. Keyed on a boolean written ONLY on the stop branch, never on
        # `quality_gate_attempts >= MAX` — that stays true for the rest of the run, so an unrelated
        # LATER stop would inherit this label. Same class of mislabelling the review_stop_* branches
        # were added to fix, inverted. It sits here, after the early-RETURN branches above
        # (blocked_open_questions / rejected_by_engineer) so it can never preempt those.
        reason, station = "quality_gate_exhausted", 4.5
    elif state.get("eval_stopped"):
        # D6. Keyed on `eval_stopped`, never on `eval_gap`: a gap from a BOUNCED pass stays in
        # state, so keying on it would mislabel a later, unrelated stop -- the exact class the
        # quality_gate arm above documents. Sits directly below it because the deterministic
        # gate outranks the LLM judge when both fired (see graph.after_quality_gate).
        reason, station = "eval_accuracy_failed", 4.6
    elif state.get("review_coverage_stopped"):
        # B1. Keyed on `review_coverage_stopped`, never on `review_coverage_gap` — a gap from a
        # BOUNCED pass persists in state and would mislabel a later, unrelated stop.
        reason, station = "review_coverage_gap", 5
    elif state.get("needs_onboarding"):
        reason, station = "repo_onboarding_exhausted", 6   # still unsupported after MAX_ONBOARD_ATTEMPTS
    elif trivial_green:
        # Distinguish the two things that both arrive as rung 0. "The skill never emitted the field"
        # is a contract defect in ocean-automation-testing; "the run proved nothing" is a fact about
        # this ticket. They demand different work, and sharing one label meant every operator read
        # the first as the second — with SKILL.md:689 marking the field REQUIRED and 0 of 18 recorded
        # verdicts carrying it, the first is the far likelier reading today.
        reason, station = ("sit_fidelity_not_reported" if not state.get("rung_emitted", True)
                           else "trivial_green_no_signal"), 6
    elif review_stop_blocking:
        reason, station = "review_budget_exhausted_blocking_findings", 5
    elif review_stop_no_diff:
        reason, station = "review_budget_exhausted_no_diff", 5
    elif fc == "code_fault":
        reason, station = "coding_attempts_exhausted", 6
    elif fc == "environment_failure" and state.get("preflight_failed"):
        reason, station = "environment_failure_non_retriable", 6   # resource-insufficient; retrying can't help
    elif fc == "environment_failure":
        reason, station = "environment_failure_retries_exhausted", 6
    elif fc == "could_not_verify":
        reason, station = "could_not_verify", 6
    else:
        reason, station = "sit_failed", 6
    # Finding 3 (architecture review, "Give Pipeline Memory"): record a structured lesson keyed by
    # (domain_bucket, action_sig, fail_sig) so a LATER ticket in the SAME domain can recall it before
    # starting fresh (see researcher()'s recall_lessons() call) instead of re-discovering the exact
    # same failure pattern from zero every time. Best-effort -- never blocks or affects this run's
    # own outcome; only feeds a future one.
    lessons.record_failure(
        domain_bucket=state.get("domain_bucket") or "",
        action_sig=f"station_{station}",
        fail_sig=reason,
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
    )
    telemetry.station_event(state["execution_id"], station, "stop", reason=reason)
    if state.get("pr_number"):
        pr_note = f"service PR #{state['pr_number']} left draft"
    elif state.get("quality_gate_stopped"):
        pr_note = (f"no PR opened -- the deterministic quality gate still rejects the changed files "
                   f"on branch {state.get('branch')!r}; needs an engineer")
    elif state.get("review_coverage_stopped"):
        pr_note = (f"no PR opened -- {state.get('review_coverage_gap')} carry branch "
                   f"{state.get('branch')!r} but were never adversarially reviewed; reviewing only "
                   f"{state.get('review_repos_covered')} would have shipped the rest unreviewed")
    elif state.get("eval_stopped"):
        pr_note = (f"no PR opened -- the independent accuracy evaluator FAILED the diff on branch "
                   f"{state.get('branch')!r}; needs an engineer")
    elif review_stop_blocking:
        pr_note = (f"a REJECTED diff exists on branch {state.get('branch')!r} -- unresolved "
                   f"CRITICAL/MAJOR at review-budget exhaustion; no PR opened, needs a human")
    else:
        pr_note = "no PR was opened -- no diff for the reviewer to approve"
    # Don't prefix review-stage stops with "sit_failed:" -- they never reached SIT. Don't prefix a
    # trivial-green stop with it either -- the SIT genuinely passed, it just didn't prove anything.
    outcome_prefix = ("quality_gate_stopped" if state.get("quality_gate_stopped")
                       else "review_coverage_stopped" if state.get("review_coverage_stopped")
                       else "eval_accuracy_stopped" if state.get("eval_stopped")
                       else "review_stopped" if (review_stop_blocking or review_stop_no_diff)
                       else "sit_unverified" if trivial_green
                       else "sit_failed")
    return {"final_status": "failed", "ready_flipped": False,
            "final_outcome": f"{outcome_prefix}:{reason}; {pr_note}"}


# ------------------------------------------------------------------ RCA agent
async def rca_agent(state: OceanState) -> dict:
    """Run ocean-rca. Deliverable is the evidence-cited report; the outcome also
    says whether a code fix is needed. A human reviews the posted comment at rca_review_gate
    next, before the graph acts on it (diagram: RCA agent -> RCA review gate -> RCA Done |
    Fix needed -> coder). On fix_needed the RCA brief is handed to the coder."""
    telemetry.station_event(state["execution_id"], 0.1, "start", route="rca")
    v: schemas.RcaVerdict = await agents.run_agent(
        agent_md="rca-research.md",   # ocean-coding-agent worker (fk-aideveloper single source); drives the ocean-rca approach
        node="rca_agent",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Produce the ocean-rca evidence-cited report for {state['ticket_id']} following "
            f"rca-research.md EXACTLY — its Step 0 connectivity gate, rejected-hypotheses discipline, "
            f"and Step 5.5 adversarial self-critique are HARD REQUIREMENTS, not optional (an earlier "
            f"version of this instruction said '5-part report' and omitted all three, which an "
            f"architecture review flagged as the automated path silently dropping every guardrail the "
            f"skill provides on a human-run investigation — do not reintroduce that gap). "
            f"WRITE the completed **7-part report** as markdown to an absolute file path and return "
            f"that path as `report_path`: (1) Flow Analysis, (2) Hypotheses Explored & Rejected — every "
            f"plausible alternative mechanism + the SPECIFIC evidence that ruled it out, (3) Root Cause "
            f"— code-, PR-, graph-, or log-verified with a specific file/function/config cited, and "
            f"containing the REQUIRED literal line `INDEPENDENT STATUS CHECK: <the query/log check you "
            f"ran to verify the reporter's own premise + its result, or 'N/A: <why there is no "
            f"verifiable premise>'>` — the graph mechanically checks for this and REFUSES to post a "
            f"report without it (reporter-premise-acceptance hard gate), AND the REQUIRED literal "
            f"line `DISCRIMINATOR: <the check you ran that SEPARATES this root cause from its "
            f"nearest rival + its result, or 'N/A: <why only one mechanism was plausible>'>` — a "
            f"SECOND mechanically-checked gate for a DIFFERENT bias (adjacent-mechanism-confidence: "
            f"naming a real, adjacent mechanism that plausibly fits and stopping, without running "
            f"what would tell it from the mechanism actually responsible). If both hypotheses "
            f"predict the same observation it is not a discriminator; answer insufficient_evidence "
            f"rather than inventing one, "
            f"(4) Impact, (5) Fix, (6) Prevention, (7) Adversarial Self-Critique — the Step 5.5 output: "
            f"every `investigator-bias` entry from skills/ocean-rca/eval/bias-registry.json, the "
            f"specific check performed against THIS hypothesis, and the verdict. Do NOT post to Jira "
            f"yourself — the graph's rca_report step posts the report as a SINGLE Jira comment "
            f"(deterministic, one comment per investigation). "
            f"STRICT PRODUCTION SAFETY: use rca-app / fourkites MCP tools for READ/GET only; NEVER call "
            f"any create/update/delete/resolve tool against production. Then decide: "
            f"does the root cause require a code fix in an ocean repo? If yes, set fix_needed=true "
            f"and populate findings_for_coder with a concrete implementation brief (repo, file, "
            f"what to change, why). If it is working-as-expected / config / data with no code change, "
            f"set fix_needed=false. Do NOT open a PR.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.RcaVerdict,
    )
    telemetry.station_event(state["execution_id"], 0.1, "end", fix_needed=v.fix_needed)
    return {"rca_fix_needed": v.fix_needed, "rca_findings": v.findings_for_coder,
            "rca_report_path": v.report_path}


# ------------------------------------------------------------------ RCA report post (plain code, one Jira comment)
def _check_rca_report(report_text: str) -> tuple[list[str], str]:
    """Finding 3: run fk-aideveloper's `ocean-rca/tools/check_rca_report.py` against a drafted RCA
    report. Returns `(problems, unverified_reason)`.

    Exactly one of the two is ever non-empty, and the distinction is the whole point:
      * `problems` non-empty — the checker RAN and returned a verdict. Fail CLOSED: refuse to post.
      * `unverified_reason` non-empty — the checker could not run (missing, crashed, wrong version,
        timed out, unparseable output). Fail OPEN, because a broken checker must not block a
        legitimate RCA from reaching Jira — but say so, loudly, and carry the fact forward so the
        run's own summary admits the report went out UNVERIFIED.
      * both empty — the checker ran and the report passed.

    Returning a bare `[]` for both cases was the bug a review caught: only the missing-script branch
    was loud, and the other four infra failure modes (non-zero exit, no stdout, an older copy without
    `--json`, argparse exit 2, timeout) all fell into `except Exception: return []` — indistinguishable
    from "the report passed". A junk 3-section report was posted to Jira with no output at all.

    Delegates to that script rather than reimplementing the rules here, so the checker and the
    SKILL.md section list it enforces stay in one place — the same single-source discipline the
    control plane already follows for worker prompts."""
    script = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-rca" / "tools" / "check_rca_report.py"

    def _unverified(reason: str) -> tuple[list[str], str]:
        # LOUD, not silent. A judge review found this exact shape three times now: a gate that
        # quietly disables itself is worse than no gate, because the run still LOOKS gated.
        ui.milestone(f"RCA report gate DID NOT RUN — {reason}. The report will be posted UNVERIFIED. "
                     f"(checker: {script}; is FK_AIDEVELOPER_DIR on a branch carrying "
                     f"skills/ocean-rca/tools/?)")
        return [], reason

    if not script.exists():
        return _unverified("checker script not found")
    tmp = ""
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as fh:
            fh.write(report_text)
            tmp = fh.name
        proc = subprocess.run([sys.executable, str(script), tmp, "--json"],
                              capture_output=True, text=True, timeout=30)
        try:
            parsed = json.loads(proc.stdout)
        except Exception:  # noqa: BLE001
            return _unverified(f"checker produced no parseable --json output "
                               f"(exit {proc.returncode}): {(proc.stderr or proc.stdout or '')[:200]}")
        if not isinstance(parsed, dict) or "problems" not in parsed:
            return _unverified(f"checker output has no `problems` key (exit {proc.returncode}) — "
                               f"an older copy of the script without --json support?")
        return list(parsed.get("problems") or []), ""
    except subprocess.TimeoutExpired:
        return _unverified("checker timed out after 30s")
    except Exception as e:  # noqa: BLE001 — a checker hiccup must never block a real report
        return _unverified(f"checker could not be executed: {type(e).__name__}: {e}")
    finally:
        if tmp:
            try:
                Path(tmp).unlink()          # not unlink(missing_ok=) — keep this 3.7-safe
            except OSError:
                pass


async def rca_report(state: OceanState) -> dict:
    """Post the RCA worker's 7-part report to Jira as a SINGLE comment — deterministic plain code, run
    by the graph, NOT the worker. This is the control-plane's own Jira transport (`jira.py`, the same
    Bearer-token REST path used for the lifecycle transitions), so RCA posting no longer depends on the
    headless worker having an interactively-authenticated Atlassian MCP (which may be absent in headless
    runs). Best-effort: no report file / no JIRA_API_TOKEN -> silent no-op, exactly one comment at most."""
    tid = state["ticket_id"]
    telemetry.station_event(state["execution_id"], 0.12, "start")
    report_text = ""
    path = state.get("rca_report_path") or ""
    if path:
        p = Path(path)
        if p.exists():
            report_text = p.read_text(encoding="utf-8", errors="replace").strip()
    # Finding 3 / the bias registry's hard-gate proposal: MECHANICALLY verify the report carries all
    # seven Step-5 sections AND an answered `INDEPENDENT STATUS CHECK:` before it goes to Jira. The
    # escalation ladder's own definition of a hard-gate is "a structural requirement the report format
    # mechanically enforces (a required field, section, or refusal condition), not an instruction to
    # remember" — so this REFUSES rather than warns. A report that fails is left unposted with the
    # reasons surfaced, because posting an investigation that skipped its own falsification steps is
    # worse than posting nothing: it reads as complete.
    if report_text:
        gate, unverified = _check_rca_report(report_text)
    else:
        gate, unverified = ["no report file was produced"], ""
    if report_text and not gate:
        jira.comment(tid, f"🤖 Aquaman Ocean RCA:\n\n{report_text}")
    elif gate:
        for problem in gate[:4]:
            ui.milestone(f"RCA report gate: {problem}")
        telemetry.station_event(state["execution_id"], 0.12, "report_gate_failed",
                                problems="; ".join(gate)[:300])
    if unverified:
        telemetry.station_event(state["execution_id"], 0.12, "report_gate_unverified",
                                reason=unverified[:300])
    telemetry.station_event(state["execution_id"], 0.12, "end",
                            posted=bool(report_text and not gate), gate_problems=len(gate),
                            unverified=bool(unverified))
    # `rca_report_unverified` rides alongside the problems list rather than folding into it: folding
    # would fail the run closed on an INFRA fault, and a missing checker is not evidence of a bad
    # report. It must still reach the terminal summary — a report posted with its gate skipped should
    # never read as a gated one.
    return {"rca_report_gate_problems": gate, "rca_report_unverified": unverified}


# ------------------------------------------------------------------ RCA review gate (human, before acting on the RCA)
async def rca_review_gate(state: OceanState) -> dict:
    """Human review of the RCA report the graph's rca_report node already posted as a Jira comment
    (rca_agent itself never posts), before the graph acts on its own conclusion — either ending at
    the terminal report (no fix) or
    proceeding into autonomous coding (fix needed). Default ON (interrupt() + wait).
    OCEAN_PIPELINE_RCA_REVIEW_AUTO skips the pause and auto-approves, for the headless
    --rca-only control-plane flow."""
    exec_id = state["execution_id"]
    if config.RCA_REVIEW_AUTO:
        telemetry.station_event(exec_id, 0.15, "auto", decision="approve")
        return {"rca_approval_decision": "approve"}
    from langgraph.types import interrupt
    # The prompt must describe what ACTUALLY happened. It used to say "just posted as a Jira comment"
    # unconditionally — including on the path where the gate refused to post, which sent the reviewer
    # to Jira to read a comment that was never written (judge review).
    gate = state.get("rca_report_gate_problems") or []
    unverified = state.get("rca_report_unverified") or ""
    if gate:
        prompt = (f"The RCA report was NOT posted — it failed its own quality gate "
                  f"({'; '.join(str(g) for g in gate[:3])}). The draft is at "
                  f"{state.get('rca_report_path') or '(no file)'}. Resume with --approve to end the "
                  f"run as a reported failure, or --reject to stop here. Approving will NOT proceed "
                  f"to coding: the graph never acts autonomously on a report that failed its gate.")
    else:
        posted = "just posted as a Jira comment"
        if unverified:
            posted += f" — but posted UNVERIFIED, its gate did not run ({unverified})"
        prompt = (f"Review the RCA report {posted}, then resume with --approve to proceed "
                  f"(report-only if no fix was needed, or on to coding if one was) or --reject to "
                  f"stop here without acting on it.")
    decision = interrupt({
        "action": "rca_review",
        "ticket_id": state["ticket_id"],
        "fix_needed": state.get("rca_fix_needed", False),
        "report_posted": not gate,
        "gate_problems": [str(g) for g in gate],
        "prompt": prompt,
    })
    telemetry.station_event(exec_id, 0.15, "decision", decision=str(decision))
    return {"rca_approval_decision": str(decision)}


# ------------------------------------------------------------------ unsupported route (terminal)
async def unsupported_route(state: OceanState) -> dict:
    """Researcher classified a route this Ocean pipeline does not handle
    (sop / loft / ff_onboarding / unclassified). Stop cleanly instead of coding it."""
    route = state.get("route")
    telemetry.station_event(state["execution_id"], 0, "unsupported_route", route=route)
    return {"final_status": "failed",
            "final_outcome": f"unsupported route '{route}' for the isbu Ocean pipeline"}


# ------------------------------------------------------------------ RCA Done (terminal, no fix)
async def rca_done(state: OceanState) -> dict:
    # Finding 3 (judge follow-up): do NOT report a report as delivered when the gate refused to post
    # it. rca_report leaves `rca_report_gate_problems` non-empty in exactly that case, and saying
    # "delivered" there is the same class of false-completeness the RCA guardrails exist to prevent.
    gate = state.get("rca_report_gate_problems") or []
    if gate:
        # NOT "done" — this branch RETURNS final_status="failed". Sharing the phase with the posted
        # branch below made `_STATUS["done"] = "completed"` mark the station completed on a run that
        # failed, so `stations_completed_list` listed rca_router while `stations_failed_list` was
        # empty. The comment above was already about exactly this class of false completeness; the
        # telemetry status is the same claim in another column (judge review).
        telemetry.station_event(state["execution_id"], 0.1, "gate_refused", fix_needed=False,
                                report_posted=False, gate_problems=len(gate))
        return {"final_status": "failed",
                "final_outcome": (f"rca_report_NOT_delivered: the report failed its own quality gate "
                                  f"and was not posted ({gate[0]}); needs an engineer")}
    # Posted, but the gate never ran (checker missing/broken). Say so — "delivered" alone would imply
    # it cleared a verification it never faced.
    unverified = state.get("rca_report_unverified") or ""
    fix_needed = bool(state.get("rca_fix_needed"))
    telemetry.station_event(state["execution_id"], 0.1, "done", fix_needed=fix_needed,
                            report_posted=True, unverified=bool(unverified))
    if unverified:
        # Do NOT say "no code fix needed" when a fix WAS needed and was deliberately withheld —
        # after_rca_review stops an unverified RCA short of autonomous coding, and the summary has to
        # name that as a withheld fix rather than imply the RCA concluded no fix was required.
        tail = ("the fix was NOT started: coding off an unverified root cause needs an engineer"
                if fix_needed else "no code fix needed")
        return {"final_status": "rca_report",
                "final_outcome": (f"rca_report_delivered_UNVERIFIED — the report gate did not run "
                                  f"({unverified}); {tail}")}
    return {"final_status": "rca_report", "final_outcome": "rca_report_delivered (no code fix needed)"}
