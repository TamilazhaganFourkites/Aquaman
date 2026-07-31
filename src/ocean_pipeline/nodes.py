"""Graph nodes.

Two kinds of node:
  * worker nodes — telemetry -> run ONE narrow agent/skill via the Claude Agent SDK ->
    return a partial state update. The graph (graph.py) owns all sequencing/routing/loops.
  * plain-code nodes (open_pr, flip_ready) — deterministic git/gh operations run directly
    here via gitops.py, NOT delegated to an agent, so the process is exact and testable.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

from . import agents, config, gitops, jira, schemas, telemetry
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
    """<=300-token ticket summary pushed to each worker."""
    s = (
        f"Ticket: {state['ticket_id']}\n"
        f"Context: {state.get('context', '(none)')}\n"
        f"Target repos: {json.dumps(state.get('target_repos', []))}\n"
    )
    if state.get("rca_findings"):
        # RCA-originated fix: the gates + coder work from the RCA brief, not a coding-route packet.
        s += f"Origin: RCA fix. RCA brief: {json.dumps(state['rca_findings'])}\n"
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
        f"run`, `docker build`, or start your own container — copy the CURRENT code in and exec:\n"
        f"    docker cp {rd}/. {name}:/app/fourkites/test/\n"
        f"    docker exec <required -e env for YOUR station: RAILS_ENV/FK_ENVIRONMENT/AWS_*/BUNDLE_GEMFILE "
        f"per local-docker-run.md §3 — unit specs use FK_ENVIRONMENT=test, SIT uses qat> {name} bash -lc "
        f"'cd /app/fourkites/test && bundle exec rspec <changed_spec_files>'\n"
        f"Re-`docker cp` after each edit (the container persists across your RED→GREEN cycles AND the "
        f"reviewer/SIT stations); batch specs into ONE `rspec` call. Only fall back to your own "
        f"`docker run` recipe if a `docker exec {name}` probe fails.\n"
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


# ------------------------------------------------------------------ Station 0
async def researcher(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 0, "start")
    jira.transition(state["ticket_id"], "In Progress")   # best-effort; no-op without a Jira token
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
    telemetry.station_event(state["execution_id"], 0, "end", route=v.route,
                            domain_bucket=v.domain_bucket,
                            checkout_sync="; ".join(sync_status) or "no target repo to sync")
    return {"route": v.route, "research_packet": _load_json(v.packet_path),
            "target_repos": v.target_repos, "domain_bucket": v.domain_bucket}


# ------------------------------------------------------------------ Station 0.5 — ocean SME consult
_SME_BY_BUCKET = {
    "callback_notification": "sme-callback-notification.md",
    "load_creation": "sme-load-creation.md",
    "ocean_tracking_milestones": "sme-ocean-milestones.md",
    "ocean_data_quality": "sme-ocean-data-quality.md",
    "jt_data_quality": "sme-jt-data-quality.md",
    "event_processing_failure": "sme-event-processing-failure.md",
}


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
    tool = config.FK_AIDEVELOPER_DIR / "skills" / "ocean-qa-agent" / "tools" / "ruby_image_cache.py"
    results: list[str] = []
    for r in (state.get("target_repos") or []):
        repo = r.get("repo", "")
        name = repo.split("/")[-1]
        is_ruby = "docker" in (r.get("build_env") or "").lower() or (r.get("language") or "").lower() == "ruby"
        worktree = config.PROJECTS_ROOT / name
        if not repo or not is_ruby:
            continue
        if not tool.exists():
            results.append(f"{name}: skip (image-cache tool not found)"); continue
        if not (worktree / ".git").exists():
            results.append(f"{name}: skip (no local checkout to build from)"); continue
        try:
            env, has_token = _image_cache_env()
            proc = await asyncio.create_subprocess_exec(
                *_image_cache_argv(tool, name, worktree, has_token), env=env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            try:
                out, _ = await asyncio.wait_for(proc.communicate(), timeout=config.IMAGE_PREWARM_TIMEOUT)
            except asyncio.TimeoutError:
                proc.kill()
                results.append(f"{name}: prewarm timed out (coder will build)"); continue
            tail = ((out or b"").decode(errors="replace").strip().splitlines() or [""])[-1]
            results.append(f"{name}: {'ready' if proc.returncode == 0 else 'build failed'} ({tail[:80]})")
        except OSError as e:
            results.append(f"{name}: prewarm error ({e})")
    telemetry.station_event(exec_id, 0.6, "end", prewarm="; ".join(results) or "no ruby target to prewarm")
    return {}


# ------------------------------------------------------------------ Station 3.5 — persistent container
async def prep_container(state: OceanState) -> dict:
    """Latency #1: start ONE booted container from the pre-warmed image so the coder/reviewer/SIT
    stations reuse it (docker cp + docker exec) instead of each doing its own `docker run` +
    image/env re-derivation (D4/D5/P1). Best-effort and fallback-safe: not Ruby, no Docker, no image,
    or a start failure all leave container_ready=False and the stations use their own recipe."""
    exec_id = state["execution_id"]
    telemetry.station_event(exec_id, 3.5, "start")
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
    container = f"ocean-{name}-{exec_id}"
    try:
        # Resolve (cache-hit if prep_image already built it) the pre-warmed image tag.
        env, has_token = _image_cache_env()
        proc = await asyncio.create_subprocess_exec(
            *_image_cache_argv(tool, name, worktree, has_token), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=config.IMAGE_PREWARM_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            telemetry.station_event(exec_id, 3.5, "skip", reason="image resolve timed out")
            return {"container_ready": False}
        tag = (((out or b"").decode(errors="replace").strip().splitlines() or [""])[-1]) if proc.returncode == 0 else ""
        if not tag:
            telemetry.station_event(exec_id, 3.5, "skip", reason="image tag unresolved")
            return {"container_ready": False}
        await _docker(["rm", "-f", container])           # clear any stale same-named container
        started = await _docker(["run", "-d", "--name", container, "--entrypoint", "sleep", tag, "infinity"])
        ready = started == 0
        telemetry.station_event(exec_id, 3.5, "end",
                                container=container if ready else f"start failed (docker exit {started})")
        return {"container_name": container if ready else "", "container_ready": ready}
    except OSError as e:
        telemetry.station_event(exec_id, 3.5, "skip", reason=f"prep error ({e})")
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
    telemetry.station_event(state["execution_id"], 1, "end", blocking=v.blocking)
    return {"dependency_report": {"report_path": v.report_path, "notes": v.notes},
            "dependency_blocking": v.blocking}


# ------------------------------------------------------------------ Station 1.5
async def reachability_gate(state: OceanState) -> dict:
    telemetry.station_event(state["execution_id"], 1.5, "start")
    v: schemas.ReachabilityVerdict = await agents.run_agent(
        agent_md="reachability.md",   # ocean-coding-agent worker (fk-aideveloper single source)
        node="reachability_gate",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        task_prompt=(
            f"Execution-verify every ALREADY_MET / self-solve / blocked / cross-repo claim for "
            f"{state['ticket_id']}. Emit the binding reachability-report.json.\n\n{_summary(state)}"
        ),
        verdict_model=schemas.ReachabilityVerdict,
    )
    telemetry.station_event(state["execution_id"], 1.5, "end", blocking=v.blocking)
    return {"reachability_report": _load_json(v.report_path), "reachability_blocking": v.blocking}


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

    # The graph owns the clone location: the coder clones into a per-run workspace and works
    # there, so the reviewer and any rework pass run against the SAME tree (reuse on re-entry).
    workspace = config.workspace_dir(state["execution_id"])
    v: schemas.CoderVerdict = await agents.run_agent(
        agent_md="code.md",   # ocean-coding-agent worker (fk-aideveloper single source)
        node="coder",
        ticket_id=state["ticket_id"],
        execution_id=state["execution_id"],
        cwd=workspace,
        task_prompt=(
            f"Decompose and implement {state['ticket_id']} per FK North Star. isbu: commit + PUSH "
            f"the branch and STOP (no PR — the graph opens it).{rework}\n\n"
            f"WORKSPACE: clone the target repo into {workspace} and do all work there. If the clone "
            f"already exists (a rework pass re-enters here), reuse it — `git fetch` + checkout the "
            f"ticket branch — do NOT re-clone. Report its absolute path as repo_dir.\n\n"
            f"Binding reachability report (build what it says is NOT_YET_BUILT; do not re-litigate "
            f"its verdicts):\n{_brief(state.get('reachability_report'), limit=12000)}\n\n"
            f"Ocean SME ownership/reuse guidance:\n{_brief(state.get('sme_findings'))}\n\n"
            f"Research summary:\n{_brief(state.get('research_packet'))}\n\n"
            f"{_summary(state)}"
            f"{_container_directive(state, str(workspace))}"
        ),
        verdict_model=schemas.CoderVerdict,
    )
    telemetry.station_event(state["execution_id"], 4, "end")
    # A fresh code pass supersedes prior SIT findings; clear them once addressed.
    # Persist WHICH repo the coder pushed to + WHERE the clone lives + the PR title/body it
    # proposed, so the reviewer/rework run in the same tree and open_pr opens deterministically
    # (preserve prior values if a rework pass leaves them blank).
    return {"branch": v.branch,
            "files_changed": v.files_changed, "sit_findings": [],
            "service_repo": v.repo or state.get("service_repo", ""),
            "worktree_dir": v.repo_dir or state.get("worktree_dir", ""),
            "pr_title": v.pr_title or state.get("pr_title", ""),
            "pr_body": v.pr_body or state.get("pr_body", "")}


# ------------------------------------------------------------------ Station 5
async def harsh_reviewer(state: OceanState) -> dict:
    iteration = state.get("review_iteration", 0)
    telemetry.station_event(state["execution_id"], 5, "start", review_iteration=iteration)
    # Review in the SAME clone the coder pushed from, so `git diff` + independent test
    # re-execution see the real tree (falls back to the default cwd if unset).
    wt = state.get("worktree_dir") or ""
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
    )
    telemetry.station_event(state["execution_id"], 5, "end", verdict=v.verdict)
    return {"review_verdict": v.verdict, "review_findings": v.findings,
            "review_iteration": iteration + 1}


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
    )
    partial = _load_json(str(verdict_path))
    needs = bool(partial.get("needs_onboarding"))
    telemetry.station_event(exec_id, 6.0, "end", needs_onboarding=needs)
    return {"needs_onboarding": needs, "onboard_repo": partial.get("onboard_repo", "")}


# ------------------------------------------------------------------ Station 6b — author (draft + STOP)
async def sit_author(state: OceanState) -> dict:
    """Draft the SIT scenarios + sample test (skill Station 1) and STOP — no TestRail cases, no run.
    A human reviews the draft at qa_review_gate before anything executes or gets committed. On a
    'changes' loop-back, the reviewer's note is fed in so ocean-qa-agent revises the draft."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    it = state.get("qa_review_iteration", 0)
    telemetry.station_event(exec_id, 6.1, "start", qa_review_iteration=it)
    note = state.get("qa_note") or ""
    revise = (f"\nThe reviewer requested CHANGES to the prior draft — revise the scenarios/test to "
              f"address this feedback:\n{note}\n") if note else ""
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_author",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 1 (author) ONLY for {tid} (`--only author`), HEADLESS. "
            f"Station 0 (resolve) already ran — do NOT re-resolve. Draft the invariant-compliant SIT "
            f"scenarios + the pytest file via ocean-qa-agent with `--no-review --skip-testrail`: "
            f"AUTHOR/UPDATE the test file (reuse only if it still covers the current diff) and record the "
            f"scenarios + the test path, then STOP. Do NOT create TestRail cases and do NOT execute the "
            f"SIT — a human reviews this draft next.{revise}\n\n{_summary(state)}"
        ),
    )
    partial = _load_json(str(config.automation_verdict_path(tid)))
    telemetry.station_event(exec_id, 6.1, "end")
    return {"qa_test_path": partial.get("test_path") or partial.get("existing_test_path", ""),
            "qa_review_iteration": it + 1, "qa_note": ""}


# ------------------------------------------------------------------ Station 6b.5 — QA review gate (human 3-way)
async def qa_review_gate(state: OceanState) -> dict:
    """Human review of the drafted SIT — the same 3-way choice ocean-qa-agent offers interactively
    (approve-with-TestRail / approve-without-TestRail / changes), surfaced at the graph level so it
    works headless. Default ON (interrupt + wait). QA_REVIEW_AUTO skips the pause and auto-approves
    (with TestRail only if QA_TESTRAIL is set)."""
    exec_id = state["execution_id"]
    if config.QA_REVIEW_AUTO:
        decision = "approve_testrail" if config.QA_TESTRAIL else "approve_no_testrail"
        telemetry.station_event(exec_id, 6.15, "auto", qa_decision=decision)
        return {"qa_decision": decision, "qa_note": ""}
    from langgraph.types import interrupt
    raw = interrupt({
        "action": "qa_review",
        "ticket_id": state["ticket_id"],
        "test_path": state.get("qa_test_path"),
        "prompt": ("Review the drafted SIT scenarios + sample test, then resume with ONE of: "
                   "`--qa approve-testrail` | `--qa approve-no-testrail` | "
                   "`--qa changes --note '<feedback>'`."),
    })
    decision = raw.get("decision") if isinstance(raw, dict) else str(raw)
    note = raw.get("note", "") if isinstance(raw, dict) else ""
    telemetry.station_event(exec_id, 6.15, "decision", qa_decision=decision)
    return {"qa_decision": decision, "qa_note": note}


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
            f"native-host Ruby run as passed. Capture per-test pass/fail to reports/junit.xml. Do NOT "
            f"run Station 3 (report/verdict) — the graph's sit_triage node does that next."
            f"{retry_note}"
            f"\n\n{_summary(state)}"
            f"{_container_directive(state, state.get('worktree_dir', ''))}"
            f"{_sit_infra_directive(state)}"
        ),
    )
    telemetry.station_event(exec_id, 6.2, "end")
    return {}


# ------------------------------------------------------------------ Station 6c' — TestRail cases (parallel)
async def sit_testrail(state: OceanState) -> dict:
    """Create the TestRail cases for the approved SIT (Project 22 / Suite 197). Runs IN PARALLEL with
    sit_run — TestRail's API is slow + rate-limited, so it must not block the functional gate. Returns
    the run id via STATE (a dedicated file, not the shared verdict json) to avoid a write race with
    the concurrent sit_run/sit_triage."""
    tid, exec_id = state["ticket_id"], state["execution_id"]
    telemetry.station_event(exec_id, 6.3, "start")
    tr_path = config.artifacts_dir(exec_id) / "testrail_run.txt"
    if tr_path.exists():
        tr_path.unlink()
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_testrail",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 1b (testrail) ONLY for {tid} (`--only testrail`): "
            f"create the TestRail cases for the SIT already authored + approved via ocean-qa-agent "
            f"(Project 22 / Suite 197 — its Steps 6/6a). Do ONLY TestRail case creation "
            f"for the EXISTING authored test at {state.get('qa_test_path') or '(the ticket SIT)'} — do "
            f"NOT re-author, execute, or open any PR. This runs in parallel with the local SIT run, so "
            f"touch ONLY TestRail (respect its rate limits). Write ONLY the integer TestRail run id to "
            f"{tr_path}.\n\n{_summary(state)}"
        ),
    )
    run_id = 0
    if tr_path.exists():
        try:
            run_id = int(tr_path.read_text().strip())
        except (ValueError, OSError):
            run_id = 0
    telemetry.station_event(exec_id, 6.3, "end", testrail_run_id=run_id)
    return {"testrail_run_id": run_id}


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
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="environment_failure", preflight_short_circuit=True)
        return {"automation_result": "failed", "failure_class": "environment_failure",
                "execution_mode": "local-mock-first",
                "test_automation_pr_url": "", "sit_findings": [],
                "needs_onboarding": False, "onboard_repo": "",
                "sit_report": {"tests": [], "changed_repos": [], "dependencies": [],
                               "evidence": state.get("preflight_reason", ""), "testrail_run_id": 0,
                               "ac_coverage": []}}
    await agents.run_skill(
        skill_name="ocean-automation-testing",
        node="sit_triage",
        ticket_id=tid,
        task_prompt=(
            f"Run ocean-automation-testing Station 3 (report) ONLY for {tid} (`--only report`): parse "
            f"reports/junit.xml for the authoritative per-test pass/fail; TRIAGE any failure — test_fault "
            f"(fix + re-run, capped) vs code_fault (real defect -> findings_for_coder) vs could_not_verify. "
            f"On PASS, commit the SIT into cloudqwest/test-automation on an {tid}/… branch, open a DRAFT PR "
            f"(reuse an existing {tid} test-automation PR — do not duplicate), and set test_automation_pr_url. "
            f"Write the verdict object to {verdict_path} exactly per SKILL.md Station 3. Do NOT flip the "
            f"service PR, merge, or deploy.\n\n{_summary(state)}"
        ),
    )
    if not verdict_path.exists():
        telemetry.station_event(exec_id, 6.4, "end", automation_result="failed",
                                failure_class="could_not_verify")
        return {"automation_result": "failed", "failure_class": "could_not_verify",
                "needs_onboarding": False, "sit_report": {}, "sit_findings": [],
                "final_outcome": "sit skill wrote no verdict"}

    v = schemas.AutomationVerdict.model_validate_json(verdict_path.read_text())
    telemetry.station_event(exec_id, 6.4, "end", automation_result=v.automation_result,
                            failure_class=v.failure_class, execution_mode=v.execution_mode,
                            needs_onboarding=v.needs_onboarding)
    return {
        "automation_result": v.automation_result,
        "failure_class": v.failure_class,
        "execution_mode": v.execution_mode,
        "test_automation_pr_url": v.test_automation_pr_url,
        "sit_findings": v.findings_for_coder,
        "needs_onboarding": v.needs_onboarding,
        "onboard_repo": v.onboard_repo,
        "sit_report": {
            "tests": [t.model_dump() for t in v.tests],
            "changed_repos": [c.model_dump() for c in v.changed_repos],
            "dependencies": [d.model_dump() for d in v.dependencies],
            "evidence": v.evidence,
            # prefer the id from the parallel sit_testrail branch (via state) over the skill's verdict
            "testrail_run_id": state.get("testrail_run_id") or v.testrail_run_id,
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
    return {"coding_attempts": attempt, "review_iteration": 0, "review_findings": [],
            "env_retry_attempts": 0}


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
async def human_gate(state: OceanState) -> dict:
    """Optional human approval before the ready-flip (OCEAN_PIPELINE_REQUIRE_APPROVAL). Default OFF
    -> pass-through (auto-flip on green). When ON, interrupt() pauses the run until an engineer
    resumes with a decision (`ocean-pipeline --resume <exe> --approve|--reject`). Either way the
    pipeline still never merges or deploys — that boundary is unchanged."""
    if not config.REQUIRE_APPROVAL:
        return {}
    from langgraph.types import interrupt
    decision = interrupt({
        "action": "flip_service_pr_ready",
        "ticket_id": state["ticket_id"],
        "pr_number": state.get("pr_number"),
        "test_automation_pr_url": state.get("test_automation_pr_url"),
        "prompt": ("SIT passed. Approve flipping the service PR to ready-for-review? "
                   "Resume with --approve or --reject."),
    })
    return {"approval_decision": str(decision)}


# ------------------------------------------------------------------ ready-flip (plain code, on GREEN)
async def flip_ready(state: OceanState) -> dict:
    """On PASS: cross-link the test-automation PR into the service PR and flip the service PR to
    ready-for-review — deterministic gh, run by the graph, NOT an agent. Also moves the ticket to
    In Review + posts a PR-link comment (best-effort Jira). This is the intended AUTOMATED terminal
    action; the human boundary is merge/deploy, which the pipeline never performs."""
    telemetry.station_event(state["execution_id"], 6.5, "start")
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
            "final_outcome": f"sit_passed; service PR(s) {prs_str} ready-for-review"}


# ------------------------------------------------------------------ stop (rejected / failed / could_not_verify / budget exhausted)
async def stop_run(state: OceanState) -> dict:
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
    if state.get("needs_onboarding"):
        reason = "repo_onboarding_exhausted"   # still unsupported after MAX_ONBOARD_ATTEMPTS
    elif fc == "code_fault":
        reason = "coding_attempts_exhausted"
    elif fc == "environment_failure" and state.get("preflight_failed"):
        reason = "environment_failure_non_retriable"   # resource-insufficient; retrying can't help
    elif fc == "environment_failure":
        reason = "environment_failure_retries_exhausted"
    elif fc == "could_not_verify":
        reason = "could_not_verify"
    else:
        reason = "sit_failed"
    telemetry.station_event(state["execution_id"], 6, "stop", reason=reason)
    return {"final_status": "failed", "ready_flipped": False,
            "final_outcome": f"sit_failed:{reason}; service PR left draft"}


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
            f"Produce the ocean-rca evidence-cited report for {state['ticket_id']}. "
            f"WRITE the completed 5-part report as markdown to an absolute file path and return that "
            f"path as `report_path`: root cause, evidence (with proof — specific SigNoz/ClickHouse log "
            f"lines + the source that produced them + read-only Redshift records), affected "
            f"service/component, and recommended fix. Do NOT post to Jira yourself — the graph's "
            f"rca_report step posts the report as a SINGLE Jira comment (deterministic, one comment "
            f"per investigation). "
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
async def rca_report(state: OceanState) -> dict:
    """Post the RCA worker's 5-part report to Jira as a SINGLE comment — deterministic plain code, run
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
    if report_text:
        jira.comment(tid, f"🤖 Aquaman Ocean RCA:\n\n{report_text}")
    telemetry.station_event(state["execution_id"], 0.12, "end", posted=bool(report_text))
    return {}


# ------------------------------------------------------------------ RCA review gate (human, before acting on the RCA)
async def rca_review_gate(state: OceanState) -> dict:
    """Human review of the RCA report rca_agent already posted as a Jira comment, before the
    graph acts on its own conclusion — either ending at the terminal report (no fix) or
    proceeding into autonomous coding (fix needed). Default ON (interrupt() + wait).
    OCEAN_PIPELINE_RCA_REVIEW_AUTO skips the pause and auto-approves, for the headless
    --rca-only control-plane flow."""
    exec_id = state["execution_id"]
    if config.RCA_REVIEW_AUTO:
        telemetry.station_event(exec_id, 0.15, "auto", decision="approve")
        return {"rca_approval_decision": "approve"}
    from langgraph.types import interrupt
    decision = interrupt({
        "action": "rca_review",
        "ticket_id": state["ticket_id"],
        "fix_needed": state.get("rca_fix_needed", False),
        "prompt": ("Review the RCA report just posted as a Jira comment, then resume with "
                   "--approve to proceed (report-only if no fix was needed, or on to coding if "
                   "one was) or --reject to stop here without acting on it."),
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
    telemetry.station_event(state["execution_id"], 0.1, "done", fix_needed=False)
    return {"final_status": "rca_report", "final_outcome": "rca_report_delivered (no code fix needed)"}
