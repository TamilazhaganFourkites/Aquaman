"""Aquaman batch monitor — a small local web app that runs a batch of ocean-pipeline
tickets CONCURRENTLY (up to AQUAMAN_MAX_CONCURRENT at once, default 3) and shows real,
live station-by-station progress for each.

This is NOT a second control plane and does not re-implement any pipeline logic. It
only ever does two things:
  1. Spawns `ocean-pipeline <ticket>` as a subprocess — up to AQUAMAN_MAX_CONCURRENT at
     once (a shared asyncio.Semaphore, _RUN_SEMAPHORE below). This USED to be strictly
     one-at-a-time: Station 6 (local SIT) binds fixed ports (ocean-service :5050,
     MockServer :1080, Postgres :5432, Kafka :9092) and a shared local repo checkout, a
     genuine collision risk with no protection outside this app. It no longer needs to
     be — `ocean_pipeline` itself now carries machine-wide protection for exactly this
     (a flock-based SIT-stage slot, `MAX_CONCURRENT_SIT`, default 1; and a build-slot for
     prep_image/prep_container/coder/harsh_reviewer/reachability_gate, `MAX_CONCURRENT_
     BUILDS`, default 2) — so multiple tickets' early stages (research, coding, review)
     run genuinely concurrently, their Docker-heavy build stages queue safely behind
     each other 2-at-a-time, and at most one is ever actually inside Station 6/SIT at
     once, machine-wide, regardless of how many tickets this app is driving. Aquaman's
     own `run-batch.sh` CLI script is still deliberately sequential (a design choice
     there, not a limitation here).
  2. Parses that subprocess's own stdout, using the exact, stable contract already
     printed by `ocean_pipeline/ui.py` (banner / station_start / step / summary) —
     no separate source of truth, no guessing at graph internals.

Run:
    cd Aquaman/monitor
    pip install -r requirements.txt
    export AQUAMAN_BIN=/absolute/path/to/Aquaman/.venv/bin/ocean-pipeline
    export AQUAMAN_DIR=/absolute/path/to/Aquaman     # optional, sets the subprocess cwd
    export AQUAMAN_MAX_CONCURRENT=3                  # optional, defaults to 3
    uvicorn app:app --port 8799
    open http://localhost:8799/
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import shutil
import signal
import time
import uuid
from dataclasses import dataclass, field
from os import environ
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import jira_client
import store
from runtime import runtime

AQUAMAN_BIN = environ.get("AQUAMAN_BIN", "ocean-pipeline")
AQUAMAN_DIR = environ.get("AQUAMAN_DIR") or None
MAX_CONCURRENT_TICKETS = int(environ.get("AQUAMAN_MAX_CONCURRENT", "3"))
# I2 recurrence (run-monitoring-findings-06af6088.md): the existing quota_exhausted classification +
# fail-fast (agents.py) stops WASTEFUL IN-PROCESS retries, but does nothing about a human (or a
# script) repeatedly clicking retry on a ticket whose last failure was the org spend limit -- a real
# batch burned ~40+ hits over ~1h this way, each retry re-entering the SAME node fresh only to hit
# the still-dry pool again seconds later. This cooldown makes a blind rapid retry require an
# explicit override instead of silently repeating the exact same failure.
QUOTA_RETRY_COOLDOWN_SECONDS = int(environ.get("AQUAMAN_QUOTA_RETRY_COOLDOWN_SECONDS", "600"))

LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)


class _QuietSuccessAccessLog(logging.Filter):
    """The frontend polls GET /batches/{id} every 2s — uvicorn's default access log
    prints a line for every one of those, which drowns out anything worth actually
    reading. Suppress only 2xx; 4xx/5xx (a real failure, a stale batch id, etc.)
    still print, since those are exactly what's worth seeing in the terminal."""
    def filter(self, record: logging.LogRecord) -> bool:
        status = record.args[-1] if record.args else None
        return not (isinstance(status, int) and 200 <= status < 300)


logging.getLogger("uvicorn.access").addFilter(_QuietSuccessAccessLog())

# Mirrors ocean_pipeline/ui.py::_LABELS verbatim (label text -> node id) so a parsed
# console line can be placed onto a station id. Keep this in sync if ui.py's own
# _LABELS dict changes — it is the single source of truth this file is deliberately
# NOT re-implementing, only reading the printed output of.
LABEL_TO_NODE = {
    "Research & routing": "researcher",
    "Ocean SME consult": "sme_consult",
    "Root-cause analysis (RCA)": "rca_agent",
    "RCA review — awaiting approval": "rca_review_gate",
    "RCA report delivered": "rca_done",
    "Unsupported ticket — stopped": "unsupported_route",
    "Dependency resolution": "dep_resolver",
    "Reachability verification": "reachability_gate",
    "Open questions — awaiting approval": "blocked_review_gate",
    "GAN-hardened test scenarios": "qa_scenarios",
    "Coding": "coder",
    "Static quality gate": "quality_gate",
    "Adversarial code review": "harsh_reviewer",
    "Open draft PR": "open_pr",
    "Local SIT — resolve & gate": "sit_resolve",
    "Local SIT — draft test": "sit_author",
    "QA review — awaiting approval": "qa_review_gate",
    "Local SIT — run": "sit_run",
    "TestRail — writing cases": "sit_testrail",
    "Pre-warming the Ruby image": "prep_image",
    "Starting the shared test container": "prep_container",
    "Removing the shared test container": "teardown_container",
    "Local SIT — triage & verdict": "sit_triage",
    "Onboarding an unsupported repo": "learn_repo",
    "Rework — SIT found a defect": "prep_rework",
    "Retry — SIT hit an environment issue": "prep_env_retry",
    "Awaiting human approval": "human_gate",
    "Flip PR to ready-for-review": "flip_ready",
    "Stopped — needs an engineer": "stop_run",
}
GATE_NODES = {"qa_review_gate", "rca_review_gate", "human_gate", "blocked_review_gate"}

_HEADER_RE = re.compile(r"^▶\s+\d{2}:\d{2}:\d{2}\s+(.+)$")
_BANNER_EXE_RE = re.compile(r"run (EXE-[0-9a-f]+)")
_DONE_RE = re.compile(r"^\[DONE\]\s+(\S+)\s+status=(\S+)(?:\s+pr=#(\d+))?")
_FAILED_RE = re.compile(r"^\[FAILED\]\s*(.*)$")
_PAUSED_RE = re.compile(r"^\[PAUSED\]\s*(.*)$", re.S)
# MM-14793 (I2): distinct from [FAILED] — the org's Claude Code spend pool ran dry, not a real
# code/env/transport failure (run-monitoring-findings.md). cli.py prints this instead of [FAILED].
_QUOTA_RE = re.compile(r"^\[QUOTA_EXHAUSTED\]\s*(.*)$", re.S)
_RESULT_RE = re.compile(r"^\s*RESULT:\s*(\S+)")


def _parse_step_line(line: str):
    """`  {icon}  {label:<38}{elapsed:>7}   {highlight}` from ui.py::step(). Split on
    the elapsed token rather than fixed columns, so it's robust to label length."""
    s = line.strip("\n")
    if len(s) < 5 or s[2] not in ("✓", "✗"):
        return None
    icon = s[2]
    rest = s[5:]
    # ui.py::_fmt_elapsed puts a space between the minutes and seconds tokens ("20m 03s") — \s?
    # tolerates it (and the older no-space form, for any already-running process still printing it).
    m = re.search(r"(\d+m\s?\d+s|\d+s)", rest)
    if not m:
        return None
    label = rest[: m.start()].rstrip()
    highlight = rest[m.end():].strip()
    return icon, label, m.group(1), highlight


@dataclass
class StepEvent:
    node: str
    label: str
    state: str  # "running" | "ok" | "crit"
    elapsed: str | None = None
    highlight: str | None = None


@dataclass
class TicketRun:
    ticket: str
    status: str = "queued"  # queued | running | paused | done | failed
    execution_id: str | None = None
    events: list[StepEvent] = field(default_factory=list)
    paused_gate: str | None = None
    paused_message: str | None = None
    final_status: str | None = None
    final_outcome: str | None = None
    pr_number: int | None = None
    started_at: float | None = None
    finished_at: float | None = None
    log_path: str | None = None
    raw_lines: list[str] = field(default_factory=list)
    # Free-form --context forwarded to ocean-pipeline on the initial fresh dispatch only (resolved
    # per-ticket in create_batch: an explicit ticket_contexts[ticket] override wins over the batch's
    # own default context). Deliberately NOT persisted to sqlite (store.py) -- it only matters at the
    # moment _run_batch_from builds this ticket's CLI args, and a resumed/reconciled run always uses
    # `--resume`, which cli.py never reads --context for anyway, so nothing is lost across a restart.
    context: str = ""

    def to_json(self) -> dict:
        return {
            "ticket": self.ticket,
            "status": self.status,
            "execution_id": self.execution_id,
            "events": [e.__dict__ for e in self.events],
            "paused_gate": self.paused_gate,
            "paused_message": self.paused_message,
            "final_status": self.final_status,
            "final_outcome": self.final_outcome,
            "pr_number": self.pr_number,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "log_path": self.log_path,
            "context": self.context,
            # bounded tail, not the full history — this is exactly what the subprocess
            # printed at the batch's chosen --log-level (nothing filtered client-side);
            # the cap is defensive since a developer-level run can print thousands of
            # lines, not something normally hit at management/team level.
            "raw_tail": self.raw_lines[-500:],
        }


@dataclass
class Batch:
    id: str
    tickets: list[TicketRun]
    log_level: str = "team"  # management | team | developer — ocean-pipeline's own --log-level
    cursor: int = 0
    created_at: float = 0.0
    runner_task: asyncio.Task | None = None
    # Explicit per-batch overrides for the three gate-auto env vars (config.py),
    # keyed by the real OCEAN_PIPELINE_* name -> "1" or "" — always set to one of
    # these two, never omitted, so the checkbox state wins regardless of whatever
    # is already exported in the shell that launched uvicorn.
    env_overrides: dict[str, str] = field(default_factory=dict)
    # "Stop batch" control signal — in-memory/live-session only, never persisted (like
    # runner_task): _run_batch_from checks this before starting each NEXT ticket. It does NOT
    # interrupt whichever ticket is already mid-drive — /batches/{id}/stop kills that one
    # explicitly too, via the same mechanism as a per-ticket Kill.
    cancelled: bool = False

    def to_json(self) -> dict:
        return {"id": self.id, "cursor": self.cursor, "log_level": self.log_level,
                 "cancelled": self.cancelled, "tickets": [t.to_json() for t in self.tickets]}


BATCHES: dict[str, Batch] = {}

# Persist + reconcile BEFORE anything else can run: a "running" row surviving from a
# prior process lifetime means that OS subprocess is definitely dead (this is a fresh
# process start) — flip it to "interrupted" now, so it never looks eternally in-flight.
store.init()
_stale = store.reconcile_stale()
if _stale:
    print(f"[monitor] flipped {_stale} stale 'running' row(s) to 'interrupted' "
          f"from a prior process lifetime", flush=True)

# GLOBAL, not per-batch, but per-PROCESS ONLY: an asyncio.Semaphore lives in this process's memory,
# so it bounds how many ocean-pipeline subprocesses THIS monitor process ever has in flight at once
# (manual batches and the auto-queue share the same budget) — real N-way concurrency, not the bare
# mutex this used to be. It provides ZERO coordination across separate monitor processes (e.g. 3
# monitors on 3 ports) — it structurally cannot, since each process gets its own independent
# semaphore object. That's fine: the actual machine-wide protection against Station 6's Docker/
# port/repo contention now lives in ocean_pipeline itself (a flock-based SIT-stage slot,
# MAX_CONCURRENT_SIT, plus a build-slot for prep_image/prep_container/coder/harsh_reviewer/
# reachability_gate, MAX_CONCURRENT_BUILDS — both cross-process by construction since they
# coordinate via lock FILES, not in-memory state). This semaphore is just an admission cap on top
# of that — how many tickets THIS process tries to run at once — not a safety mechanism itself.
_RUN_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_TICKETS)

# Tracks every currently-live `ocean-pipeline` subprocess, keyed by ticket, so any one of them can
# be force-killed from the UI independently of the others — there was previously no way to do this
# short of killing the whole monitor process. With real concurrency, MULTIPLE entries here are
# meaningfully live at once (up to MAX_CONCURRENT_TICKETS); keying by ticket (not a single global
# var) is what makes that correct, not just convenient.
#
# Value is a LIST, not a single Process: create_batch rejects duplicate ticket ids WITHIN one
# batch submission, but that can't stop the same ticket from being live in a manual batch AND the
# auto-queue at once. A plain dict[str, Process] silently corrupts under that overlap — the
# second start overwrites the first's entry, and whichever process finishes first then pops the
# OTHER's still-live entry out from under it, permanently breaking Kill for it. A list per ticket
# means every concurrently-live process for that ticket stays tracked and killable regardless.
_RUNNING_PROCS: dict[str, list[asyncio.subprocess.Process]] = {}


def _resolve_idx(batch: "Batch", run: "TicketRun") -> int | None:
    """`run`'s CURRENT position within `batch.tickets`, recomputed fresh on every
    call rather than trusted from a value captured once at drive-start. The
    auto-queue's list can be mutated (discovery inserts, a dismiss) while a drive is
    still in flight — a stale captured index would then point at a DIFFERENT
    ticket's slot after such a shift, corrupting that other ticket's persisted row
    (confirmed by direct reproduction: dismissing an earlier terminal entry while a
    later one was still running left two conflicting rows for the running ticket —
    one stuck at 'running' forever, one ghost row with its real final state at the
    wrong index). Manual batches never mutate their own list mid-run, so this is a
    cheap no-op there; recomputing unconditionally makes both paths correct without
    needing to special-case which one can shift under a live drive.
    Returns None if `run` is no longer in the batch at all (e.g. dismissed)."""
    try:
        return next(i for i, t in enumerate(batch.tickets) if t is run)
    except StopIteration:
        return None


async def _drive_process(run: TicketRun, args: list[str],
                          env_overrides: dict[str, str] | None = None,
                          batch: "Batch" = None) -> None:
    """Spawn one `ocean-pipeline` invocation and update `run` live as its stdout
    streams in. Returns when the process exits — either finished (done/failed) or
    paused at a gate (the process itself exits in that case; resuming means spawning
    a brand-new `ocean-pipeline --resume ...` process, exactly as the CLI documents).

    `batch` is used ONLY to persist state via store.save_ticket() at the same points
    `run`'s in-memory fields already change — every caller (manual batches, the
    auto-queue worker, resume) supplies it. The persisted index is resolved fresh at
    each write via _resolve_idx(), not passed in statically — see that function's
    docstring for why.

    Every raw line is echoed to this app's own terminal (prefixed by ticket, so a
    resumed/second process is distinguishable from the first) AND appended to a
    per-ticket log file under `logs/`, matching run-batch.sh's own `<TICKET>-<ts>.log`
    convention — the parsed events feed the UI, but the full raw stream stays available
    for `tail -f` or a post-mortem, same as a plain terminal run would give you.

    Holds a _RUN_SEMAPHORE slot for the process's entire lifetime (spawn through wait()), so up
    to MAX_CONCURRENT_TICKETS invocations — from any mix of manual batches and the auto-queue —
    can run at once on this machine; a (MAX_CONCURRENT_TICKETS + 1)-th caller blocks right here
    until a slot frees. `run.status` flips to "running" only once a slot is actually acquired
    (not when the caller merely dispatched it) — a ticket queued behind a full semaphore
    correctly shows as still "queued" in the UI, not prematurely "running" before real work
    starts.

    Checks `batch.cancelled` right here, AFTER acquiring the slot but BEFORE spawning anything —
    this is the only point in a queued ticket's life where a Stop click can actually still take
    effect. `_run_batch_from`'s own per-ticket cancelled check happens in a tight loop with no
    `await` in it, so by the time a `/stop` request could ever be handled, every ticket in that
    batch has already been dispatched as a task — checking cancelled there alone is not enough
    for a ticket still queued behind a full semaphore. This check is what actually honors it: a
    still-queued ticket that was cancelled while waiting simply gives back its slot and returns
    without ever running, staying "queued" forever (never given a new terminal status, matching
    stop_batch's own documented design)."""
    await _RUN_SEMAPHORE.acquire()
    if batch is not None and batch.cancelled:
        _RUN_SEMAPHORE.release()
        return
    run.status = "running"
    if run.started_at is None:
        run.started_at = time.time()
    if batch is not None:
        idx_now = _resolve_idx(batch, run)
        if idx_now is not None:
            store.save_ticket(batch.id, idx_now, run)
    proc = None
    log_fh = None
    # Inherit this app's own environment (AQUAMAN_BIN's PATH, credentials, etc.) and
    # layer the batch's gate-auto overrides on top — config.py reads these via
    # os.environ.get(...) at the CHILD process's own import time, so this is the only
    # way to make the checkbox state win over whatever's exported in the parent shell.
    child_env = {**environ, **(env_overrides or {})}
    try:
        proc = await asyncio.create_subprocess_exec(
            AQUAMAN_BIN, *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            cwd=AQUAMAN_DIR, env=child_env,
            start_new_session=True,   # own process group — a Ctrl+C on uvicorn's terminal
                                       # sends SIGINT to the whole foreground group; without
                                       # this, that would ALSO interrupt the real, possibly
                                       # hours-long, real-side-effect ocean-pipeline run. It's
                                       # also exactly what makes a deliberate Kill (below) able to
                                       # take out the whole process group, not just this one PID.
        )
        _RUNNING_PROCS.setdefault(run.ticket, []).append(proc)
        if run.log_path is None:
            ts = time.strftime("%Y%m%d-%H%M%S")
            run.log_path = str(LOGS_DIR / f"{run.ticket}-{ts}.log")
        log_fh = open(run.log_path, "a", encoding="utf-8")

        current_label: str | None = None
        assert proc.stdout is not None
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            # Terminal + log file: always the FULL verbose stream, unfiltered.
            print(f"[{run.ticket}] {line}", flush=True)
            log_fh.write(line + "\n")
            log_fh.flush()
            # UI: every line the process actually prints at whatever --log-level this
            # batch chose — the subprocess itself controls verbosity now (config.py's
            # own LOG_LEVEL gating), so there's nothing left to filter client-side.
            # Capped defensively — a developer-level run can print thousands of lines.
            run.raw_lines.append(line)
            if len(run.raw_lines) > 2000:
                del run.raw_lines[:-2000]
            if not line.strip():
                continue

            if run.execution_id is None:
                m = _BANNER_EXE_RE.search(line)
                if m:
                    run.execution_id = m.group(1)
                    if batch is not None:
                        idx_now = _resolve_idx(batch, run)
                        if idx_now is not None:
                            store.save_ticket(batch.id, idx_now, run)

            m = _HEADER_RE.match(line)
            if m:
                header_label = m.group(1)
                # The node-evaluator sidecar (agents.py::evaluate_node) drives its OWN real SDK
                # session per scored node — ui.station_start() prints its header the same way any
                # real station's does, but always as the literal label "eval_<node>" (agents.py
                # passes label=node=f"eval_{node}" straight through, no LABEL_TO_NODE entry exists
                # or should exist for it). It's an advisory internal accuracy check, not a pipeline
                # stage the user needs to see as its own row — worse, its completion is printed as
                # a plain "[eval] <node>: accuracy=..." line (nodes.py::_eval_node) that matches
                # NEITHER _parse_step_line NOR this header pattern, so an event created for it here
                # would never resolve out of "running": duplicate phantom steps stuck pulsing
                # forever, one per eval'd node. Skip entirely — don't create an event, and don't
                # update current_label (a real station's own header stays authoritative for any
                # [PAUSED] line that follows, since eval_node always completes synchronously before
                # its enclosing station's own outcome line prints).
                if header_label.startswith("eval_"):
                    continue
                current_label = header_label
                node = LABEL_TO_NODE.get(current_label, current_label)
                # agents.py's own _drive_with_retry() calls ui.station_start() (this header)
                # on EVERY attempt, including transient retries (config.MAX_AGENT_RETRIES=2,
                # so up to 3 header reprints for one logical station pass) — before a single
                # outcome line eventually arrives for the pass as a whole. If this node's own
                # most recent event is still "running", this header is a retry of that SAME
                # in-flight attempt, not a new one — replace it in place so re-running doesn't
                # stack duplicate spinners that never resolve. Searched in reverse BY NODE
                # (not just events[-1]) so this stays correct if PARALLEL_ANALYSIS interleaves
                # a different node's header in between retries of this one. A genuinely new
                # pass (e.g. the code_fault rework loop re-entering `coder` after an already-
                # resolved attempt) still appends fresh, since that prior entry is ok/crit by
                # then, not "running".
                for i in range(len(run.events) - 1, -1, -1):
                    if run.events[i].node == node:
                        if run.events[i].state == "running":
                            run.events[i] = StepEvent(node=node, label=current_label, state="running")
                        else:
                            run.events.append(StepEvent(node=node, label=current_label, state="running"))
                        break
                else:
                    run.events.append(StepEvent(node=node, label=current_label, state="running"))
                continue

            parsed = _parse_step_line(line)
            if parsed:
                icon, label, elapsed, highlight = parsed
                node = LABEL_TO_NODE.get(label, label)
                # replace the matching "running" placeholder for this node, if present
                for e in reversed(run.events):
                    if e.node == node and e.state == "running":
                        e.state = "ok" if icon == "✓" else "crit"
                        e.elapsed = elapsed
                        e.highlight = highlight or None
                        break
                else:
                    run.events.append(StepEvent(node=node, label=label,
                                                 state="ok" if icon == "✓" else "crit",
                                                 elapsed=elapsed, highlight=highlight or None))
                continue

            m = _PAUSED_RE.match(line)
            if m:
                run.status = "paused"
                run.paused_message = m.group(1).strip()
                if current_label:
                    run.paused_gate = LABEL_TO_NODE.get(current_label, current_label)
                if batch is not None:
                    idx_now = _resolve_idx(batch, run)
                    if idx_now is not None:
                        store.save_ticket(batch.id, idx_now, run)
                continue

            m = _DONE_RE.match(line)
            if m:
                run.final_status = m.group(2)
                if m.group(3):
                    run.pr_number = int(m.group(3))
                continue

            m = _FAILED_RE.match(line)
            if m:
                run.final_status = run.final_status or "failed"
                run.final_outcome = run.final_outcome or m.group(1).strip()
                continue

            m = _QUOTA_RE.match(line)
            if m:
                # MM-14793 (I2): distinct from a generic failure — the org's spend pool is dry.
                # Auto-pause the auto-discovery worker so it doesn't pick up NEW tickets into the
                # same known-dry wall (mirrors runtime.py's own documented scope: this stops the
                # auto-worker from starting new work, it does NOT retroactively stop sibling
                # tickets in the SAME manual batch that are already running concurrently — those
                # were dispatched together at batch-launch time, before this line could arrive).
                run.final_status = "quota_exhausted"
                run.final_outcome = run.final_outcome or m.group(1).strip()
                if not runtime.paused:
                    runtime.paused, runtime.paused_reason = True, (
                        f"auto-paused: org monthly spend limit hit on {run.ticket} — "
                        f"resume once credits return")
                continue

            m = _RESULT_RE.match(line)
            if m and not run.final_outcome:
                run.final_outcome = line.strip()
    finally:
        # Both guarded with `is not None`: if create_subprocess_exec itself raised
        # (e.g. AQUAMAN_BIN misconfigured), neither was ever assigned — this still
        # releases the lock and lets the original exception propagate, rather than
        # masking it with a NameError on a variable that was never set.
        # Remove only THIS process's entry, not the whole key — another concurrently-live
        # process for the same ticket string (a duplicate across a manual batch and the
        # auto-queue) must stay tracked and killable.
        procs = _RUNNING_PROCS.get(run.ticket)
        if procs is not None and proc in procs:
            procs.remove(proc)
            if not procs:
                _RUNNING_PROCS.pop(run.ticket, None)
        if log_fh is not None:
            log_fh.close()
        if proc is not None:
            await proc.wait()
            if run.status != "paused":
                run.status = "done" if (run.final_status or "").lower() in ("completed",) else (
                    "failed" if run.final_status else ("done" if proc.returncode == 0 else "failed"))
                run.finished_at = time.time()
            if batch is not None:
                idx_now = _resolve_idx(batch, run)
                if idx_now is not None:
                    store.save_ticket(batch.id, idx_now, run)
        _RUN_SEMAPHORE.release()


async def _run_batch_from(batch: Batch, start: int) -> None:
    """Dispatch tickets[start:] CONCURRENTLY — one asyncio task per ticket, bounded only by
    the shared _RUN_SEMAPHORE (capacity MAX_CONCURRENT_TICKETS), not by each other. A ticket
    that pauses at a gate, fails, or is still queued behind a full semaphore never blocks or
    cancels its siblings — each one drives and persists its own status completely
    independently (see _drive_process's own docstring for the queued->running transition).
    The cancelled check here is a cheap early-out only — this loop has no `await` in it, so it
    always finishes dispatching every ticket in one uninterrupted slice before any `/stop`
    request could possibly be handled; it can never actually catch a Stop clicked mid-loop.
    The check that actually matters is inside `_drive_process` itself, right after it acquires
    a semaphore slot — that's the real point where a still-queued ticket can and does notice
    `batch.cancelled` and back out before ever spawning a process."""
    for i in range(start, len(batch.tickets)):
        if batch.cancelled:
            return
        run = batch.tickets[i]
        args = [run.ticket, "--log-level", batch.log_level]
        if run.context:
            # `--context=<value>` (single argv element), NOT `--context <value>`. As a SEPARATE
            # argument, a value argparse recognizes as an option string dies with
            # `expected one argument` and the ticket never launches — e.g. a bare "--strict-unmocked".
            # (argparse does accept a `--`-prefixed value CONTAINING a space, so the failure is
            # narrower than first described; the `=` form removes the class entirely.) A single
            # leading `-` was always fine.
            args += [f"--context={run.context}"]
        asyncio.create_task(_drive_process(run, args, batch.env_overrides, batch=batch))
    # Informational only now (no control-flow decision reads it) — "every ticket in this
    # batch has been dispatched," not "which one is currently active" (that concept no
    # longer applies once tickets run concurrently instead of one at a time).
    batch.cursor = len(batch.tickets)
    store.save_batch(batch)


async def _resume_ticket(batch: Batch, run: TicketRun, args: list[str]) -> None:
    """Resume path for a manual-batch ticket paused at a gate. Does NOT chain into driving
    any other ticket afterward — with concurrent dispatch, every other ticket in this batch
    either already started as its own task at batch-launch time or is independently paused/
    terminal; there is no longer a "next sequential ticket" to continue into. Same shape as
    _resume_auto_ticket below for exactly that reason."""
    run.paused_gate = None
    run.paused_message = None
    await _drive_process(run, args, batch.env_overrides, batch=batch)


async def _resume_auto_ticket(run: TicketRun, args: list[str]) -> None:
    """Resume path for an auto-queue ticket paused at a gate. Deliberately does NOT
    chain into driving subsequent tickets the way _resume_ticket does for a manual
    batch — _auto_worker's own poll loop already picks up the next queued ticket on
    its own cadence once this one finishes, respecting runtime.paused."""
    run.paused_gate = None
    run.paused_message = None
    await _drive_process(run, args, {}, batch=_auto_batch)


# ── auto-discovery queue ─────────────────────────────────────────────────────
# A single persistent Batch (id "auto") that the discovery loop appends newly-found
# tickets to, and _auto_worker drains one at a time from ITS OWN perspective — through the
# EXACT SAME _drive_process function (and therefore the SAME _RUN_SEMAPHORE) manual batches
# use, so an auto-discovered ticket and manual-batch tickets share one concurrency budget:
# whichever gets to _RUN_SEMAPHORE.acquire() first simply takes a slot, and everyone else
# (auto or manual) competes fairly for what's left. Both background tasks default OFF
# (runtime.paused=True) — real git/PR/Jira side effects must never fire unattended without
# an explicit human Resume.
_auto_batch = Batch(id="auto", tickets=[], log_level="team", created_at=time.time())


async def _auto_worker() -> None:
    """Forever: if not paused and the auto-queue has a still-queued, NEVER-YET-RUN ticket at
    its front, drive it — one ticket at a time from the auto-queue's OWN perspective (this loop
    never starts a second one before the first finishes), but that single in-flight auto ticket
    shares the SAME _RUN_SEMAPHORE budget as manual-batch tickets, so it may genuinely have to
    wait for a slot if manual batches are using them all. `run.status` is left "queued" here —
    _drive_process itself flips it to "running" only once it actually acquires a slot, so a
    ticket waiting behind a full semaphore shows accurately as still queued, not prematurely
    running.

    The `not run.execution_id` half of the check matters: /resume and /retry now ALSO set an
    auto ticket's status to "queued" while its own dispatch is pending (same reasoning, see
    resume_batch/retry_ticket) — but those tickets already HAVE an execution_id (required to
    resume/retry at all), and their own scheduled _resume_auto_ticket task is what must drive
    them, with `--resume <execution_id> ...` args. Without this guard, this loop would race
    that task, see the same "queued" ticket, and drive it FRESH (no --resume flag) instead —
    silently restarting it from scratch rather than resuming, and potentially double-running it.
    A genuinely fresh, never-run auto-discovered ticket never has an execution_id until its
    first real run actually starts, so this correctly only ever matches a true first dispatch.

    Wrapped in try/except so one bad tick (e.g. a transient exception from _drive_process) can
    never permanently kill background draining — matches _discovery_loop's own resilience
    below."""
    while True:
        try:
            if not runtime.paused:
                for run in _auto_batch.tickets:
                    if run.status == "queued" and not run.execution_id:
                        await _drive_process(run, [run.ticket, "--log-level", _auto_batch.log_level],
                                              {}, batch=_auto_batch)
                        break
        except Exception as e:  # noqa: BLE001 — the auto-worker must survive a bad tick
            print(f"[monitor] auto-worker error: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(5)


async def _discovery_loop() -> None:
    """Every AQUAMAN_POLL_INTERVAL_SECONDS (default 120s): if not paused, search the
    fixed JQL, skip tickets already known to the auto-queue, and insert new arrivals
    into the queue's still-PENDING tail ordered by jira_client.priority_key — never
    reordering anything already running/done. A read-only Jira search only; no label
    writes, no side effects until a ticket is actually picked up by _auto_worker."""
    interval = int(environ.get("AQUAMAN_POLL_INTERVAL_SECONDS", "120"))
    while True:
        try:
            if not runtime.paused:
                issues = jira_client.search(jira_client.DEFAULT_JQL)
                known = {t.ticket for t in _auto_batch.tickets}
                new = [i for i in issues if i["key"] not in known]
                new.sort(key=jira_client.priority_key)
                if new:
                    pending_start = next(
                        (i for i, t in enumerate(_auto_batch.tickets) if t.status == "queued"),
                        len(_auto_batch.tickets),
                    )
                    for offset, issue in enumerate(new):
                        _auto_batch.tickets.insert(pending_start + offset, TicketRun(ticket=issue["key"]))
                    for i, t in enumerate(_auto_batch.tickets):
                        store.save_ticket("auto", i, t)
        except Exception as e:  # noqa: BLE001 — discovery must survive a bad tick
            print(f"[monitor] discovery-loop error: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(interval)


@contextlib.asynccontextmanager
async def _lifespan(_app: FastAPI):
    # asyncio.create_task() at bare module-import time would silently attach to a
    # throwaway, never-run event loop (uvicorn creates its OWN loop only once it
    # actually starts serving) — verified directly: the task gets created with no
    # error, but never executes. The lifespan context manager is the correct place:
    # it runs inside uvicorn's real, running loop.
    auto_worker_task = asyncio.create_task(_auto_worker())
    discovery_task = asyncio.create_task(_discovery_loop())
    yield
    auto_worker_task.cancel()
    discovery_task.cancel()


app = FastAPI(title="Aquaman Batch Monitor", lifespan=_lifespan)


class CreateBatchBody(BaseModel):
    tickets: list[str]
    log_level: str = "team"         # management | team | developer — ocean-pipeline's own --log-level
    qa_autoapprove: bool = False    # OCEAN_PIPELINE_QA_AUTOAPPROVE — skip qa_review_gate's pause
    rca_review_auto: bool = False   # OCEAN_PIPELINE_RCA_REVIEW_AUTO — skip rca_review_gate's pause
    qa_testrail: bool = False       # OCEAN_PIPELINE_TESTRAIL — real TestRail cases on auto-approve
    context: str = ""               # batch-level default --context, applied to any ticket below
                                     # with no entry in ticket_contexts
    ticket_contexts: dict[str, str] = Field(default_factory=dict)  # per-ticket --context override,
                                     # keyed by ticket id — wins over `context` for that one ticket


class ResumeBody(BaseModel):
    decision: str          # "approve" | "reject" | "approve_testrail" | "approve_no_testrail" | "changes"
                           # | "answer" | "post" (blocked_review_gate: answer/post/reject)
    note: str = ""
    # Which paused ticket this decision is for. Now that a batch can run several tickets
    # concurrently, MORE THAN ONE can be paused at a gate in the same batch at once — "the"
    # paused ticket is no longer unambiguous. Optional only for backward compatibility: if
    # omitted, resume_batch falls back to the old single-paused-ticket scan.
    ticket: str = ""


def _flag(on: bool) -> str:
    return "1" if on else ""


@app.post("/batches")
async def create_batch(body: CreateBatchBody) -> dict:
    tickets = [t.strip() for t in body.tickets if t.strip()]
    if not tickets:
        raise HTTPException(422, "provide at least one ticket id")
    # Reject duplicates rather than silently dropping or running them: with real concurrency,
    # two TicketRun objects for the same ticket string can now be genuinely live at once, and
    # _RUNNING_PROCS (keyed by ticket string — see its own comment) can only ever track ONE
    # live process per ticket. A second concurrent dispatch of the same ticket would silently
    # overwrite the first's entry, and whichever finishes first would then pop the SECOND's
    # still-live entry out from under it — permanently breaking Kill for that ticket. Erroring
    # here (a simple paste mistake, most likely) is far better than that silent corruption.
    dupes = sorted({t for t in tickets if tickets.count(t) > 1})
    if dupes:
        raise HTTPException(422, f"duplicate ticket id(s) in this batch: {', '.join(dupes)}")
    env_overrides = {
        "OCEAN_PIPELINE_QA_AUTOAPPROVE": _flag(body.qa_autoapprove),
        "OCEAN_PIPELINE_RCA_REVIEW_AUTO": _flag(body.rca_review_auto),
        "OCEAN_PIPELINE_TESTRAIL": _flag(body.qa_testrail),
    }
    level = body.log_level if body.log_level in ("management", "team", "developer") else "team"
    default_context = body.context.strip()
    # Per-ticket override wins over the batch default; a ticket with no entry (or a
    # whitespace-only override) falls back to default_context.
    batch = Batch(
        id=str(uuid.uuid4())[:8],
        tickets=[TicketRun(ticket=t, context=body.ticket_contexts.get(t, "").strip() or default_context)
                 for t in tickets],
        log_level=level, env_overrides=env_overrides, created_at=time.time())
    BATCHES[batch.id] = batch
    store.save_batch(batch)
    for i, t in enumerate(batch.tickets):
        store.save_ticket(batch.id, i, t)
    batch.runner_task = asyncio.create_task(_run_batch_from(batch, 0))
    return {"batch_id": batch.id}


@app.get("/batches")
async def list_batches() -> dict:
    """Every batch still held in memory (this process's uptime only — nothing persists
    across a uvicorn restart), newest first. Exists so a reloaded/reopened browser tab
    can reconnect to a batch that's still actually running server-side, even though the
    frontend's own in-memory `batchId` was lost on reload."""
    batches = sorted(BATCHES.values(), key=lambda b: b.created_at, reverse=True)
    return {"batches": [
        {"id": b.id, "created_at": b.created_at,
         "tickets": [{"ticket": t.ticket, "status": t.status} for t in b.tickets]}
        for b in batches
    ]}


@app.get("/batches/{batch_id}")
async def get_batch(batch_id: str) -> dict:
    batch = BATCHES.get(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    return batch.to_json()


@app.post("/batches/{batch_id}/stop")
async def stop_batch(batch_id: str) -> dict:
    """Stop a MANUAL batch (not the auto-queue — that already has its own pause control).
    Kills EVERY currently-running ticket in the batch (plural, now that tickets run
    concurrently, not just whichever single one used to be mid-drive) via the same mechanism
    as a per-ticket Kill. Also sets batch.cancelled, which any ticket in this batch still
    queued behind a full semaphore will see and honor the moment it would otherwise acquire a
    slot (the actual check lives in `_drive_process`, right after `_RUN_SEMAPHORE.acquire()` —
    see its docstring) — so a queued ticket really is abandoned, not silently started later,
    matching what the UI's own confirm dialog promises. Queued tickets are left as "queued"
    (not given a new terminal status) — a deliberate scope call to avoid a new status value
    rippling through every pill/CSS rule for a rarely-used action; they're simply never
    dispatched for real for the life of this batch."""
    batch = BATCHES.get(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    batch.cancelled = True
    running = [t for t in batch.tickets if t.status == "running"]
    killed = [t.ticket for t in running if _kill_process(t.ticket)]
    return {"stopped": batch_id, "killed_tickets": killed}


@app.post("/batches/{batch_id}/resume")
async def resume_batch(batch_id: str, body: ResumeBody) -> dict:
    batch = _resolve_batch(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    # Prefer an exact ticket match — now that tickets in a batch can run concurrently, MORE
    # THAN ONE can be paused at a gate at the same time, so "the" paused ticket is no longer
    # unambiguous the way it was when a batch could only ever have one ticket in flight.
    # batch.cursor is never trusted for this either way: for a manual batch it's now purely
    # informational (see _run_batch_from), and _auto_batch never maintained one at all
    # (_auto_worker just iterates looking for "queued").
    if body.ticket:
        run = next((t for t in batch.tickets if t.ticket == body.ticket and t.status == "paused"), None)
    else:
        # Back-compat fallback for a caller that didn't send `ticket`: correct only when at
        # most one ticket in this batch happens to be paused right now.
        run = next((t for t in batch.tickets if t.status == "paused"), None)
    if not run or not run.execution_id:
        raise HTTPException(409, "no paused ticket waiting for a decision")
    if run.paused_gate not in GATE_NODES:
        # Guards exactly the risk cli.py's own _pause_message() docstring calls out:
        # the wrong resume flag for an unrecognized gate silently misroutes the resume
        # rather than erroring — so an unknown gate must fail loudly here instead.
        raise HTTPException(500, f"unrecognized gate {run.paused_gate!r} — "
                             f"add it to GATE_NODES/LABEL_TO_NODE in app.py")

    args = ["--resume", run.execution_id, "--log-level", batch.log_level]
    if run.paused_gate == "qa_review_gate":
        qa_map = {"approve_testrail": "approve-testrail",
                  "approve_no_testrail": "approve-no-testrail", "changes": "changes"}
        args += ["--qa", qa_map.get(body.decision, body.decision)]
        if body.note:
            args += ["--note", body.note]
    elif run.paused_gate == "blocked_review_gate":
        # MM-14816 (G20): 3-way blocked-open-questions gate. decision ∈ {answer, post, reject};
        # `answer` carries the human's answers in note. (The outward Jira post happens only on `post`.)
        args += ["--blocked", body.decision]
        if body.note:
            args += ["--note", body.note]
    else:
        args += ["--approve" if body.decision == "approve" else "--reject"]

    # Set synchronously, BEFORE scheduling the task: asyncio.create_task only SCHEDULES
    # _resume_ticket, it doesn't run it — a second /resume click arriving before the task
    # gets its first chance to run would otherwise still see status == "paused" and spawn a
    # second concurrent `--resume` for the same ticket. "queued" (not "running") is what
    # _drive_process itself will flip this to once it actually acquires a semaphore slot —
    # setting "running" here would lie about a ticket that might still be waiting behind a
    # full semaphore, and would also keep matching this same paused-ticket lookup's `status`
    # filter incorrectly. "queued" both accurately reflects "decision submitted, not yet
    # actually resumed" AND naturally blocks a double-click (a second /resume immediately
    # after this one finds no ticket with status=="paused" anymore, same 409 as normal).
    run.status = "queued"
    idx_now = _resolve_idx(batch, run)
    if idx_now is not None:
        store.save_ticket(batch.id, idx_now, run)
    if batch is _auto_batch:
        asyncio.create_task(_resume_auto_ticket(run, args))
    else:
        asyncio.create_task(_resume_ticket(batch, run, args))
    return {"resumed": run.ticket, "args": args}


def _quota_cooldown_block(final_status: str | None, finished_at: float | None, force: bool) -> str | None:
    """I2 recurrence: if the LAST attempt ended in quota_exhausted and finished less than
    QUOTA_RETRY_COOLDOWN_SECONDS ago, block a retry unless the caller explicitly passes
    ?force=true. Returns an error message to raise as a 409, or None to let the retry proceed.
    A missing/unknown finished_at (e.g. an old history row from before this field existed)
    fails OPEN (returns None) -- this is a courtesy speed-bump against blind rapid-fire retries,
    not a hard safety gate, so an inability to compute the cooldown must never itself block a
    legitimate retry."""
    if force or final_status != "quota_exhausted" or not finished_at:
        return None
    remaining = QUOTA_RETRY_COOLDOWN_SECONDS - (time.time() - finished_at)
    if remaining <= 0:
        return None
    return (f"last attempt hit the org spend limit {int(time.time() - finished_at)}s ago -- "
            f"retrying now will very likely hit the same wall (this already burned ~40+ cycles "
            f"in one batch). Wait {int(remaining)}s, or pass ?force=true to retry anyway.")


def _resolve_batch(batch_id: str) -> "Batch | None":
    """"auto" resolves to the auto-queue singleton — it's never registered in BATCHES (that dict is
    manual batches only), but the UI's unified Live view treats an auto-picked ticket like a manual
    one for retry/kill/dismiss too, so every ticket-action route needs to accept its batch id."""
    return BATCHES.get(batch_id) or (_auto_batch if batch_id == "auto" else None)


@app.post("/batches/{batch_id}/tickets/{ticket}/retry")
async def retry_ticket(batch_id: str, ticket: str, force: bool = False) -> dict:
    """Plain checkpoint resume for a FAILED/INTERRUPTED ticket — distinct from /resume above (which
    only ever answers a paused GATE decision, --approve/--reject/--qa). ocean-pipeline's own --resume
    replays from the last LangGraph checkpoint regardless of why the run stopped: a StationError
    (e.g. "no verdict written" after a killed background task) does not invalidate that checkpoint,
    so this can genuinely pick back up — the coder's own re-entry logic already checks git state for
    already-completed work — rather than starting the ticket over from scratch.

    `force=true` bypasses the quota-cooldown speed-bump below (I2 recurrence) for a deliberate
    early retry once credits are confirmed back."""
    batch = _resolve_batch(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    run = next((t for t in batch.tickets if t.ticket == ticket and t.status in ("failed", "interrupted")), None)
    if not run:
        raise HTTPException(409, "no failed/interrupted ticket with that id to retry")
    if not run.execution_id:
        raise HTTPException(409, "no execution id recorded for this ticket — nothing to resume from")
    block = _quota_cooldown_block(run.final_status, run.finished_at, force)
    if block:
        raise HTTPException(409, block)

    args = ["--resume", run.execution_id, "--log-level", batch.log_level]
    # "queued", not "running" — see resume_batch's own comment: _drive_process flips this to
    # "running" itself once it actually acquires a semaphore slot, and leaving it at "queued"
    # here also naturally blocks a double-click retry (a second click no longer matches this
    # route's failed/interrupted status filter).
    run.status = "queued"
    run.final_status = None
    run.final_outcome = None   # clear the stale failure message — a fresh one lands if this fails too
    # Only finished_at is reset (so the UI shows this as in-progress rather than frozen at the
    # old finish time) — started_at is deliberately left as-is (the original run's real start
    # time) so once this retry completes, the duration badge reflects the full original-to-
    # completion span rather than just this resume step's own few seconds. _drive_process's
    # `if started_at is None` guard leaves the preserved value alone. log_path is deliberately
    # left alone too — the retry's output appends to the same file, preserving the full history
    # of both attempts in one place, same convention as a station's own re-run logging.
    run.finished_at = None
    idx_now = _resolve_idx(batch, run)
    if idx_now is not None:
        store.save_ticket(batch.id, idx_now, run)
    if batch is _auto_batch:
        asyncio.create_task(_resume_auto_ticket(run, args))
    else:
        asyncio.create_task(_resume_ticket(batch, run, args))
    return {"retried": ticket, "args": args}


def _kill_process(ticket: str) -> bool:
    """Force-kill EVERY live subprocess tracked for `ticket` (the WHOLE process group of each —
    see kill_ticket's docstring for why), not just one — normally there's exactly one, but a
    duplicate ticket running concurrently across a manual batch and the auto-queue means there
    can legitimately be more. Returns whether at least one process was actually found;
    best-effort otherwise, shared by the per-ticket Kill route and /batches/{id}/stop's "also
    kill whatever's mid-drive"."""
    procs = _RUNNING_PROCS.get(ticket)
    if not procs:
        return False
    for proc in list(procs):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass   # already exited between the caller's own state check and this call landing
    return True


@app.post("/tickets/{ticket}/kill")
async def kill_ticket(ticket: str) -> dict:
    """Force-kill a genuinely stuck/hung ocean-pipeline subprocess for `ticket` — previously the
    only way to stop one was killing the whole monitor process. Sends SIGKILL to the WHOLE process
    GROUP (not just the direct child): _drive_process spawns with start_new_session=True specifically
    so ocean-pipeline's own real subprocesses (docker, git, the SDK's own claude CLI) live in that
    same group — killing only the direct PID would leave those orphaned and still running. Not
    batch-scoped: _RUNNING_PROCS is the single source of truth for "is this ticket's process still
    alive", independent of which batch (manual or auto) it belongs to."""
    if not _kill_process(ticket):
        raise HTTPException(404, "no running process for that ticket")
    return {"killed": ticket}


@app.post("/batches/{batch_id}/tickets/{ticket}/dismiss")
async def dismiss_ticket(batch_id: str, ticket: str) -> dict:
    """Remove a TERMINAL (done/failed/interrupted) ticket from `batch_id`'s list — works for both a
    manual batch and the auto-queue ("auto"). Never removes a genuinely queued/running/paused
    ticket, so this can't be used to skip or hide in-flight work — EXCEPT a "queued" ticket whose
    batch has been cancelled (via /stop): that one is permanently abandoned (_drive_process's own
    cancelled check guarantees it will never actually dispatch — see that function's docstring),
    so it's inert exactly like a terminal ticket, just without a status value of its own. Without
    this carve-out there would be no way to ever clear such a row from the Live view short of
    restarting the whole monitor process. `batch.cancelled` is never true for the auto-queue (Stop
    only applies to manual batches), so this carve-out is a no-op there. Only the FIRST matching
    entry is removed if the same ticket id appears more than once. Re-persists the remaining
    tickets with fresh sequential indices, since removing an entry shifts every later index down
    by one."""
    batch = _resolve_batch(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    idx_to_remove = next(
        (i for i, t in enumerate(batch.tickets)
         if t.ticket == ticket and (t.status in ("done", "failed", "interrupted")
                                     or (t.status == "queued" and batch.cancelled))),
        None,
    )
    if idx_to_remove is None:
        raise HTTPException(404, "no terminal ticket with that id in this batch")
    batch.tickets.pop(idx_to_remove)
    store.clear_batch_tickets(batch.id)
    for i, t in enumerate(batch.tickets):
        store.save_ticket(batch.id, i, t)
    return {"removed": ticket}


@app.get("/discovered")
async def discovered() -> dict:
    """On-demand peek at the fixed JQL — independent of the auto-discovery loop
    (works the same whether auto-pickup is paused or resumed). Includes each
    ticket's recent-failure count (store.recent_failures_for) as an informational
    warning, never auto-blocking."""
    jql = jira_client.DEFAULT_JQL
    issues = jira_client.search(jql)
    return {
        "jql": jql,
        "enabled": jira_client._enabled(),
        "tickets": [{**i, "recent_failures": store.recent_failures_for(i["key"])} for i in issues],
    }


@app.get("/history")
async def history(limit: int = 200) -> dict:
    return {"counts": store.counts(), "tickets": store.recent_tickets(limit)}


@app.delete("/history/{row_id}")
async def delete_history_row(row_id: int) -> dict:
    return {"deleted": store.delete_ticket(row_id)}


@app.post("/history/{row_id}/retry")
async def retry_history_ticket(row_id: int, force: bool = False) -> dict:
    """Retry a ticket from persisted History — the ONLY way to retry a failed/interrupted run once
    the monitor has restarted, since BATCHES/`_auto_batch` are in-memory only but this table
    survives. Creates a fresh single-ticket manual batch wrapping the same execution_id (a plain
    --resume checkpoint replay, same as /batches/{id}/tickets/{ticket}/retry) so the Live tab shows
    it exactly like any other manual batch. Defaults to "team" log level — the original batch's
    level isn't looked up here, matching this app's existing bias toward one simple default over
    per-call config plumbing for a rarely-used recovery path.

    `force=true` bypasses the quota-cooldown speed-bump below (I2 recurrence)."""
    row = store.get_ticket(row_id)
    if not row:
        raise HTTPException(404, "no history row with that id")
    if row["status"] not in ("failed", "interrupted"):
        raise HTTPException(409, "only a failed/interrupted history entry can be retried")
    if not row["execution_id"]:
        raise HTTPException(409, "no execution id recorded for this ticket — nothing to resume from")
    block = _quota_cooldown_block(row["final_status"], row["finished_at"], force)
    if block:
        raise HTTPException(409, block)

    # status="queued". started_at is inherited from the original row (a real past timestamp, not
    # "now") so the retried run's duration badge reflects the full original-to-completion span
    # instead of just this resume step's own few seconds — _drive_process's `if started_at is
    # None` guard leaves an already-set value alone, so this doesn't reintroduce the "queued time
    # inflates duration" problem a fresh `time.time()` stamp here would.
    run = TicketRun(ticket=row["ticket"], execution_id=row["execution_id"], status="queued",
                     started_at=row["started_at"])
    batch = Batch(id=str(uuid.uuid4())[:8], tickets=[run], log_level="team", created_at=time.time())
    BATCHES[batch.id] = batch
    store.save_batch(batch)
    store.save_ticket(batch.id, 0, run)
    args = ["--resume", row["execution_id"], "--log-level", batch.log_level]
    batch.runner_task = asyncio.create_task(_resume_ticket(batch, run, args))
    return {"batch_id": batch.id, "retried": run.ticket, "args": args}


@app.get("/auto")
async def auto_queue() -> dict:
    return {"paused": runtime.paused, "paused_reason": runtime.paused_reason,
            **_auto_batch.to_json()}


@app.get("/control/status")
async def control_status() -> dict:
    return {"paused": runtime.paused, "paused_reason": runtime.paused_reason}


# ── Environment health strip — best-effort, read-only, never fatal ──────────
async def _check_disk() -> dict:
    try:
        usage = shutil.disk_usage(str(Path(__file__).parent))
        free_gb = usage.free / (1024**3)
        pct_free = usage.free / usage.total * 100
        return {"ok": pct_free >= 10 and free_gb >= 5, "free_gb": round(free_gb, 1), "pct_free": round(pct_free, 1)}
    except Exception:  # noqa: BLE001 — health checks are best-effort, never fatal
        return {"ok": None}


async def _communicate_with_timeout(proc: asyncio.subprocess.Process, timeout: float) -> bytes:
    """proc.communicate() bounded by timeout — abandoning an asyncio.wait_for'd await does NOT
    kill the underlying OS process, so on a timeout this explicitly kills+reaps it before
    re-raising, rather than leaking an orphaned `docker` process every time a health check times
    out (this endpoint is polled every 20s from the frontend — a hung daemon would otherwise
    accumulate a new zombie/orphan every poll, indefinitely)."""
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return out
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise


_DOCKER_SIZE_RE = re.compile(r"^([\d.]+)\s*([A-Za-z]+)$")
_DOCKER_SIZE_UNITS = {"b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}


def _parse_docker_size(s: str) -> float:
    """"38.05GB" / "129.9MB" / "0B" -> GB, as a plain float. Docker's own humanized units
    (go-units), not KiB/MiB — matches what `docker system df` actually prints. Returns 0.0 for
    anything that doesn't parse rather than raising — this is display-only, best-effort data."""
    m = _DOCKER_SIZE_RE.match(s.strip())
    if not m:
        return 0.0
    value, unit = m.groups()
    bytes_ = float(value) * _DOCKER_SIZE_UNITS.get(unit.lower(), 0)
    return bytes_ / 1e9


async def _check_docker_space() -> dict | None:
    """`docker system df` — the standard "why is Docker eating my disk" command. Returns total
    space Docker is holding across images/containers/volumes/build-cache, and how much of that
    is reclaimable (safe to prune). None (not a dict) on any failure — the caller treats that as
    "no space data", not "Docker is down" (that's _check_docker's own `ok` field)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "system", "df", "--format", "{{.Size}}|{{.Reclaimable}}",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out = await _communicate_with_timeout(proc, 3.0)
        if proc.returncode != 0:
            return None
        total_gb = reclaimable_gb = 0.0
        for line in out.decode().splitlines():
            if "|" not in line:
                continue
            size_s, reclaim_s = line.split("|", 1)
            total_gb += _parse_docker_size(size_s)
            reclaimable_gb += _parse_docker_size(reclaim_s.split("(")[0])  # strip "(NN%)"
        return {"total_gb": round(total_gb, 1), "reclaimable_gb": round(reclaimable_gb, 1)}
    except Exception:  # noqa: BLE001
        return None


# `docker info`'s .Name is the actual context/VM host, not "Docker" — e.g. Rancher Desktop's
# is literally "lima-rancher-desktop" (confirmed live on this machine), Colima's contains
# "colima". Mapping it means the tooltip says what's ACTUALLY running instead of a generic
# "Docker daemon" that reads as Docker Desktop specifically when it may not be.
_DOCKER_BACKEND_HINTS = (("rancher-desktop", "Rancher Desktop"), ("colima", "Colima"))


def _docker_backend_label(name: str) -> str:
    lowered = name.lower()
    for hint, label in _DOCKER_BACKEND_HINTS:
        if hint in lowered:
            return label
    return name or "Docker"


async def _check_docker() -> dict:
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "info", "--format", "{{.ServerVersion}}|{{.Name}}",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out = await _communicate_with_timeout(proc, 2.0)
        ok = proc.returncode == 0
        version, _, name = out.decode().strip().partition("|")
        result = {"ok": ok, "version": version or None,
                   "backend": _docker_backend_label(name) if ok else None}
        if ok:
            # Best-effort: a space-check failure never demotes the daemon-reachable verdict —
            # ok stays True, space fields just stay absent (frontend treats that as "unknown").
            space = await _check_docker_space()
            if space is not None:
                result["total_gb"] = space["total_gb"]
                result["reclaimable_gb"] = space["reclaimable_gb"]
                # 10GB reclaimable is the "you should probably prune" line for a dev laptop —
                # not a hard ceiling, just a nudge surfaced as an amber dot instead of green.
                result["space_warn"] = space["reclaimable_gb"] >= 10
        return result
    except Exception:  # noqa: BLE001
        return {"ok": False}


async def _check_reachability() -> dict:
    # Reuses jira_client's own JIRA_BASE_URL host — the only "internal host" this app already
    # has a real reason to reach. A plain TCP connect (not a full HTTPS GET) is the cheapest
    # honest signal of "is the network path open".
    host = jira_client.JIRA_BASE_URL.split("//", 1)[-1].split("/", 1)[0]
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, 443), timeout=1.5)
        writer.close()
        await writer.wait_closed()
        return {"ok": True, "host": host}
    except Exception:  # noqa: BLE001
        return {"ok": False, "host": host}


@app.get("/health/env")
async def health_env() -> dict:
    """Best-effort, read-only environment probes for the header health strip. Every check is
    independently guarded and all three run CONCURRENTLY (not sequentially) so total latency is
    bounded by the single slowest check, not their sum; an outer wait_for is a defense-in-depth
    ceiling so a pathological hang anywhere still returns with whatever finished. _check_docker
    itself does two sequential calls (daemon version, then `system df` for space) — up to ~5s
    worst case — so the ceiling here is 6s, not the 3s a single-call check would need."""
    try:
        disk, docker, net = await asyncio.wait_for(
            asyncio.gather(_check_disk(), _check_docker(), _check_reachability(),
                            return_exceptions=True),
            timeout=6.0,
        )
    except asyncio.TimeoutError:
        disk, docker, net = {"ok": None}, {"ok": None}, {"ok": None}
    disk = disk if isinstance(disk, dict) else {"ok": None}
    docker = docker if isinstance(docker, dict) else {"ok": None}
    net = net if isinstance(net, dict) else {"ok": None}
    return {"disk": disk, "docker": docker, "reachability": net}


@app.post("/control/pause")
async def control_pause(reason: str = "operator") -> dict:
    runtime.paused, runtime.paused_reason = True, reason
    return {"paused": True, "paused_reason": reason}


@app.post("/control/resume")
async def control_resume() -> dict:
    runtime.paused, runtime.paused_reason = False, ""
    return {"paused": False}


_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="ui")
