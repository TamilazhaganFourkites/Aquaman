"""Aquaman batch monitor — a small local web app that runs a SEQUENTIAL batch of
ocean-pipeline tickets and shows real, live station-by-station progress.

This is NOT a second control plane and does not re-implement any pipeline logic. It
only ever does two things:
  1. Spawns `ocean-pipeline <ticket>` as a subprocess, one ticket at a time (never
     concurrently — Station 6 needs exclusive Docker/port/local-repo access, the same
     reason Aquaman's own `run-batch.sh` is sequential).
  2. Parses that subprocess's own stdout, using the exact, stable contract already
     printed by `ocean_pipeline/ui.py` (banner / station_start / step / summary) —
     no separate source of truth, no guessing at graph internals.

Run:
    cd Aquaman/monitor
    pip install -r requirements.txt
    export AQUAMAN_BIN=/absolute/path/to/Aquaman/.venv/bin/ocean-pipeline
    export AQUAMAN_DIR=/absolute/path/to/Aquaman     # optional, sets the subprocess cwd
    uvicorn app:app --port 8799
    open http://localhost:8799/
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from os import environ
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import jira_client
import store
from runtime import runtime

AQUAMAN_BIN = environ.get("AQUAMAN_BIN", "ocean-pipeline")
AQUAMAN_DIR = environ.get("AQUAMAN_DIR") or None

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
    "Coding": "coder",
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
GATE_NODES = {"qa_review_gate", "rca_review_gate", "human_gate"}

_HEADER_RE = re.compile(r"^▶\s+\d{2}:\d{2}:\d{2}\s+(.+)$")
_BANNER_EXE_RE = re.compile(r"run (EXE-[0-9a-f]+)")
_DONE_RE = re.compile(r"^\[DONE\]\s+(\S+)\s+status=(\S+)(?:\s+pr=#(\d+))?")
_FAILED_RE = re.compile(r"^\[FAILED\]\s*(.*)$")
_PAUSED_RE = re.compile(r"^\[PAUSED\]\s*(.*)$", re.S)
_RESULT_RE = re.compile(r"^\s*RESULT:\s*(\S+)")


def _parse_step_line(line: str):
    """`  {icon}  {label:<38}{elapsed:>7}   {highlight}` from ui.py::step(). Split on
    the elapsed token rather than fixed columns, so it's robust to label length."""
    s = line.strip("\n")
    if len(s) < 5 or s[2] not in ("✓", "✗"):
        return None
    icon = s[2]
    rest = s[5:]
    m = re.search(r"(\d+m\d+s|\d+s)", rest)
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

    def to_json(self) -> dict:
        return {"id": self.id, "cursor": self.cursor, "log_level": self.log_level,
                 "tickets": [t.to_json() for t in self.tickets]}


BATCHES: dict[str, Batch] = {}

# Persist + reconcile BEFORE anything else can run: a "running" row surviving from a
# prior process lifetime means that OS subprocess is definitely dead (this is a fresh
# process start) — flip it to "interrupted" now, so it never looks eternally in-flight.
store.init()
_stale = store.reconcile_stale()
if _stale:
    print(f"[monitor] flipped {_stale} stale 'running' row(s) to 'interrupted' "
          f"from a prior process lifetime", flush=True)

# GLOBAL, not per-batch: two independently-created batches (e.g. two browser tabs)
# must never run ocean-pipeline concurrently either — Station 6's Docker/port/repo
# contention is a machine-wide constraint, not a within-one-batch one.
_RUN_LOCK = asyncio.Lock()


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

    Holds _RUN_LOCK for the process's entire lifetime (spawn through wait()) so two
    ocean-pipeline invocations — even from two independently-created batches — can
    never run at once on this machine."""
    await _RUN_LOCK.acquire()
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
                                       # hours-long, real-side-effect ocean-pipeline run.
        )
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
                current_label = m.group(1)
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

            m = _RESULT_RE.match(line)
            if m and not run.final_outcome:
                run.final_outcome = line.strip()
    finally:
        # Both guarded with `is not None`: if create_subprocess_exec itself raised
        # (e.g. AQUAMAN_BIN misconfigured), neither was ever assigned — this still
        # releases the lock and lets the original exception propagate, rather than
        # masking it with a NameError on a variable that was never set.
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
        _RUN_LOCK.release()


async def _run_batch_from(batch: Batch, start: int) -> None:
    """Drive tickets[start:] in order — SEQUENTIAL, never concurrent (see module
    docstring). Stops the moment a ticket pauses at a gate; `/resume` restarts this
    same function from that ticket's index once a decision is submitted."""
    for i in range(start, len(batch.tickets)):
        batch.cursor = i
        run = batch.tickets[i]
        run.status = "running"
        run.started_at = time.time()
        store.save_batch(batch)
        store.save_ticket(batch.id, i, run)
        await _drive_process(run, [run.ticket, "--log-level", batch.log_level], batch.env_overrides,
                              batch=batch)
        if run.status == "paused":
            return
    batch.cursor = len(batch.tickets)
    store.save_batch(batch)


async def _resume_ticket(batch: Batch, run: TicketRun, args: list[str]) -> None:
    """Resume path for a MANUAL batch only — after this ticket finishes, keep driving
    the rest of the batch in order. NOT used for the auto-queue (see
    _resume_auto_ticket): the auto-queue's own _auto_worker already owns pacing the
    next queued ticket on its 5s poll, respecting runtime.paused; chaining straight
    into _run_batch_from here would blast through every remaining auto-queued ticket
    in one shot regardless of pause state."""
    run.paused_gate = None
    run.paused_message = None
    await _drive_process(run, args, batch.env_overrides, batch=batch)
    if run.status == "paused":
        return
    # Identity lookup, not `.index(run)`: TicketRun is a plain dataclass, so `==`
    # compares field VALUES — two tickets with the same id submitted twice in one
    # batch would otherwise resolve to whichever occurs first, not this actual run.
    idx = next(i for i, t in enumerate(batch.tickets) if t is run)
    await _run_batch_from(batch, idx + 1)


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
# tickets to, and _auto_worker drains one at a time — through the EXACT SAME
# _drive_process function (and therefore the same _RUN_LOCK) manual batches use, so
# an auto-discovered ticket and a manual batch can never run concurrently; whichever
# gets to _RUN_LOCK.acquire() first simply makes the other wait its turn. Both
# background tasks default OFF (runtime.paused=True) — real git/PR/Jira side effects
# must never fire unattended without an explicit human Resume.
_auto_batch = Batch(id="auto", tickets=[], log_level="team", created_at=time.time())


async def _auto_worker() -> None:
    """Forever: if not paused and the auto-queue has a still-queued ticket at its
    front, drive it. Wrapped in try/except so one bad tick (e.g. a transient
    exception from _drive_process) can never permanently kill background draining —
    matches _discovery_loop's own resilience below."""
    while True:
        try:
            if not runtime.paused:
                for i, run in enumerate(_auto_batch.tickets):
                    if run.status == "queued":
                        run.status = "running"
                        run.started_at = time.time()
                        store.save_ticket("auto", i, run)
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


class ResumeBody(BaseModel):
    decision: str          # "approve" | "reject" | "approve_testrail" | "approve_no_testrail" | "changes"
    note: str = ""


def _flag(on: bool) -> str:
    return "1" if on else ""


@app.post("/batches")
async def create_batch(body: CreateBatchBody) -> dict:
    tickets = [t.strip() for t in body.tickets if t.strip()]
    if not tickets:
        raise HTTPException(422, "provide at least one ticket id")
    env_overrides = {
        "OCEAN_PIPELINE_QA_AUTOAPPROVE": _flag(body.qa_autoapprove),
        "OCEAN_PIPELINE_RCA_REVIEW_AUTO": _flag(body.rca_review_auto),
        "OCEAN_PIPELINE_TESTRAIL": _flag(body.qa_testrail),
    }
    level = body.log_level if body.log_level in ("management", "team", "developer") else "team"
    batch = Batch(id=str(uuid.uuid4())[:8], tickets=[TicketRun(ticket=t) for t in tickets],
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


@app.post("/batches/{batch_id}/resume")
async def resume_batch(batch_id: str, body: ResumeBody) -> dict:
    # "auto" resolves to the auto-queue singleton — it's never registered in BATCHES
    # (that dict is manual batches only), but the UI's unified Live view shows gate
    # controls for an auto-picked ticket exactly like a manual one, so this route must
    # accept its batch id too.
    batch = BATCHES.get(batch_id) or (_auto_batch if batch_id == "auto" else None)
    if not batch:
        raise HTTPException(404, "batch not found")
    # Scan for the paused ticket rather than trust batch.cursor: for a manual batch
    # cursor always matches (set right before driving each ticket, per _run_batch_from),
    # but _auto_batch never maintains a cursor at all (_auto_worker just iterates
    # looking for "queued") — batch.cursor there is always its dataclass default, so
    # indexing by it would silently grab the wrong ticket (or none) whenever the
    # actually-paused entry isn't at index 0. At most one ticket in either kind of
    # batch is ever "paused" at a time, so a scan is unambiguous and correct for both.
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
    else:
        args += ["--approve" if body.decision == "approve" else "--reject"]

    # Set synchronously, BEFORE scheduling the task: asyncio.create_task only
    # SCHEDULES _resume_ticket, it doesn't run it — a second /resume click arriving
    # before the task gets its first chance to run would otherwise still see
    # status == "paused" and spawn a second concurrent `--resume` for the same ticket.
    run.status = "running"
    idx_now = _resolve_idx(batch, run)
    if idx_now is not None:
        store.save_ticket(batch.id, idx_now, run)
    if batch is _auto_batch:
        asyncio.create_task(_resume_auto_ticket(run, args))
    else:
        asyncio.create_task(_resume_ticket(batch, run, args))
    return {"resumed": run.ticket, "args": args}


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


@app.get("/auto")
async def auto_queue() -> dict:
    return {"paused": runtime.paused, "paused_reason": runtime.paused_reason,
            **_auto_batch.to_json()}


@app.post("/auto/{ticket}/dismiss")
async def dismiss_auto_ticket(ticket: str) -> dict:
    """Remove a TERMINAL (done/failed/interrupted) entry from the live auto-queue —
    never removes a queued/running ticket, so this can't be used to skip work. Only
    the FIRST matching terminal entry is removed if the same ticket id appears more
    than once. Re-persists the whole remaining queue with fresh sequential indices,
    since removing an entry shifts every later index down by one."""
    idx_to_remove = next(
        (i for i, t in enumerate(_auto_batch.tickets)
         if t.ticket == ticket and t.status in ("done", "failed", "interrupted")),
        None,
    )
    if idx_to_remove is None:
        raise HTTPException(404, "no terminal ticket with that id in the auto-queue")
    _auto_batch.tickets.pop(idx_to_remove)
    store.clear_batch_tickets("auto")
    for i, t in enumerate(_auto_batch.tickets):
        store.save_ticket("auto", i, t)
    return {"removed": ticket}


@app.get("/control/status")
async def control_status() -> dict:
    return {"paused": runtime.paused, "paused_reason": runtime.paused_reason}


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
