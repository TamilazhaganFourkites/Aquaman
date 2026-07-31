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
import re
import time
import uuid
from dataclasses import dataclass, field
from os import environ
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

AQUAMAN_BIN = environ.get("AQUAMAN_BIN", "ocean-pipeline")
AQUAMAN_DIR = environ.get("AQUAMAN_DIR") or None

LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

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
_CLI_NOTE_RE = re.compile(r"^\[ocean-pipeline\]")  # e.g. the Langfuse-tracing startup line
_DONE_RE = re.compile(r"^\[DONE\]\s+(\S+)\s+status=(\S+)(?:\s+pr=#(\d+))?")
_FAILED_RE = re.compile(r"^\[FAILED\]\s*(.*)$")
_PAUSED_RE = re.compile(r"^\[PAUSED\]\s*(.*)$", re.S)
_RESULT_RE = re.compile(r"^\s*RESULT:\s*(\S+)")

# Confirmed against a real captured `--verbose` transcript (Aquaman/run.log, MM-14472):
# at developer level there are actually THREE distinct line shapes, not two — station
# headers/steps (ui.py, team+), curated milestones "     · text" (ui.py, developer-only),
# and a third, separate raw-agent-firehose prefix "    [stationname] ..." that ui.py does
# NOT print at all (it comes from agents.py's own streaming, not ui.py) — none of the
# patterns below match that third shape, so it's correctly excluded without special-casing it.


def _is_team_level_line(line: str) -> bool:
    """True for exactly the lines ui.py prints at team level or above (banner, the
    ocean-pipeline startup note, station headers, step outcomes, [DONE]/[FAILED]/
    [PAUSED], RESULT) — false for developer-only lines (milestones, the raw per-agent
    firehose). Used to filter what the UI sees; the terminal/log file always get every
    line regardless of this filter."""
    return bool(
        _BANNER_EXE_RE.search(line)
        or _CLI_NOTE_RE.match(line)
        or _HEADER_RE.match(line)
        or _parse_step_line(line)
        or _DONE_RE.match(line)
        or _FAILED_RE.match(line)
        or _PAUSED_RE.match(line)
        or _RESULT_RE.match(line)
    )


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
            # bounded tail, not the full history — raw_lines is already team-level-only
            # (see _is_team_level_line), so this is mostly a defensive cap, not a
            # frequently-hit limit; the full verbose stream lives at log_path.
            "raw_tail": self.raw_lines[-500:],
        }


@dataclass
class Batch:
    id: str
    tickets: list[TicketRun]
    cursor: int = 0
    runner_task: asyncio.Task | None = None

    def to_json(self) -> dict:
        return {"id": self.id, "cursor": self.cursor,
                 "tickets": [t.to_json() for t in self.tickets]}


BATCHES: dict[str, Batch] = {}

# GLOBAL, not per-batch: two independently-created batches (e.g. two browser tabs)
# must never run ocean-pipeline concurrently either — Station 6's Docker/port/repo
# contention is a machine-wide constraint, not a within-one-batch one.
_RUN_LOCK = asyncio.Lock()


async def _drive_process(run: TicketRun, args: list[str]) -> None:
    """Spawn one `ocean-pipeline` invocation and update `run` live as its stdout
    streams in. Returns when the process exits — either finished (done/failed) or
    paused at a gate (the process itself exits in that case; resuming means spawning
    a brand-new `ocean-pipeline --resume ...` process, exactly as the CLI documents).

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
    try:
        proc = await asyncio.create_subprocess_exec(
            AQUAMAN_BIN, *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            cwd=AQUAMAN_DIR,
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
            # UI: team-level lines only — milestones and the raw per-agent firehose
            # (developer-only) never reach run.raw_lines, hence never reach the browser.
            # (Team-level output is sparse — ~6/209 lines in a real captured run — so
            # this rarely grows large, but a heavily-retried run shouldn't grow it
            # unbounded either.)
            if _is_team_level_line(line):
                run.raw_lines.append(line)
                if len(run.raw_lines) > 2000:
                    del run.raw_lines[:-2000]
            if not line.strip():
                continue

            if run.execution_id is None:
                m = _BANNER_EXE_RE.search(line)
                if m:
                    run.execution_id = m.group(1)

            m = _HEADER_RE.match(line)
            if m:
                current_label = m.group(1)
                node = LABEL_TO_NODE.get(current_label, current_label)
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
        # Always spawn verbose (developer-level): the terminal/log file get the full
        # firehose; _is_team_level_line() filters what actually reaches the UI.
        await _drive_process(run, [run.ticket, "--log-level", "developer"])
        if run.status == "paused":
            return
    batch.cursor = len(batch.tickets)


async def _resume_ticket(batch: Batch, run: TicketRun, args: list[str]) -> None:
    run.paused_gate = None
    run.paused_message = None
    await _drive_process(run, args)
    if run.status == "paused":
        return
    # Identity lookup, not `.index(run)`: TicketRun is a plain dataclass, so `==`
    # compares field VALUES — two tickets with the same id submitted twice in one
    # batch would otherwise resolve to whichever occurs first, not this actual run.
    idx = next(i for i, t in enumerate(batch.tickets) if t is run)
    await _run_batch_from(batch, idx + 1)


app = FastAPI(title="Aquaman Batch Monitor")


class CreateBatchBody(BaseModel):
    tickets: list[str]


class ResumeBody(BaseModel):
    decision: str          # "approve" | "reject" | "approve_testrail" | "approve_no_testrail" | "changes"
    note: str = ""


@app.post("/batches")
async def create_batch(body: CreateBatchBody) -> dict:
    tickets = [t.strip() for t in body.tickets if t.strip()]
    if not tickets:
        raise HTTPException(422, "provide at least one ticket id")
    batch = Batch(id=str(uuid.uuid4())[:8], tickets=[TicketRun(ticket=t) for t in tickets])
    BATCHES[batch.id] = batch
    batch.runner_task = asyncio.create_task(_run_batch_from(batch, 0))
    return {"batch_id": batch.id}


@app.get("/batches/{batch_id}")
async def get_batch(batch_id: str) -> dict:
    batch = BATCHES.get(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    return batch.to_json()


@app.post("/batches/{batch_id}/resume")
async def resume_batch(batch_id: str, body: ResumeBody) -> dict:
    batch = BATCHES.get(batch_id)
    if not batch:
        raise HTTPException(404, "batch not found")
    run = batch.tickets[batch.cursor] if batch.cursor < len(batch.tickets) else None
    if not run or run.status != "paused" or not run.execution_id:
        raise HTTPException(409, "no paused ticket waiting for a decision")
    if run.paused_gate not in GATE_NODES:
        # Guards exactly the risk cli.py's own _pause_message() docstring calls out:
        # the wrong resume flag for an unrecognized gate silently misroutes the resume
        # rather than erroring — so an unknown gate must fail loudly here instead.
        raise HTTPException(500, f"unrecognized gate {run.paused_gate!r} — "
                             f"add it to GATE_NODES/LABEL_TO_NODE in app.py")

    args = ["--resume", run.execution_id, "--log-level", "developer"]
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
    asyncio.create_task(_resume_ticket(batch, run, args))
    return {"resumed": run.ticket, "args": args}


_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="ui")
