"""E2 — durable, out-of-process markers for the human gates.

Adopted from AM (`fk-aideveloper` PR#320, `skills/fk-execute/SKILL.md:313-316` on
`origin/fk-aideveloper-am`), which writes `FK-DESIGN: awaiting-approval|approved` and three
siblings. The adoption review (`important-notes/ocean-vs-DY-AM-what-to-adopt.md`) records that an
earlier draft dismissed these as "only the marker names" and took the taxonomy instead — backwards:
**the durable external state is the valuable half.**

WHY OCEAN NEEDS IT. Ocean's gate state lives ONLY inside the LangGraph SQLite checkpoint. Nothing
outside the process can see that a run is waiting, which gate it is waiting at, or which flags would
answer it — you have to attach to the run's own stdout, or open the checkpoint DB and know its
schema. That blindness is what makes a bare `--resume EXE` and a wrong-gate
`--resume EXE --blocked reject` indistinguishable to an operator, which is the C6 defect one level
up: the CLI now refuses a wrong-gate flag, but the operator still had no way to know which flag was
right without the run telling them.

A flat directory, one file per waiting run, so the question a supervisor actually asks —
"what is blocked on a human right now?" — is an `ls`, not a query:

    ~/.ocean-pipeline/gates/EXE-1a2b3c4d.json

Deliberately NOT the run's artifacts dir: a supervisor would have to enumerate every execution and
open each one to find the few that are waiting.

Every function here is best-effort and never raises. A marker is an OBSERVATION for a human, never
a source of truth — the checkpoint remains authoritative, and a run must never fail because its
marker could not be written. Same reasoning as telemetry.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

GATES_DIR = Path(os.environ.get(
    "OCEAN_PIPELINE_GATES_DIR", str(Path.home() / ".ocean-pipeline" / "gates")))


def _path(execution_id: str) -> Path:
    return GATES_DIR / f"{execution_id}.json"


def write(execution_id: str, ticket_id: str, gate: str, resume_hint: str = "") -> None:
    """Record that this run is WAITING at `gate`. Best-effort; never raises."""
    try:
        GATES_DIR.mkdir(parents=True, exist_ok=True)
        _path(execution_id).write_text(json.dumps({
            "execution_id": execution_id,
            "ticket_id": ticket_id,
            "gate": gate,                       # the paused node, e.g. "qa_review_gate"
            "state": "awaiting-human",
            "resume_hint": resume_hint,         # the exact flags that answer THIS gate
            "written_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }, indent=2) + "\n")
    except Exception:  # noqa: BLE001 — a marker must never be able to fail a run
        pass


def clear(execution_id: str) -> None:
    """The run is no longer waiting (resumed, finished, or failed). Best-effort; never raises."""
    try:
        _path(execution_id).unlink()
    except FileNotFoundError:
        pass
    except Exception:  # noqa: BLE001
        pass


def read(execution_id: str) -> dict:
    """This run's marker, or {} when it is not waiting. Never raises."""
    try:
        return json.loads(_path(execution_id).read_text())
    except Exception:  # noqa: BLE001 — absent, unreadable or malformed all mean "nothing to report"
        return {}


def waiting() -> list[dict]:
    """Every run currently waiting on a human, oldest first. Never raises.

    This is the function the whole module exists for: answering "what needs me?" without attaching
    to a run or opening the checkpoint database.
    """
    try:
        markers = [read(p.stem) for p in sorted(GATES_DIR.glob("*.json"))]
    except Exception:  # noqa: BLE001
        return []
    return [m for m in markers if m.get("state") == "awaiting-human"]
