"""lessons.py — a durable, cross-ticket lesson store for the coding-pipeline loop.

Finding 3 (architecture review of PR Aquaman#4/fk-aideveloper#294): "the tooling that runs
[compounding] isn't in the repo, so it's manual... ticket 200 is just as smart as ticket 1." The
review's own roadmap splits this into two separate initiatives: "Mechanize Compounding Loop"
(ship an eval harness, extend the golden set to all domain buckets, wire the bias registry's
hard-gate proposal — a genuinely larger, separate follow-up, NOT attempted here) and "Give Pipeline
Memory" (this file): a PostToolUseFailure-style capture that writes a structured lesson when a run
stops without shipping, keyed by (domain_bucket, action_sig, fail_sig), recalled by a LATER ticket
in the same domain before it starts fresh.

Storage: a single JSON file under `config.ARTIFACTS_ROOT` — confirmed the only mechanism actually
buildable today without a new MCP round-trip: Aquaman's `telemetry.py` only has INSERT-only
ClickHouse tools (`aidev_insert_execution`/`aidev_insert_station_event`); no read tool is available
to pipeline code, so a lesson written to ClickHouse could never be read back from within a run. A
local file, keyed by domain rather than ticket, is a real, working substitute — it survives across
runs on this machine (not across a fresh machine/deploy, which the ClickHouse tables would, but they
aren't readable today; extending them to a read path is exactly the kind of thing the separate,
larger "Mechanize Compounding Loop" initiative should do).

CONCURRENCY (judge-review finding on the first cut of this file): `config.ARTIFACTS_ROOT` is a
single path shared across every `ocean-pipeline` invocation on this machine, and the Monitor
(`monitor/app.py`) dispatches up to `MAX_CONCURRENT_TICKETS` (default 3) of those as SEPARATE OS
SUBPROCESSES, not threads — a plain `threading.Lock` (the first cut's approach) gives zero
protection across processes and can lose an update when two tickets in the same domain fail
concurrently. Uses the SAME `fcntl.flock`-based cross-process locking pattern already established in
this file's own package (`nodes.py`'s SIT/build/GAN slot locks) instead.

Best-effort throughout, matching monitor/store.py's own established convention: a lesson-store
hiccup (corrupt file, disk full, lock contention) must never break a real pipeline run — every
public function swallows its own exceptions and returns a safe empty/no-op result.
"""
from __future__ import annotations

import fcntl
import json
import time
from pathlib import Path

from . import config


def _lessons_path() -> Path:
    p = config.ARTIFACTS_ROOT / "lessons.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _lock_path() -> Path:
    # A dedicated lock file, not the data file itself: flock's semantics are simplest against a
    # file that's never truncated/rewritten mid-lock (record_failure below reopens the DATA file
    # separately, under this lock, for the actual read-modify-write).
    p = config.ARTIFACTS_ROOT / "lessons.json.lock"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:  # noqa: BLE001 — corrupt/partial file -> treat as empty, never crash the run
        return []


def record_failure(domain_bucket: str, action_sig: str, fail_sig: str, ticket_id: str,
                    execution_id: str, note: str = "") -> None:
    """Best-effort, never raises. Increments `recurrence_count` if this EXACT
    (domain_bucket, action_sig, fail_sig) triple was already seen; otherwise appends a new record.
    `tickets[]` is capped at the 20 most recent so a domain that fails the same way constantly can't
    grow the file unbounded. No-op if domain_bucket/fail_sig is empty — a signature with no domain
    to key on, or no failure reason, isn't recallable and isn't worth writing.

    The read-modify-write is a single critical section under an EXCLUSIVE, BLOCKING `fcntl.flock` on
    a dedicated lock file (cross-process safe, unlike a `threading.Lock` — see module docstring). A
    concurrent ticket in the same domain briefly waits rather than racing; a crashed holder's lock is
    released by the kernel, so this can never deadlock a future run."""
    if not domain_bucket or not fail_sig:
        return
    data_path = _lessons_path()
    try:
        with open(_lock_path(), "w") as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)   # blocking -- this critical section is brief
            try:
                records = _read_records(data_path)
                now = time.time()
                for r in records:
                    if (r.get("domain_bucket") == domain_bucket and r.get("action_sig") == action_sig
                            and r.get("fail_sig") == fail_sig):
                        r["recurrence_count"] = r.get("recurrence_count", 1) + 1
                        r["last_seen"] = now
                        tickets = r.get("tickets") or []
                        if not isinstance(tickets, list):
                            tickets = []
                        if ticket_id and ticket_id not in tickets:
                            tickets.append(ticket_id)
                        r["tickets"] = tickets[-20:]
                        if note:
                            r["note"] = note
                        break
                else:
                    records.append({
                        "domain_bucket": domain_bucket,
                        "action_sig": action_sig,
                        "fail_sig": fail_sig,
                        "recurrence_count": 1,
                        "first_seen": now,
                        "last_seen": now,
                        "tickets": [ticket_id] if ticket_id else [],
                        "execution_id": execution_id,
                        "note": note,
                    })
                data_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
    except Exception:  # noqa: BLE001 — a lesson-store hiccup must never break a real run
        pass


def recall_lessons(domain_bucket: str, min_recurrence: int = 2, limit: int = 5) -> list[dict]:
    """Best-effort, never raises. Returns records for `domain_bucket` with
    `recurrence_count >= min_recurrence`, sorted by recurrence descending, capped at `limit`.
    `min_recurrence` defaults to 2 (not 1) deliberately — a failure seen exactly once isn't yet a
    PATTERN worth surfacing to every future ticket in the domain; recalling every one-off would
    drown the genuinely recurring ones in noise. Empty list on a fresh/missing/corrupt store or a
    domain with nothing qualifying yet — that's the expected steady state early on, not an error.

    Read-only: no lock needed (`_read_records` tolerates a concurrent writer mid-rewrite by treating
    a transiently-invalid JSON parse as "empty" rather than raising, and a stale-but-valid read here
    is harmless — recall is advisory context, not a correctness-critical gate)."""
    if not domain_bucket:
        return []
    try:
        records = _read_records(_lessons_path())
        matches = [r for r in records
                   if r.get("domain_bucket") == domain_bucket
                   and r.get("recurrence_count", 0) >= min_recurrence]
        matches.sort(key=lambda r: r.get("recurrence_count", 0), reverse=True)
        return matches[:limit]
    except Exception:  # noqa: BLE001
        return []
