"""lessons.py — a durable, cross-ticket lesson store for the coding-pipeline loop.

Finding 3 (architecture review of PR Aquaman#4/fk-aideveloper#294): "the tooling that runs
[compounding] isn't in the repo, so it's manual... ticket 200 is just as smart as ticket 1." This
file is the "Give the pipeline a memory" half: a structured lesson store keyed by
(domain_bucket, action_sig, fail_sig), written during a run and recalled by a LATER ticket in the
same domain before it starts fresh.

TWO writers feed it, deliberately:
  * `agents._capture_tool_failure` — the real `PostToolUseFailure` hook the review named, firing on
    each individual TOOL failure. This is the important one: it captures failures a run RECOVERED
    from, and failures on runs that ultimately shipped — the majority of what's worth not repeating,
    and all invisible to a run-terminal capture.
  * `nodes.stop_run` — one coarse record of WHY a run ended badly, keyed by station. Complements the
    above rather than duplicating it (a run can end badly with no tool failure at all, e.g. a review
    budget exhausted).
Recall happens in `nodes.researcher` and is folded into `_summary`, so every downstream station sees
it. (The separate "Mechanize the compounding loop" initiative — eval harness, golden-set coverage,
the bias-registry hard gate — lives in fk-aideveloper's ocean-rca skill, not here.)

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
import os
import re
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


def normalize_fail_sig(error: str, max_len: int = 120) -> str:
    """Collapse a raw tool error into a stable SIGNATURE that recurs across tickets.

    A lesson is only useful if the SAME underlying failure produces the SAME key on a different
    ticket, so the volatile parts must go: ids, paths, ports, hex/uuids, quoted literals and numbers
    all differ per run while naming the same defect. Best-effort and pure — never raises.

    The line CHOICE matters as much as the substitutions. Taking the first line unconditionally
    collapsed every Python traceback to the single key `Traceback (most recent call last):` — merging
    genuinely distinct defects into one record whose `note` was then overwritten by whichever failure
    wrote last (found in review). For a traceback the informative line is the LAST one, so pick that.

    The substitution ORDER matters too: `<hex>` before `<n>` meant `900000` -> `<n> ms` but
    `90000000` -> `<hex> ms`, i.e. the same numeric slot keyed differently by magnitude alone. The
    hex rule now requires an actual hex LETTER, so a pure digit run is always `<n>`."""
    try:
        lines = [ln.strip() for ln in (error or "").strip().splitlines() if ln.strip()]
        if not lines:
            return ""
        # A traceback header names the mechanism, not the defect — the exception on the last line does.
        s = lines[-1] if lines[0].startswith("Traceback (most recent call last)") else lines[0]
        s = re.sub(r"/[^\s'\"]+", "<path>", s)                       # absolute paths
        # shas/uuids/ids — must contain a hex letter, else a long DIGIT run would key as <hex> while a
        # shorter one keys as <n> (same slot, two keys — they could never accumulate to min_recurrence).
        s = re.sub(r"\b(?=[0-9a-fA-F]{8,}\b)[0-9a-fA-F]*[a-fA-F][0-9a-fA-F]*\b", "<hex>", s)
        s = re.sub(r"\b\d+(?:\.\d+)+\b", "<ver>", s)                 # 1.6.8.1 / 1.5.4 -> one token
        s = re.sub(r"\b\d+\b", "<n>", s)                             # ports, counts, line numbers
        s = re.sub(r"'[^']*'|\"[^\"]*\"", "<str>", s)                # quoted literals
        # "<n> failure" vs "<n> failures" is the same defect at a different count — one key, not two.
        s = re.sub(r"(<n> [A-Za-z]+)s\b", r"\1", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s[:max_len]
    except Exception:  # noqa: BLE001
        return (error or "")[:max_len]


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
                # ATOMIC: write a sibling temp file, fsync, then os.replace. A plain write_text that
                # is killed mid-flight leaves a TRUNCATED file, and `_read_records` reads that as
                # `[]` — so the very next record_failure rewrites the store from empty and the whole
                # history is silently gone (reproduced in review). os.replace is atomic within a
                # directory, so a reader sees either the old file or the new one, never a torn one.
                tmp = data_path.with_suffix(data_path.suffix + f".tmp.{os.getpid()}")
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(records, fh, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, data_path)
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
    except Exception:  # noqa: BLE001 — a lesson-store hiccup must never break a real run
        pass


def recall_lessons(domain_bucket: str, min_tickets: int = 2, limit: int = 5) -> list[dict]:
    """Best-effort, never raises. Returns records for `domain_bucket` seen on at least `min_tickets`
    DISTINCT tickets, most-recurrent first, capped at `limit`.

    The threshold is on distinct TICKETS, not on `recurrence_count`. Gating on the raw count made the
    "cross-ticket" claim false: the `PostToolUseFailure` hook fires on every individual tool failure,
    and an agent retrying one failing command three times in a single run reached a count of 3 by
    itself — so ONE flaky run permanently injected its own noise into every later ticket in the
    domain (reproduced in review). Two distinct tickets is the weakest honest evidence that a failure
    is a property of the DOMAIN rather than of one run.

    Empty list on a fresh/missing/corrupt store or a domain with nothing qualifying yet — the
    expected steady state early on, not an error.

    Read-only: no lock needed (`_read_records` tolerates a concurrent writer mid-rewrite by treating
    a transiently-invalid JSON parse as "empty" rather than raising, and a stale-but-valid read here
    is harmless — recall is advisory context, not a correctness-critical gate)."""
    if not domain_bucket:
        return []
    try:
        out = []
        for r in _read_records(_lessons_path()):
            if r.get("domain_bucket") != domain_bucket:
                continue
            tickets = r.get("tickets") or []
            if not isinstance(tickets, list) or len(set(tickets)) < min_tickets:
                continue
            out.append(r)
        out.sort(key=lambda r: r.get("recurrence_count", 0), reverse=True)
        return out[:limit]
    except Exception:  # noqa: BLE001
        return []
