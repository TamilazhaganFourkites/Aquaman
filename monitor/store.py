"""Persistent execution history for the batch monitor — plain stdlib sqlite3, no ORM,
no new pip dependency (matches the existing minimalism of monitor/ and
ocean_pipeline/jira.py, both stdlib-only).

Mirrors TicketRun/Batch's existing summary fields (app.py) — deliberately NOT a
richer/different state machine: a live TicketRun.status stays exactly
queued|running|paused|done|failed. "interrupted" is written ONLY by
reconcile_stale(), against PERSISTED rows from a prior process lifetime whose real
OS subprocess is definitely dead (the monitor itself was killed/restarted) — it is
never assigned to a live TicketRun.

Every public function is best-effort: a persistence hiccup must never break a live
pipeline run, so failures are swallowed broadly (matching ocean_pipeline/jira.py's
own `except Exception` pattern, not a narrower except — sqlite can raise things
beyond OSError, e.g. sqlite3.OperationalError on a locked/corrupt file).

Type hints reference TicketRun/Batch by name only (no import) to avoid a circular
import with app.py, which imports this module.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).parent / "monitor.db"


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=5)
    con.row_factory = sqlite3.Row
    return con


def init() -> None:
    try:
        with _conn() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    id TEXT PRIMARY KEY,
                    created_at REAL NOT NULL,
                    log_level TEXT NOT NULL DEFAULT 'team',
                    cursor INTEGER NOT NULL DEFAULT 0
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    idx INTEGER NOT NULL,
                    ticket TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_id TEXT,
                    paused_gate TEXT,
                    paused_message TEXT,
                    final_status TEXT,
                    final_outcome TEXT,
                    pr_number INTEGER,
                    started_at REAL,
                    finished_at REAL,
                    log_path TEXT,
                    UNIQUE(batch_id, idx)
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_tickets_started ON tickets(started_at)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_tickets_status  ON tickets(status)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_tickets_ticket  ON tickets(ticket)")
    except Exception:  # noqa: BLE001 — persistence must never block startup
        pass


def save_batch(batch: Any) -> None:
    try:
        with _conn() as con:
            # INSERT OR REPLACE, not ON CONFLICT ... DO UPDATE (SQLite's UPSERT syntax needs
            # 3.24+; verified this machine's bundled sqlite3 is 3.22.0, where DO UPDATE is a
            # syntax error). REPLACE against a UNIQUE/PRIMARY KEY conflict deletes+reinserts
            # the row — safe here since nothing references batches.id/tickets.id as an FK.
            con.execute(
                "INSERT OR REPLACE INTO batches (id, created_at, log_level, cursor) "
                "VALUES (?, ?, ?, ?)",
                (batch.id, batch.created_at, batch.log_level, batch.cursor),
            )
    except Exception:  # noqa: BLE001
        pass


def save_ticket(batch_id: str, idx: int, run: Any) -> None:
    try:
        with _conn() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO tickets
                    (batch_id, idx, ticket, status, execution_id, paused_gate,
                     paused_message, final_status, final_outcome, pr_number,
                     started_at, finished_at, log_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (batch_id, idx, run.ticket, run.status, run.execution_id, run.paused_gate,
                 run.paused_message, run.final_status, run.final_outcome, run.pr_number,
                 run.started_at, run.finished_at, run.log_path),
            )
    except Exception:  # noqa: BLE001
        pass


def recent_tickets(limit: int = 200) -> list[dict]:
    try:
        with _conn() as con:
            rows = con.execute(
                "SELECT id, batch_id, idx, ticket, status, execution_id, paused_gate, "
                "paused_message, final_status, final_outcome, pr_number, started_at, "
                "finished_at, log_path FROM tickets "
                "ORDER BY COALESCE(started_at, 0) DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:  # noqa: BLE001
        return []


def counts() -> dict:
    try:
        with _conn() as con:
            rows = con.execute("SELECT status, COUNT(*) AS n FROM tickets GROUP BY status").fetchall()
            return {r["status"]: r["n"] for r in rows}
    except Exception:  # noqa: BLE001
        return {}


def delete_ticket(row_id: int) -> bool:
    """Delete one persisted ticket row by its sqlite id (as returned by
    recent_tickets()) — used to clean up a failed/interrupted entry from History.
    Best-effort; returns whether a row was actually removed."""
    try:
        with _conn() as con:
            cur = con.execute("DELETE FROM tickets WHERE id = ?", (row_id,))
            return cur.rowcount > 0
    except Exception:  # noqa: BLE001
        return False


def clear_batch_tickets(batch_id: str) -> None:
    """Delete every persisted row for one batch — used when re-persisting the
    auto-queue after a dismiss, so a shrunk queue doesn't leave stale trailing rows
    at indices past the new (shorter) length."""
    try:
        with _conn() as con:
            con.execute("DELETE FROM tickets WHERE batch_id = ?", (batch_id,))
    except Exception:  # noqa: BLE001
        pass


def reconcile_stale() -> int:
    """Flip every persisted 'running' row to 'interrupted' — called once at startup,
    before any new batch can be created. A 'running' row surviving to a fresh process
    start means the OS subprocess that was driving it is definitely gone (killed along
    with the prior monitor process, or the machine restarted) — it did not error, so
    it must not be mislabeled 'failed'; it also must not be left looking like it's
    still in flight forever. Returns the number of rows flipped, for a startup log line."""
    try:
        with _conn() as con:
            cur = con.execute("UPDATE tickets SET status = 'interrupted' WHERE status = 'running'")
            return cur.rowcount
    except Exception:  # noqa: BLE001
        return 0


def recent_failures_for(ticket: str, within_days: int = 7) -> int:
    """Count of 'failed' rows for this exact ticket id within the last `within_days`
    days — a lightweight, honest analog to oas-autodev's "lessons" recurrence count.
    No correction workflow, no enforcement/escalation state: purely an informational
    signal surfaced as a warning badge in Discovered/History, never auto-blocking."""
    try:
        cutoff = time.time() - within_days * 86400
        with _conn() as con:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM tickets WHERE ticket = ? AND status = 'failed' "
                "AND COALESCE(finished_at, started_at, 0) >= ?",
                (ticket, cutoff),
            ).fetchone()
            return row["n"] if row else 0
    except Exception:  # noqa: BLE001
        return 0
