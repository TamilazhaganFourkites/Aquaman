"""Best-effort Jira lifecycle updates — graph-owned, non-blocking.

fk-aideveloper reaches Jira for writes through the Atlassian MCP (OAuth), which a headless
Python process can't easily use. But its `scripts/catalog_harvester.py` shows the plain REST v2
pattern — `Authorization: Bearer $JIRA_API_TOKEN` against `fourkites.atlassian.net/rest/api/2/…`
— so we replicate that here to move the ticket through its lifecycle deterministically from the
graph (In Progress at start, In Review + a PR-link comment at ready-flip).

Best-effort, exactly like telemetry: no `JIRA_API_TOKEN` -> silent no-op; any error is swallowed.
It must never block or fail a pipeline run. Uses stdlib urllib (no extra dependency).
"""
from __future__ import annotations

import json
import os
import urllib.request

JIRA_BASE_URL = os.environ.get("JIRA_BASE_URL", "https://fourkites.atlassian.net").rstrip("/")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN", "")
DEBUG = os.environ.get("OCEAN_PIPELINE_JIRA_DEBUG", "").lower() in ("1", "true", "yes")
TIMEOUT = float(os.environ.get("OCEAN_PIPELINE_JIRA_TIMEOUT", "10"))


def _enabled() -> bool:
    return bool(JIRA_API_TOKEN)


def _req(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{JIRA_BASE_URL}/rest/api/2/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {JIRA_API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:  # noqa: S310 — fixed FK host
        raw = r.read().decode()
        return json.loads(raw) if raw else {}


def transition(ticket_id: str, target_status: str) -> None:
    """Move the issue to the first available transition whose name matches `target_status`
    (case-insensitive substring). Best-effort: unknown transition or any error -> no-op."""
    if not _enabled() or not ticket_id:
        return
    try:
        transitions = _req("GET", f"issue/{ticket_id}/transitions").get("transitions", [])
        t = next((x for x in transitions if target_status.lower() in x.get("name", "").lower()), None)
        if not t:
            if DEBUG:
                print(f"[jira] no transition matching {target_status!r} for {ticket_id}")
            return
        _req("POST", f"issue/{ticket_id}/transitions", {"transition": {"id": t["id"]}})
        if DEBUG:
            print(f"[jira] {ticket_id} -> {t['name']}")
    except Exception as e:  # noqa: BLE001 — Jira updates never block the pipeline
        if DEBUG:
            print(f"[jira] transition failed: {type(e).__name__}: {e}")


def comment(ticket_id: str, body: str) -> None:
    """Post a plain-text comment (REST v2 body is a plain string). Best-effort."""
    if not _enabled() or not ticket_id or not body:
        return
    try:
        _req("POST", f"issue/{ticket_id}/comment", {"body": body})
        if DEBUG:
            print(f"[jira] commented on {ticket_id}")
    except Exception as e:  # noqa: BLE001
        if DEBUG:
            print(f"[jira] comment failed: {type(e).__name__}: {e}")


def comment_once(ticket_id: str, body: str, marker: str) -> None:
    """Idempotent comment (MM-14816 / G20 P3): post `body` only if no existing comment on the ticket
    already contains `marker`. oas-autodev's resume_awaiting_input re-runs the pipeline on every new
    Jira comment, so a re-blocked run must NOT re-post the same open questions each cycle. Best-effort:
    if the existence check errors, fall back to posting (a duplicate comment is better than a silently
    dropped escalation)."""
    if not _enabled() or not ticket_id or not body:
        return
    try:
        # orderBy=-created → NEWEST first, so our recent block comment is in the first page even on a
        # long-lived ticket (Jira v2 defaults to oldest-first, which would push it past maxResults and
        # silently re-post on every resume — the P3 no-respam guarantee).
        existing = _req("GET", f"issue/{ticket_id}/comment?maxResults=100&orderBy=-created").get("comments", [])
        if any(marker in (c.get("body") or "") for c in existing):
            if DEBUG:
                print(f"[jira] comment_once skipped for {ticket_id} — marker already present")
            return
    except Exception as e:  # noqa: BLE001 — can't check → fall through and post (don't drop the ask)
        if DEBUG:
            print(f"[jira] comment_once existence-check failed ({type(e).__name__}); posting anyway")
    comment(ticket_id, body)
