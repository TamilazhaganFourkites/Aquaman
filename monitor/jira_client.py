"""Best-effort Jira ticket discovery for the batch monitor — self-contained, NOT
touching src/ocean_pipeline/jira.py (keeps monitor/ isolated from the core pipeline
package).

Auth: HTTP Basic (base64 "email:api_token"), NOT Bearer — verified directly against
the real fourkites.atlassian.net instance: the same JIRA_API_TOKEN that already works
for ocean_pipeline/jira.py's Bearer-authed transition()/comment() calls returned a
403 Forbidden here under Bearer auth, and 200 OK under Basic auth with JIRA_EMAIL
paired in. This matches oas-autodev/control_plane/jira.py's own working
implementation exactly (see its module docstring: "token-only (Basic auth)") — Jira
Cloud's REST API requires Basic auth with an API token, unlike the Bearer scheme
ocean_pipeline/jira.py uses (which apparently only needed to work for its own two
specific write endpoints, not general search).

Endpoint: POST /rest/api/3/search/jql first (the current, non-deprecated endpoint),
falling back to GET /rest/api/2/search on 404/410 — verified directly: this Jira
instance already returns 410 Gone on the classic v2 POST /search endpoint. Also
matches oas-autodev's own fallback logic exactly.

Best-effort throughout: no token / any error -> empty list, never raises. Loads
monitor/.env if present (a real env var always wins — same precedence oas-autodev's
own config.py documents) so credentials can live in a local, gitignored file instead
of being exported every shell session. Hand-rolled parser, stdlib only — no
python-dotenv, no requests/httpx.
"""
from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request


def _load_dotenv() -> None:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if val and val[0] in "\"'":
                q = val[0]
                end = val.find(q, 1)
                val = val[1:end] if end != -1 else val[1:]
            os.environ.setdefault(key, val)  # setdefault: a real exported env var always wins


_load_dotenv()

JIRA_BASE_URL = os.environ.get("JIRA_BASE_URL", "https://fourkites.atlassian.net").rstrip("/")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN", "")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL", "")
DEBUG = os.environ.get("OCEAN_PIPELINE_JIRA_DEBUG", "").lower() in ("1", "true", "yes")
TIMEOUT = float(os.environ.get("OCEAN_PIPELINE_JIRA_TIMEOUT", "10"))

DEFAULT_JQL = os.environ.get(
    "AQUAMAN_DISCOVER_JQL",
    'project = MM AND assignee = currentUser() AND status = "Refined" ORDER BY updated ASC',
)

# Lower sorts first — used only to order the auto-queue's still-pending tail.
_PRIORITY_WEIGHT = {"Highest": 0, "High": 1, "Medium": 2, "Low": 3, "Lowest": 4}


def _enabled() -> bool:
    return bool(JIRA_API_TOKEN and JIRA_EMAIL)


def _headers() -> dict[str, str]:
    auth = base64.b64encode(f"{JIRA_EMAIL}:{JIRA_API_TOKEN}".encode()).decode()
    return {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _post(url: str, body: dict) -> "urllib.request.Request":
    return urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                   headers=_headers())


def search(jql: str) -> list[dict]:
    """Return matching issues as [{key, summary, status, updated, priority}].

    POST /rest/api/3/search/jql first (the current, non-deprecated endpoint), falling
    back to the same request against /rest/api/2/search on 404/410 — this exact
    fallback shape is verified against the real Jira instance (v2 returns 410 Gone
    here) and matches oas-autodev/control_plane/jira.py's own working search().

    Best-effort: no token/email / no jql / any error -> empty list, never raises —
    this is a "nice to have" discovery list, not something that should ever break the
    monitor page or the auto-discovery loop. maxResults=50, no pagination — an
    intentional scope limit for "list eligible tickets", not a bug: a Refined queue
    past 50 for one engineer would be its own problem worth noticing, not silently
    paginating."""
    if not _enabled() or not jql:
        return []
    body = {"jql": jql, "fields": ["summary", "status", "updated", "priority"], "maxResults": 50}
    try:
        try:
            req = _post(f"{JIRA_BASE_URL}/rest/api/3/search/jql", body)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:  # noqa: S310 — fixed FK host
                data = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code not in (404, 410):
                raise
            req = _post(f"{JIRA_BASE_URL}/rest/api/2/search", body)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:  # noqa: S310 — fixed FK host
                data = json.loads(r.read().decode())
        out = []
        for issue in data.get("issues", []):
            f = issue.get("fields") or {}
            out.append({
                "key": issue.get("key"),
                "summary": f.get("summary"),
                "status": (f.get("status") or {}).get("name"),
                "updated": f.get("updated"),
                "priority": (f.get("priority") or {}).get("name"),
            })
        return out
    except Exception as e:  # noqa: BLE001 — discovery never blocks the monitor
        if DEBUG:
            print(f"[jira_client] search failed: {type(e).__name__}: {e}")
        return []


def priority_key(ticket: dict) -> tuple:
    """(priority_weight, updated) — lower sorts first. Used ONLY to position new
    auto-queue arrivals among the still-pending tail; never reorders anything
    already running/done. Unknown/missing priority defaults to "Medium" weight
    rather than last-place, since an unrecognized value is more likely a Jira
    priority-scheme difference than a genuinely low-priority ticket."""
    return (_PRIORITY_WEIGHT.get(ticket.get("priority"), 2), ticket.get("updated") or "")
