"""aidev-db telemetry over the FourKites HTTP MCP server.

fk-execute writes `pipeline_executions` (START/END) and `pipeline_station_events` by calling
the `aidev-db` **HTTP MCP server** — `https://rca-app-api.fourkites.com/mcp-aidev/`, auth
`Authorization: Bearer $RCA_TOKEN`. We replicate that exact transport so LangGraph runs land in
the same `aidev_db` tables (the server owns the ClickHouse INSERT + column list). This is the
Option-A path from the fk-aideveloper investigation — the server, not us, holds the schema.

Design guarantees (match fk-execute's "telemetry never blocks the pipeline"):
  * No `RCA_TOKEN` (the local-dev default) -> every call is a silent no-op.
  * Any transport error is swallowed (logged only under OCEAN_PIPELINE_TELEMETRY_DEBUG).
  * Calls are dispatched to a small thread pool so a node never waits on the network;
    `flush()` (called by the CLI at run end) drains them with a bounded timeout so the END
    row lands before the process exits.

The `mcp` SDK arrives transitively via claude-agent-sdk; the import is still guarded so a
stripped environment degrades to a no-op rather than crashing.
"""
from __future__ import annotations

import asyncio
import os
import secrets
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

AIDEV_MCP_URL = os.environ.get("AIDEV_MCP_URL", "https://rca-app-api.fourkites.com/mcp-aidev/")
RCA_TOKEN = os.environ.get("RCA_TOKEN", "")
SESSION_ENV = os.environ.get("OCEAN_PIPELINE_SESSION_ENV", "prod")
DEBUG = os.environ.get("OCEAN_PIPELINE_TELEMETRY_DEBUG", "").lower() in ("1", "true", "yes")
HTTP_TIMEOUT = float(os.environ.get("OCEAN_PIPELINE_TELEMETRY_TIMEOUT", "10"))

# The aidev-db MCP tool has no `data_source` param (the server defaults it to 'fk-execute'), so
# we mark langgraph runs in additional_context instead — enough to filter them apart later.
_SOURCE_TAG = "source=langgraph (aquaman)"

# Aquaman station-number -> the server's canonical station name (fk-execute telemetry map).
_STATION_NAMES = {
    0: "researcher", 0.1: "rca_router", 0.5: "sme_consult",
    1: "dependency_resolver", 1.5: "reachability_gate",
    3.87: "open_pr", 4: "coder", 4.5: "graph_caller_augmentation",
    4.6: "release_intelligence_writer", 5: "harsh_review", 5.9: "code_fault_rework",
    5.95: "learn_repo", 6.0: "sit_resolve", 6.2: "sit_run", 6.4: "sit_triage", 6.5: "flip_ready",
}
# Aquaman phase label -> the server's station status enum.
_STATUS = {
    "start": "started", "end": "completed", "stop": "failed", "done": "completed",
    "skip": "skipped", "unsupported_route": "skipped", "code_fault_rework": "started",
}

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="aidev-telemetry")
_futures: list[Future] = []


def new_execution_id() -> str:
    return f"EXE-{secrets.token_hex(4)}"


def _engineer() -> str:
    return (os.environ.get("OCEAN_PIPELINE_ENGINEER")
            or os.environ.get("USER") or os.environ.get("USERNAME") or "aquaman")


async def _call_async(tool: str, args: dict[str, Any]) -> None:
    from mcp import ClientSession
    try:  # newer SDK name; fall back to the older alias on older mcp versions
        from mcp.client.streamable_http import streamable_http_client as _http_client
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client as _http_client

    headers = {"Authorization": f"Bearer {RCA_TOKEN}"}
    async with _http_client(AIDEV_MCP_URL, headers=headers, timeout=HTTP_TIMEOUT) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            await session.call_tool(tool, arguments=args)


def _call_blocking(tool: str, args: dict[str, Any]) -> None:
    try:
        asyncio.run(_call_async(tool, args))
    except Exception as e:  # noqa: BLE001 — telemetry must never surface an error
        if DEBUG:
            print(f"[telemetry] {tool} failed: {type(e).__name__}: {e}")


def _dispatch(tool: str, args: dict[str, Any]) -> None:
    if DEBUG:
        print(f"[telemetry] {tool} {args}")
    if not RCA_TOKEN:
        return  # no creds -> silent no-op (local-dev default), exactly like fk-execute best-effort
    _futures.append(_executor.submit(_call_blocking, tool, args))


def flush(timeout: float = 15.0) -> None:
    """Drain outstanding telemetry writes (called by the CLI at run end so the END row lands)."""
    for fut in list(_futures):
        try:
            fut.result(timeout=timeout)
        except Exception:  # noqa: BLE001
            pass
    _futures.clear()


def execution_start(execution_id: str, ticket_id: str, route: str = "unclassified") -> None:
    _dispatch("aidev_insert_execution", {
        "execution_id": execution_id,
        "engineer": _engineer(),
        "input_type": "jira_ticket",
        "ticket_id": ticket_id,
        "route": route,
        "final_status": "running",
        "session_env": SESSION_ENV,
        "additional_context": _SOURCE_TAG,
    })


def execution_end(execution_id: str, ticket_id: str, final_status: str, route: str,
                  final_outcome: str = "") -> None:
    _dispatch("aidev_insert_execution", {
        "execution_id": execution_id,
        "engineer": _engineer(),
        "input_type": "jira_ticket",
        "ticket_id": ticket_id,
        "route": route or "unclassified",
        "final_status": final_status,
        "final_outcome": final_outcome or final_status,
        "session_env": SESSION_ENV,
        "additional_context": _SOURCE_TAG,
    })


def station_event(execution_id: str, station_number: float, phase: str, **extra: Any) -> None:
    name = _STATION_NAMES.get(station_number, f"station_{station_number}")
    args: dict[str, Any] = {
        "execution_id": execution_id,
        "station": name,
        "station_number": float(station_number),
        "status": _STATUS.get(phase, "completed"),
    }
    # Fold a few known facts into the free-text summary (best-effort; unknown keys are ignored).
    summary = "; ".join(f"{k}={v}" for k, v in extra.items() if v not in (None, "", []))
    if summary:
        args["output_summary"] = summary[:500]
    _dispatch("aidev_insert_station_event", args)
