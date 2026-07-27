"""aidev_db telemetry.

Node hooks write the same rows fk-execute writes today (pipeline_executions
START/END, pipeline_station_events), but from deterministic code instead of a
model-followed checklist. Because the graph runs under a checkpointer, a crash
resumes rather than leaving a `final_status='running'` orphan (AP-223).

These call the aidev-db MCP tools. Wire them to your MCP client of choice; the
functions are intentionally thin so the transport is swappable.
"""
from __future__ import annotations

import os
import secrets
from typing import Any


def new_execution_id() -> str:
    return f"EXE-{secrets.token_hex(4)}"


def _call_mcp(tool: str, **kwargs: Any) -> None:
    # TODO: route to the aidev-db MCP server (/mcp-aidev on rca-app).
    # Left as a hook so this repo has no hard MCP-transport dependency.
    if os.environ.get("OCEAN_PIPELINE_TELEMETRY_DEBUG"):
        print(f"[telemetry] {tool} {kwargs}")


def execution_start(execution_id: str, ticket_id: str, route: str = "unclassified") -> None:
    _call_mcp(
        "aidev_insert_execution",
        execution_id=execution_id,
        ticket_id=ticket_id,
        route=route,
        final_status="running",
        data_source="langgraph",   # distinguish from fk-execute / session_forwarder
    )


def execution_end(execution_id: str, ticket_id: str, final_status: str, route: str) -> None:
    _call_mcp(
        "aidev_insert_execution",
        execution_id=execution_id,
        ticket_id=ticket_id,
        route=route,
        final_status=final_status,
        data_source="langgraph",
    )


def station_event(execution_id: str, station_number: float, phase: str, **extra: Any) -> None:
    _call_mcp(
        "aidev_insert_station_event",
        execution_id=execution_id,
        station_number=station_number,
        phase=phase,
        **extra,
    )
