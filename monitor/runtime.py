"""Process-local auto-pickup control. Defaults PAUSED — mirrors oas-autodev's own
START_PAUSED=true default exactly: real git/PR/Jira side effects must never fire
unattended without an explicit human Resume. Pausing only stops the discovery loop
and auto-worker from picking up NEW work; manual batches (POST /batches) and
in-flight reconciliation are always available regardless of this flag."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Runtime:
    paused: bool = True
    paused_reason: str = "started paused — resume to begin auto-pickup"


runtime = Runtime()
