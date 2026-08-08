"""E1 — a machine-wide halt for ocean batches.

Adopted from DY (`fourkites-ai-plugins` PR#310: `dy-harness-engine/kill_switch.py:66-85`,
`kill-switch.sh:26-37`, checked first in `loop_control.py:69-81`). The adoption review
(`important-notes/ocean-vs-DY-AM-what-to-adopt.md` §4.2) recommends taking this one outright:
**ocean runs unattended batches and has no machine-wide halt at all.** Today the only ways to stop
a batch mid-flight are Ctrl-C on the right terminal or killing uvicorn — neither of which is
available to someone who is not sitting at that machine.

NOT a verbatim copy: `fourkites-ai-plugins` is not checked out here, so this is written to the
four semantics the review records, each of which is a deliberate choice worth keeping:

  1. CHECKED FIRST, ahead of every other budget. A halt that is consulted after the iteration,
     token and wall-clock caps is not a halt, it is a preference.
  2. MALFORMED FILE = ENGAGED. The file exists to stop things; an unreadable one is not evidence
     that continuing is safe. This is the opposite of how the rest of this codebase treats
     unparseable input, and deliberately so — every other "could not read it" here degrades toward
     continuing, because the cost is a lost run. Here the cost of guessing wrong is the thing
     someone reached for the switch to prevent.
  3. A STATUS READ NEVER RAISES. Whatever the disk does, asking "are we halted?" answers.
  4. ENV-OVERRIDABLE path, so a test or a second checkout gets its own switch.

Usage:
    OCEAN_PIPELINE_KILL_SWITCH=~/.ocean-pipeline/HALT   # default
    touch ~/.ocean-pipeline/HALT                        # engage: batches stop before the next ticket
    rm ~/.ocean-pipeline/HALT                           # release

Scope, stated honestly: this stops a batch from STARTING further tickets. It does not abort a
ticket already mid-flight — a running ocean ticket owns Docker containers, SIT infra and a pushed
branch, and tearing that down from the outside is a different (and much larger) piece of work. The
switch turns "wait hours for the batch to drain, or kill uvicorn and strand the slots" into "the
current ticket finishes and nothing else starts".
"""
from __future__ import annotations

import os
from pathlib import Path

# Env-overridable (semantic 4). Defaults beside the other durable state, not under /tmp — a halt
# that a nightly cleaner can silently disengage is worse than no halt.
KILL_SWITCH_PATH = Path(os.environ.get(
    "OCEAN_PIPELINE_KILL_SWITCH", str(Path.home() / ".ocean-pipeline" / "HALT")))

# What a caller shows a human when the switch is engaged but carries no reason.
_DEFAULT_REASON = "halted by the kill switch"


def status() -> tuple[bool, str]:
    """(engaged, reason). NEVER raises — semantic 3.

    A present file engages the halt; its contents, if any, are the reason shown to the operator.
    An unreadable/undecodable file also engages it (semantic 2): the file's existence is the
    signal, and its contents are only ever a courtesy.
    """
    try:
        if not KILL_SWITCH_PATH.exists():
            return False, ""
    except OSError:
        # Cannot even stat it. Do not infer "not halted" from a filesystem that will not answer —
        # the whole point of this switch is to be trusted when things are going wrong.
        return True, f"{_DEFAULT_REASON} (kill-switch path unreadable: {KILL_SWITCH_PATH})"
    try:
        reason = KILL_SWITCH_PATH.read_text(errors="replace").strip()
    except OSError:
        reason = ""
    return True, (reason or _DEFAULT_REASON)


def engaged() -> bool:
    """True when the halt is on. Call this BEFORE any other budget check (semantic 1)."""
    return status()[0]


def engage(reason: str = "") -> Path:
    """Turn the halt on. Returns the path, so a caller can tell the operator where it is."""
    KILL_SWITCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    KILL_SWITCH_PATH.write_text(reason or _DEFAULT_REASON)
    return KILL_SWITCH_PATH


def release() -> None:
    """Turn the halt off. A no-op when it was never on."""
    try:
        KILL_SWITCH_PATH.unlink()
    except FileNotFoundError:
        pass
