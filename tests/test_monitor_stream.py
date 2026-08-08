"""A4 — the monitor's stdout read loop must survive an over-long line.

The failure is not the lost line, it is what the exception does on the way out. `readline()` raises
`ValueError` when a line exceeds the stream buffer; that used to escape the read loop, and the
`finally` beneath it removes the process from `_RUNNING_PROCS` BEFORE awaiting `proc.wait()`. So the
slot was never released, the UI Kill button 404'd (the process was already untracked), and only
restarting uvicorn recovered it.

The analysis was confirmed end to end but has ZERO observed occurrences: across 53 logs and 25,209
lines the longest is 1,428 bytes, because `agents.py::_format_message` truncates every content branch
to ~200 chars. So this is latent-robustness work, and both mitigations are cheap: a limit with ~700x
headroom, plus a loop that survives the raise anyway.

`monitor/app.py` pulls in FastAPI and a lot of module-level state, so these tests exercise the read
loop's SHAPE against a real `asyncio` stream rather than importing the app.
"""
from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest

_APP = Path(__file__).resolve().parents[1] / "monitor" / "app.py"


def _source() -> str:
    return _APP.read_text()


def test_the_stream_limit_is_set_and_has_real_headroom():
    src = _source()
    m = re.search(r"^STREAM_LINE_LIMIT = (.+)$", src, re.MULTILINE)
    assert m, "STREAM_LINE_LIMIT is not defined"
    limit = eval(m.group(1), {}, {})  # noqa: S307 — our own literal, one line above
    assert limit >= 1024 * 1024, f"limit {limit} is not meaningfully above asyncio's 64 KiB default"
    # The observed maximum is 1,428 bytes; anything in this range is headroom, not a guess.
    assert limit / 1428 > 100

    assert "limit=STREAM_LINE_LIMIT" in src, (
        "the limit is defined but never passed to create_subprocess_exec — asyncio keeps its 64 KiB "
        "default and readline() still raises")


def test_the_read_loop_catches_the_raise_it_can_still_get():
    """Belt and braces. A limit makes the raise unlikely; it does not make it impossible (a
    misconfigured child, a future limit change). The loop must not let it reach the `finally`."""
    src = _source()
    loop = src[src.index("assert proc.stdout is not None"):]
    loop = loop[:loop.index("finally:")]
    assert "except (ValueError, asyncio.LimitOverrunError)" in loop, (
        "readline() is unguarded — a ValueError escapes to the finally, which untracks the process "
        "before awaiting proc.wait(), stranding the slot and breaking the Kill button")
    # The handler body, comments stripped — a `continue` mentioned in prose is not a `continue`.
    handler = loop.split("except (ValueError")[1]
    handler = handler[:handler.index("if not raw:")] if "if not raw:" in handler else handler
    code = "\n".join(l for l in handler.splitlines() if not l.strip().startswith("#"))
    assert re.search(r"^\s*continue\s*$", code, re.MULTILINE), (
        "the handler must keep reading; breaking would end the run's log at the first long line")
    assert not re.search(r"^\s*(break|raise)\s*$", code, re.MULTILINE), (
        "the handler must not re-raise or break — that is the behaviour that strands the slot")


def test_asyncio_really_does_raise_at_the_limit_and_survives_with_the_guard():
    """Pin the underlying behaviour rather than trusting the docs — the whole item rests on it."""
    async def _probe(limit: int, line_len: int, guarded: bool) -> tuple[int, bool]:
        reader = asyncio.StreamReader(limit=limit)
        reader.feed_data(b"x" * line_len + b"\n" + b"short\n")
        reader.feed_eof()
        read, raised = 0, False
        while True:
            try:
                raw = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                raised = True
                if not guarded:
                    raise
                continue
            if not raw:
                break
            read += 1
        return read, raised

    # Unguarded, over the limit -> the exception escapes, exactly as it did in the monitor.
    with pytest.raises((ValueError, asyncio.LimitOverrunError)):
        asyncio.run(_probe(limit=64, line_len=500, guarded=False))

    # Guarded -> the long line is dropped, the stream keeps going, the loop still terminates.
    read, raised = asyncio.run(_probe(limit=64, line_len=500, guarded=True))
    assert raised is True and read >= 1, "the guard must skip the bad line, not the rest of the run"

    # Under the limit -> nothing raises at all, which is the normal case given the 1,428-byte max.
    read, raised = asyncio.run(_probe(limit=1024 * 1024, line_len=1428, guarded=True))
    assert (read, raised) == (2, False)
