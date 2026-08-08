"""E1 — the machine-wide halt adopted from DY.

The four semantics the adoption review records are the whole value of the mechanism, so each gets
its own test. Three of them are the kind of thing that looks like a detail and is not:

  * checked FIRST — a halt consulted after the other budgets is a preference;
  * malformed file = ENGAGED — the inverse of how everything else in this codebase treats
    unparseable input, and correct here, because the cost of guessing wrong is exactly what
    someone reached for the switch to prevent;
  * a status read NEVER raises — it has to answer when the machine is misbehaving, which is when
    it will be used.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from ocean_pipeline import kill_switch, qa_batch


@pytest.fixture(autouse=True)
def _isolated_switch(tmp_path, monkeypatch):
    monkeypatch.setattr(kill_switch, "KILL_SWITCH_PATH", tmp_path / "HALT")


def test_absent_means_running_and_present_means_halted():
    assert kill_switch.engaged() is False
    assert kill_switch.status() == (False, "")

    kill_switch.engage("draining for a deploy")
    engaged, why = kill_switch.status()
    assert engaged is True and why == "draining for a deploy"

    kill_switch.release()
    assert kill_switch.engaged() is False
    kill_switch.release()   # idempotent — releasing an unengaged switch is not an error


def test_an_empty_or_unreadable_switch_still_halts(monkeypatch):
    """Malformed = ENGAGED. The file's EXISTENCE is the signal; its contents are a courtesy."""
    kill_switch.KILL_SWITCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    kill_switch.KILL_SWITCH_PATH.write_text("")
    assert kill_switch.status() == (True, "halted by the kill switch")

    kill_switch.KILL_SWITCH_PATH.write_bytes(b"\xff\xfe\x00 not utf-8")
    assert kill_switch.engaged() is True, "an undecodable file must not read as 'safe to continue'"

    # A filesystem that will not even answer `exists()` must also halt — do not infer "running"
    # from a disk that is misbehaving.
    def _boom(self):
        raise OSError("filesystem is unhappy")

    monkeypatch.setattr(Path, "exists", _boom)
    engaged, why = kill_switch.status()
    assert engaged is True and "unreadable" in why


def test_a_status_read_never_raises(monkeypatch):
    """Semantic 3, over every failure the disk can produce at either call site."""
    for attr in ("exists", "read_text"):
        def _boom(self, *a, **kw):
            raise OSError("nope")

        monkeypatch.setattr(Path, attr, _boom)
        kill_switch.status()          # must not raise
        kill_switch.engaged()
        monkeypatch.undo()


def test_the_path_is_env_overridable_and_not_under_tmp():
    """A halt a nightly cleaner can silently disengage is worse than no halt."""
    import importlib, os
    monkeypatched = dict(os.environ)
    try:
        os.environ["OCEAN_PIPELINE_KILL_SWITCH"] = "/somewhere/else/HALT"
        reloaded = importlib.reload(kill_switch)
        assert str(reloaded.KILL_SWITCH_PATH) == "/somewhere/else/HALT"

        os.environ.pop("OCEAN_PIPELINE_KILL_SWITCH")
        reloaded = importlib.reload(kill_switch)
        assert not str(reloaded.KILL_SWITCH_PATH).startswith("/tmp/")
        assert str(reloaded.KILL_SWITCH_PATH).endswith("/.ocean-pipeline/HALT")
    finally:
        os.environ.clear()
        os.environ.update(monkeypatched)
        importlib.reload(kill_switch)


def test_an_engaged_switch_stops_a_batch_before_the_next_ticket(monkeypatch, tmp_path):
    """Semantic 1, end to end: the batch must consult it BEFORE starting each ticket, and stop
    without touching the rest of the queue."""
    monkeypatch.setattr(kill_switch, "KILL_SWITCH_PATH", tmp_path / "HALT")
    started: list[str] = []

    async def _fake_run_one(ticket: str) -> dict:
        started.append(ticket)
        if len(started) == 2:               # engage midway, as an operator would
            kill_switch.engage("stop after this one")
        return {"ticket_id": ticket, "final_status": "completed"}

    monkeypatch.setattr(qa_batch, "run_one", _fake_run_one)
    results = asyncio.run(qa_batch.run_batch(["MM-1", "MM-2", "MM-3", "MM-4"]))

    assert started == ["MM-1", "MM-2"], "the batch kept starting tickets after the halt"
    assert len(results) == 2, "only completed tickets should be reported"

    # Released -> the next batch runs normally again.
    kill_switch.release()
    started.clear()
    monkeypatch.setattr(qa_batch, "run_one",
                        lambda t: asyncio.sleep(0, {"ticket_id": t, "final_status": "completed"}))
    assert len(asyncio.run(qa_batch.run_batch(["MM-5", "MM-6"]))) == 2


def test_a_switch_engaged_before_the_batch_starts_nothing(monkeypatch, tmp_path):
    monkeypatch.setattr(kill_switch, "KILL_SWITCH_PATH", tmp_path / "HALT")
    kill_switch.engage()
    called: list[str] = []

    async def _never(ticket: str) -> dict:
        called.append(ticket)
        return {}

    monkeypatch.setattr(qa_batch, "run_one", _never)
    assert asyncio.run(qa_batch.run_batch(["MM-1", "MM-2"])) == []
    assert called == [], "a pre-engaged switch must start nothing at all"
