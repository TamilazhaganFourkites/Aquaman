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

from pathlib import Path

import pytest

from ocean_pipeline import kill_switch


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


def test_every_driver_that_starts_a_run_consults_the_halt():
    """E1's stated scope is MACHINE-WIDE, and it shipped consulted in exactly one place.

    It shipped consulted in the unattended sweep only; `cli._run` (the ordinary
    `ocean-pipeline MM-1234` entry point, and the one `run-batch.sh` loops), both monitor batch
    drivers, and `run-batch.sh` itself did not — so an engaged switch stopped one driver and
    nothing else. A stop that one of several entry points honours is a convention, not a stop.

    Asserted per driver by NAME so a new driver added without the consult is visible here, rather
    than as a run that started during a halt."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    drivers = {
        "src/ocean_pipeline/cli.py": "kill_switch.status(",
        "monitor/app.py": "_kill_switch.",
        "run-batch.sh": "KILL_SWITCH",
    }
    missing = [f for f, needle in drivers.items()
               if needle not in (root / f).read_text(errors="replace")]
    assert not missing, f"these drivers can start a run during a machine-wide halt: {missing}"


def test_the_shell_driver_uses_the_same_path_the_module_does():
    """`run-batch.sh` cannot import the module, so it re-derives the path — and a drifting default
    would make the shell loop ignore a switch the Python drivers honour."""
    import importlib
    import os
    import pathlib
    import re

    from ocean_pipeline import kill_switch

    sh = (pathlib.Path(__file__).resolve().parents[1] / "run-batch.sh").read_text()
    m = re.search(r'KILL_SWITCH="\$\{OCEAN_PIPELINE_KILL_SWITCH:-\$HOME/([^"}]+)\}"', sh)
    assert m, "run-batch.sh no longer derives the kill-switch path the documented way"

    # The module's DEFAULT, not its current value: conftest redirects `KILL_SWITCH_PATH` into the
    # session temp dir, so comparing the live attribute compares the test harness, not the shipped
    # default the shell has to match.
    saved = dict(os.environ)
    try:
        os.environ.pop("OCEAN_PIPELINE_KILL_SWITCH", None)
        default = str(importlib.reload(kill_switch).KILL_SWITCH_PATH)
    finally:
        os.environ.clear()
        os.environ.update(saved)
        importlib.reload(kill_switch)
    assert default.endswith(m.group(1)), (
        f"shell default {m.group(1)!r} does not match the module default {default!r}")
