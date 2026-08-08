"""E2 — durable, out-of-process markers for the human gates.

The value is answering "what is blocked on a human right now?" WITHOUT attaching to a run's stdout
or opening the LangGraph checkpoint DB. That blindness is the C6 defect one level up: the CLI now
refuses a wrong-gate flag, but until this existed an operator had no way to learn which flag was
right except from the run's own console output, which nobody may still be watching.

A marker is an OBSERVATION, never a source of truth — the checkpoint stays authoritative, and no
marker operation may ever fail a run. That is what most of these tests are about.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ocean_pipeline import gate_marker


@pytest.fixture(autouse=True)
def _isolated_gates(tmp_path, monkeypatch):
    monkeypatch.setattr(gate_marker, "GATES_DIR", tmp_path / "gates")


def test_a_waiting_run_is_visible_without_the_checkpoint():
    assert gate_marker.waiting() == []

    gate_marker.write("EXE-1", "MM-100", "qa_review_gate", "resume with --qa approve-testrail")
    m = gate_marker.read("EXE-1")
    assert m["gate"] == "qa_review_gate" and m["state"] == "awaiting-human"
    assert m["ticket_id"] == "MM-100"
    assert "--qa" in m["resume_hint"], "the marker must carry the flags that answer THIS gate"
    assert m["written_at"]

    assert [x["execution_id"] for x in gate_marker.waiting()] == ["EXE-1"]


def test_waiting_lists_every_blocked_run_and_only_those():
    for i, gate in enumerate(("qa_review_gate", "human_gate", "blocked_review_gate"), 1):
        gate_marker.write(f"EXE-{i}", f"MM-{i}", gate, f"hint {i}")
    assert len(gate_marker.waiting()) == 3

    gate_marker.clear("EXE-2")
    remaining = {x["execution_id"] for x in gate_marker.waiting()}
    assert remaining == {"EXE-1", "EXE-3"}, "a resumed run must drop out of the list"
    assert gate_marker.read("EXE-2") == {}


def test_no_marker_operation_can_ever_fail_a_run(monkeypatch):
    """Every call is best-effort. A run must not die because its marker could not be written —
    same standing as telemetry."""
    gate_marker.clear("never-existed")          # clearing an absent marker is a no-op

    def _boom(*a, **kw):
        raise OSError("disk is unhappy")

    monkeypatch.setattr(Path, "mkdir", _boom)
    gate_marker.write("EXE-x", "MM-1", "human_gate", "hint")   # must not raise

    monkeypatch.undo()
    monkeypatch.setattr(Path, "unlink", _boom)
    gate_marker.clear("EXE-x")                                  # must not raise

    monkeypatch.undo()
    monkeypatch.setattr(Path, "read_text", _boom)
    assert gate_marker.read("EXE-x") == {}
    assert gate_marker.waiting() == []


def test_a_malformed_marker_is_ignored_not_reported_as_waiting():
    """Unlike the kill switch, a broken marker must NOT be treated as engaged. It is a report about
    someone else's run, not a safety control — inventing a waiting run from a corrupt file would
    send an operator looking for a gate that does not exist."""
    (gate_marker.GATES_DIR).mkdir(parents=True, exist_ok=True)
    (gate_marker.GATES_DIR / "EXE-bad.json").write_text("{ not json")
    (gate_marker.GATES_DIR / "EXE-good.json").write_text(
        json.dumps({"execution_id": "EXE-good", "state": "awaiting-human", "gate": "human_gate"}))

    assert gate_marker.read("EXE-bad") == {}
    assert [x["execution_id"] for x in gate_marker.waiting()] == ["EXE-good"], (
        "one corrupt marker must not hide the healthy ones, or invent a run of its own")


def test_the_gates_dir_is_flat_env_overridable_and_not_under_tmp():
    """Flat, so "what needs me?" is an `ls` rather than a walk of every execution's artifacts dir —
    and not under /tmp, where a nightly cleaner would quietly erase the record."""
    import importlib, os
    saved = dict(os.environ)
    try:
        os.environ["OCEAN_PIPELINE_GATES_DIR"] = "/somewhere/gates"
        r = importlib.reload(gate_marker)
        assert str(r.GATES_DIR) == "/somewhere/gates"

        os.environ.pop("OCEAN_PIPELINE_GATES_DIR")
        r = importlib.reload(gate_marker)
        assert not str(r.GATES_DIR).startswith("/tmp/")
        assert str(r.GATES_DIR).endswith("/.ocean-pipeline/gates")
    finally:
        os.environ.clear()
        os.environ.update(saved)
        importlib.reload(gate_marker)


def test_cli_writes_on_pause_and_clears_on_every_terminal_path():
    """The wiring, asserted on the source: a marker written and never cleared advertises a gate
    nobody can answer, and `waiting()` then shows runs that died hours ago."""
    from ocean_pipeline import cli
    src = Path(cli.__file__).read_text()
    assert "gate_marker.write(" in src, "nothing records a pause"
    # Cleared on BOTH terminal paths — the clean finish and the exception handler.
    assert src.count("gate_marker.clear(") >= 2, (
        "a crashed run must clear its marker too, or it advertises a gate forever")
    pause_at = src.index("gate_marker.write(")
    assert "[PAUSED]" in src[pause_at:pause_at + 400], "the marker must be written at the pause"
