"""The test suite must not write into the operator's real state. See tests/conftest.py.

This is the guard for a defect that had already happened and was still happening: 1026 of 1084
records in the real `~/.ocean-pipeline/timings.jsonl` were written by one unit test, and that file
is a measurement source, not scratch.

The shape of the bug is worth naming, because it recurs: the polluting test DID take `tmp_path` and
DID pass it to the function under test. It looked isolated. The write that escaped was a SECOND,
undeclared one inside the same call — telemetry appended to a durable path the test never mentioned.
A test that redirects the output it knows about proves nothing about the outputs it does not.
"""
from __future__ import annotations

import json
from pathlib import Path

from ocean_pipeline import config, gate_marker, kill_switch, metrics, report

_HOME_STATE = Path.home() / ".ocean-pipeline"


def test_no_durable_path_resolves_under_the_real_home_state_dir():
    """Every path the pipeline appends to during a run, checked as a set rather than one at a time —
    the point of failure was a path nobody had enumerated."""
    durable = {
        "config.TIMINGS_LOG": config.TIMINGS_LOG,
        "config.ARTIFACTS_ROOT": config.ARTIFACTS_ROOT,
        "gate_marker.GATES_DIR": gate_marker.GATES_DIR,
        "kill_switch.KILL_SWITCH_PATH": kill_switch.KILL_SWITCH_PATH,
    }
    for name, p in durable.items():
        assert _HOME_STATE not in Path(p).parents and Path(p) != _HOME_STATE, (
            f"{name} points into the operator's real state at {p} — a test run would pollute it")


def test_the_report_flow_that_caused_the_pollution_now_stays_contained(tmp_path):
    """The exact call that wrote 1026 records. `report.finish` appends to TIMINGS_LOG in addition to
    writing the artifacts the caller asked for; assert the line lands in the isolated log."""
    before = config.TIMINGS_LOG.read_text() if config.TIMINGS_LOG.exists() else ""

    metrics.reset()
    report.start("MM-ISOLATION", "EXE-isolation-probe")
    report.record("researcher", 1.0, {})
    report.finish({"final_status": "completed"}, tmp_path)

    assert config.TIMINGS_LOG.exists(), "the timings line went somewhere other than TIMINGS_LOG"
    added = config.TIMINGS_LOG.read_text()[len(before):]
    assert "EXE-isolation-probe" in added, "the run was not recorded in the isolated log"

    # And the real one is untouched by this test.
    real = _HOME_STATE / "timings.jsonl"
    if real.exists():
        assert "EXE-isolation-probe" not in real.read_text(), "the write escaped to the real log"


def test_a_timings_record_is_still_well_formed_under_isolation():
    """Redirecting a path must not quietly change what gets written — otherwise the isolation would
    hide a broken telemetry format instead of protecting a good one."""
    lines = [l for l in config.TIMINGS_LOG.read_text().splitlines() if l.strip()]
    assert lines, "no timings record was written under isolation"
    doc = json.loads(lines[-1])
    assert doc["execution_id"]
    assert "total_seconds" in doc or "stations" in doc or "nodes" in doc, doc.keys()
