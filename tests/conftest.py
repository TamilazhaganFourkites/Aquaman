"""Session-wide isolation of every DURABLE path the pipeline writes to.

There was no conftest.py, and `test_report_full_flow_writes_json_and_markdown` (test_graph.py:2935)
passed `tmp_path` for the artifacts directory while `report.finish` ALSO appends a line to
`config.TIMINGS_LOG` — which nothing redirected. Measured 2026-08-08: **1026 of 1084 records in
`~/.ocean-pipeline/timings.jsonl` are `EXE-report-test`**, and the count grew by 31 during a single
session's test runs. That file is not scratch data: the SIT-duration figures used to weigh the
multi-repo gate design (0.03-2.97h, median 0.55h) were measured from it, over the ~5% of records
that are real.

Fixing only the one test would leave the class open — three sibling durable paths are equally
reachable (`ARTIFACTS_ROOT`, `gate_marker.GATES_DIR`, `kill_switch.KILL_SWITCH_PATH`), and each new
one added is a fresh chance to pollute. So the isolation is session-wide and keyed to the four
env vars those paths already read.

Belt and braces, because these are resolved at IMPORT time:

  1. The env vars are set at conftest MODULE level. pytest imports conftest before any test module,
     so a later `import ocean_pipeline` resolves against the temp paths.
  2. An autouse session fixture ALSO reassigns the module attributes, which covers the case where
     something imported ocean_pipeline before this conftest ran (a plugin, or a future
     tests/<subdir>/conftest.py). Without step 2 the isolation would be silently order-dependent —
     the failure mode being "passes locally, pollutes on the machine where import order differs".

Tests that deliberately exercise the env-var contract itself (test_gate_marker.py's
`test_the_gates_dir_is_flat_env_overridable_and_not_under_tmp`) pop these vars inside their own
try/finally and restore them, so they are unaffected.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Step 1 — before ocean_pipeline is imported by any test module.
_SESSION_TMP = Path(tempfile.mkdtemp(prefix="ocean-pipeline-tests-"))
_ENV = {
    "OCEAN_PIPELINE_TIMINGS_LOG": str(_SESSION_TMP / "timings.jsonl"),
    "OCEAN_PIPELINE_ARTIFACTS": str(_SESSION_TMP / "artifacts"),
    "OCEAN_PIPELINE_GATES_DIR": str(_SESSION_TMP / "gates"),
    "OCEAN_PIPELINE_KILL_SWITCH": str(_SESSION_TMP / "HALT"),
}
for _k, _v in _ENV.items():
    os.environ[_k] = _v


@pytest.fixture(scope="session", autouse=True)
def _isolate_durable_paths():
    """Step 2 — re-point the already-resolved module attributes at the session temp dir."""
    from ocean_pipeline import config, gate_marker, kill_switch

    config.TIMINGS_LOG = Path(_ENV["OCEAN_PIPELINE_TIMINGS_LOG"])
    config.ARTIFACTS_ROOT = Path(_ENV["OCEAN_PIPELINE_ARTIFACTS"])
    gate_marker.GATES_DIR = Path(_ENV["OCEAN_PIPELINE_GATES_DIR"])
    kill_switch.KILL_SWITCH_PATH = Path(_ENV["OCEAN_PIPELINE_KILL_SWITCH"])
    yield


@pytest.fixture(scope="session")
def session_tmp() -> Path:
    """The isolated root, for a test that wants to assert against it."""
    return _SESSION_TMP
