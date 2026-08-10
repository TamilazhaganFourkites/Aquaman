"""Session-wide isolation of every DURABLE path the pipeline writes to.

There was no conftest.py, and `test_report_full_flow_writes_json_and_markdown` (test_graph.py:2935)
passed `tmp_path` for the artifacts directory while `report.finish` ALSO appends a line to
`config.TIMINGS_LOG` — which nothing redirected. Measured 2026-08-08: **1026 of 1084 records in
`~/.ocean-pipeline/timings.jsonl` are `EXE-report-test`**, and the count grew by 31 during a single
session's test runs. That file is not scratch data: the SIT-duration figures used to weigh the
multi-repo gate design were measured from it, over the ~5% of records that are real (re-derived
2026-08-08 from `station_seconds["sit_run"]`: 0.07-1.82h, median 0.58h, n=16 across 15 runs).

Those 1026 records were purged on 2026-08-08, after this isolation landed, with the pre-purge file
kept at `~/.ocean-pipeline/timings.jsonl.pre-purge-2026-08-08`. The purge cost zero `sit_run`
observations -- 16 before, 16 after.

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
import sys
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


# The single largest uncovered durable write, and it is not a file. `telemetry.py` POSTs station
# and execution rows to https://rca-app-api.fourkites.com/mcp-aidev/ with `Authorization: Bearer
# $RCA_TOKEN` whenever that variable is set. Nothing in the suite cleared it, so any test that drives
# a real node would write into PRODUCTION `aidev_db`. It is inert only because the variable happens
# to be absent from local .env files — an operator who exports it turns their test run into
# production telemetry, silently and with no failure.
os.environ.pop("RCA_TOKEN", None)

# Same class as RCA_TOKEN. `monitor/app.py` imports `jira_client`, whose module-level
# `_load_dotenv()` `setdefault`s the real JIRA_* values from `monitor/.env` -- so importing the
# monitor arms every Jira writer in the process. The suite must not be able to write to a production
# system because someone reordered an import. (The monitor fixture re-scrubs after that import.)
for _cred in ("JIRA_API_TOKEN", "JIRA_BASE_URL", "JIRA_EMAIL"):
    os.environ.pop(_cred, None)


@pytest.fixture(scope="session", autouse=True)
def _isolate_durable_paths():
    """Step 2 — re-point the already-resolved module attributes at the session temp dir."""
    from ocean_pipeline import config, gate_marker, kill_switch, telemetry

    config.TIMINGS_LOG = Path(_ENV["OCEAN_PIPELINE_TIMINGS_LOG"])
    config.ARTIFACTS_ROOT = Path(_ENV["OCEAN_PIPELINE_ARTIFACTS"])
    # DERIVED from ARTIFACTS_ROOT at import (config.py), so reassigning the root alone leaves the
    # checkpointer pointed at the operator's real ~/.ocean-pipeline/artifacts/checkpoints.sqlite --
    # exactly the already-imported case Step 1 exists to cover.
    config.CHECKPOINT_DB = str(Path(_ENV["OCEAN_PIPELINE_ARTIFACTS"]) / "checkpoints.sqlite")
    gate_marker.GATES_DIR = Path(_ENV["OCEAN_PIPELINE_GATES_DIR"])
    kill_switch.KILL_SWITCH_PATH = Path(_ENV["OCEAN_PIPELINE_KILL_SWITCH"])
    # Read into a module global at import, so popping the env var above is not enough on its own.
    telemetry.RCA_TOKEN = ""
    # Frozen at import, exactly like RCA_TOKEN — popping the env var above is not enough on its own.
    from ocean_pipeline import jira as _jira
    _jira.JIRA_API_TOKEN = ""

    # `monitor/jira_client` is the THIRD module with this shape, and the one the monitor tests pull
    # in. Imported and disarmed HERE, at session start, so it is cached in `sys.modules` already
    # scrubbed — any later import returns this object rather than re-running `_load_dotenv()`.
    # Doing it in the monitor fixture instead made the scrub order-dependent: this file's own guard
    # runs before that fixture and would import a freshly-armed copy.
    _monitor = Path(__file__).resolve().parents[1] / "monitor"
    if _monitor.is_dir():
        sys.path.insert(0, str(_monitor))
        try:
            import jira_client as _jc

            _jc.JIRA_API_TOKEN = ""
            _jc.JIRA_EMAIL = ""
            # AND the env again: importing that module ran `_load_dotenv()`, which `setdefault`s
            # JIRA_* from `monitor/.env` straight back into os.environ — undoing the pop at the top
            # of this file. Order matters, and getting it backwards re-armed every subprocess.
            for _cred in ("JIRA_API_TOKEN", "JIRA_BASE_URL", "JIRA_EMAIL"):
                os.environ.pop(_cred, None)
        except ImportError:
            pass
        finally:
            sys.path.remove(str(_monitor))
    yield


# `config.FK_AIDEVELOPER_DIR` is deliberately NOT redirected: tests legitimately read real files
# under it, and a temp dir would turn those into vacuous skips. The hazard is WRITES --
# `nodes.qa_scenarios` does `mkdir()` + `.unlink()` under `<fk-aideveloper>/memory/tickets/`, a
# mutation of a different repo's working tree. So: detect rather than redirect. A detector reports
# after the fact; that is the trade for a path whose reads must stay real.
@pytest.fixture(scope="session", autouse=True)
def _no_test_writes_into_the_sibling_repo():
    from ocean_pipeline import config

    tickets = Path(str(config.FK_AIDEVELOPER_DIR)) / "memory" / "tickets"
    before = {p.name for p in tickets.iterdir()} if tickets.is_dir() else None
    yield
    if before is None:
        return                      # the sibling checkout is absent (CI) -- nothing to protect
    after = {p.name for p in tickets.iterdir()} if tickets.is_dir() else set()
    created = sorted(after - before)
    # BOTH directions. `after - before` cannot see a DELETION, and the hazard named above is
    # `nodes.qa_scenarios` doing `parent.mkdir()` + **`.unlink()`** — a test that only unlinks a
    # real ticket's JSON left this green while the file was gone from the other repo. That is the
    # more damaging half: a created file shows up in `git status`, a deleted one shows up as a
    # change nobody made.
    removed = sorted(before - after)
    assert not created and not removed, (
        f"the test suite MUTATED the sibling fk-aideveloper checkout at {tickets} — "
        f"created: {created or 'none'}; DELETED: {removed or 'none'}. Monkeypatch "
        f"config.qa_scenarios_path / config.sit_state_path in the offending test "
        f"(test_graph.py:79 is the pattern) rather than letting a node resolve the real path. "
        f"NOTE: this fixture cannot tell your test apart from a real pipeline run or another "
        f"session writing here concurrently — check `git -C <fk-aideveloper> status` before "
        f"assuming the suite is the author.")


@pytest.fixture(scope="session")
def session_tmp() -> Path:
    """The isolated root, for a test that wants to assert against it."""
    return _SESSION_TMP


def load_module_by_path(path, name: str):
    """Import a module that is not on `sys.path` — a repo script or `monitor/app.py`.

    Two non-obvious steps, which is why it kept being got slightly wrong: register the module in
    `sys.modules` BEFORE `exec_module` (without it `@dataclass` resolves string annotations through
    `sys.modules[cls.__module__]`, gets None, and raises deep inside dataclasses.py), and put the
    script's own directory on `sys.path` first (`monitor/app.py` does `import store`, a flat
    sibling).

    `test_eval_routing.py`, `test_gan_steps234.py` and `test_telemetry_phase_and_scripts.py`
    use this. `test_monitor_slot_release.py`
    deliberately does NOT: it has to set `store.DB_PATH` to a tmp path between the import of
    `store` and the execution of `app.py`, because `app.py` runs `store.reconcile_stale()` at
    module level and would otherwise mark the operator's live runs interrupted. That ordering
    cannot be expressed through this helper, and forcing it through would hide the one thing that
    fixture exists to do. A fourth copy also existed in `test_audit_regressions.py`; the test
    around it was a duplicate of one in `test_telemetry_phase_and_scripts.py` and was deleted.
    """
    import importlib.util
    import sys
    from pathlib import Path as _P

    path = _P(path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    sys.modules[name] = mod
    try:
        spec.loader.exec_module(mod)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    finally:
        sys.path.remove(str(path.parent))
    return mod
