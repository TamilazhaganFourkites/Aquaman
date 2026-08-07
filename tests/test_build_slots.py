"""Unit tests for the build-slot concurrency mechanism (nodes.py) — a machine-wide, flock-based
counting semaphore PAIRED with a live Docker-headroom check, gating prep_image / prep_container /
coder / harsh_reviewer / reachability_gate (the Docker-heavy stages that had zero cross-process
capacity protection before this; only sit_run's own SIT slot did). Modeled on test_graph.py's
existing monkeypatch-based Docker tests — no real Docker/subprocess calls, no real flock contention
across actual OS processes (that's covered by the manual smoke test in the plan doc, not here).

Run: `pip install -e '.[test]' && pytest tests/test_build_slots.py`
"""
from __future__ import annotations

import asyncio

import pytest

from ocean_pipeline import agents, config, nodes, schemas


@pytest.fixture(autouse=True)
def _isolated_slot_dir(tmp_path, monkeypatch):
    """Redirect the build-slot lock directory AND ARTIFACTS_ROOT (workspace_dir/artifacts_dir do
    real mkdir calls, e.g. from coder's own resume-hint check) to a fresh tmp_path per test — never
    the real /tmp/ocean-pipeline, matching test_graph.py's own `_install` convention. Clears the
    in-memory fd registry so tests never leak state into each other regardless of pass/fail order."""
    slot_dir = tmp_path / "build-slots"
    slot_dir.mkdir()
    monkeypatch.setattr(nodes, "_build_slot_dir", lambda: slot_dir)
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    nodes._BUILD_SLOT_FDS.clear()
    yield
    for exec_id in list(nodes._BUILD_SLOT_FDS):
        nodes._release_build_slot(exec_id)


def _plenty_headroom(monkeypatch):
    monkeypatch.setattr(nodes, "_docker_resources", lambda: (100.0, 8))
    monkeypatch.setattr(nodes, "_docker_used_gb", lambda exclude_name_substr="": 0.0)


def _no_headroom(monkeypatch):
    monkeypatch.setattr(nodes, "_docker_resources", lambda: (10.0, 4))
    monkeypatch.setattr(nodes, "_docker_used_gb", lambda exclude_name_substr="": 9.9)


# ── slot mechanism ──────────────────────────────────────────────────────────

def test_exhausting_slots_denies_further_acquire(monkeypatch):
    _plenty_headroom(monkeypatch)
    monkeypatch.setattr(config, "MAX_CONCURRENT_BUILDS", 2)
    assert nodes._try_acquire_build_slot("EXE-1") is True
    assert nodes._try_acquire_build_slot("EXE-2") is True
    assert nodes._try_acquire_build_slot("EXE-3") is False   # both slots taken
    nodes._release_build_slot("EXE-1")
    assert nodes._try_acquire_build_slot("EXE-3") is True    # freed slot now available


def test_idempotent_for_already_held_execution(monkeypatch):
    _plenty_headroom(monkeypatch)
    monkeypatch.setattr(config, "MAX_CONCURRENT_BUILDS", 1)
    assert nodes._try_acquire_build_slot("EXE-1") is True
    # Re-entering with the SAME execution_id must recognize the existing hold, not try (and fail)
    # to grab a second slot when MAX_CONCURRENT_BUILDS=1 is already held by this same id.
    assert nodes._try_acquire_build_slot("EXE-1") is True


def test_release_frees_the_slot_for_a_new_acquire(monkeypatch):
    _plenty_headroom(monkeypatch)
    monkeypatch.setattr(config, "MAX_CONCURRENT_BUILDS", 1)
    assert nodes._try_acquire_build_slot("EXE-1") is True
    nodes._release_build_slot("EXE-1")
    assert nodes._try_acquire_build_slot("EXE-2") is True


def test_low_headroom_denies_acquisition_even_with_a_free_slot(monkeypatch):
    # The fix for the "a fixed slot count doesn't map to real memory" gap: plenty of COUNT-based
    # slots free, but live Docker usage leaves no real headroom — must still deny.
    _no_headroom(monkeypatch)
    monkeypatch.setattr(config, "MAX_CONCURRENT_BUILDS", 5)
    repos = [{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]
    assert nodes._try_acquire_build_slot("EXE-1", repos) is False
    assert "EXE-1" not in nodes._BUILD_SLOT_FDS   # the flock slot grabbed along the way was released, not leaked


def test_release_is_a_safe_noop_for_an_execution_that_never_acquired():
    nodes._release_build_slot("EXE-never-acquired")   # must not raise


def test_acquire_build_slot_waits_then_succeeds(monkeypatch):
    _plenty_headroom(monkeypatch)
    monkeypatch.setattr(config, "MAX_CONCURRENT_BUILDS", 1)
    monkeypatch.setattr(config, "BUILD_SLOT_WAIT_SECONDS", 30)
    assert nodes._try_acquire_build_slot("EXE-holder") is True

    calls = {"n": 0}

    async def fake_sleep(_secs):
        calls["n"] += 1
        if calls["n"] == 1:
            nodes._release_build_slot("EXE-holder")   # free it on the waiter's first poll

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    result = asyncio.run(nodes._acquire_build_slot("EXE-waiter"))
    assert result == "acquired after waiting"
    assert "EXE-waiter" in nodes._BUILD_SLOT_FDS


# ── _any_docker_repo predicate ───────────────────────────────────────────────

def test_any_docker_repo_predicate():
    assert nodes._any_docker_repo([{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]) is True
    assert nodes._any_docker_repo([{"repo": "cloudqwest/ocean-worker", "language": "ruby"}]) is True
    assert nodes._any_docker_repo([{"repo": "cloudqwest/ocean-service", "language": "go"}]) is False
    assert nodes._any_docker_repo([]) is False
    assert nodes._any_docker_repo(None) is False


# ── node wiring ───────────────────────────────────────────────────────────

async def _fake_coder_agent(**kwargs):
    return schemas.CoderVerdict(branch="MM-1/x", files_changed=1, repo="cloudqwest/ocean-worker",
                                repo_dir="/tmp/x", pr_title="t", pr_body="b")


def test_coder_skips_build_slot_for_non_docker_repo(monkeypatch):
    calls = []

    async def fake_acquire(exec_id, target_repos=None):
        calls.append(exec_id)
        return "acquired"

    monkeypatch.setattr(nodes, "_acquire_build_slot", fake_acquire)
    monkeypatch.setattr(nodes, "_release_build_slot", lambda exec_id: calls.append(f"release:{exec_id}"))
    monkeypatch.setattr(agents, "run_agent", _fake_coder_agent)
    state = {"execution_id": "EXE1", "ticket_id": "MM-1",
             "target_repos": [{"repo": "cloudqwest/ocean-service", "language": "go"}]}
    asyncio.run(nodes.coder(state))
    assert calls == []   # never even attempted — a pure Go/Java ticket must never wait on this pool


def test_coder_acquires_and_releases_for_a_docker_repo_without_a_ready_container(monkeypatch):
    calls = []

    async def fake_acquire(exec_id, target_repos=None):
        calls.append(("acquire", exec_id))
        return "acquired"

    monkeypatch.setattr(nodes, "_acquire_build_slot", fake_acquire)
    monkeypatch.setattr(nodes, "_release_build_slot", lambda exec_id: calls.append(("release", exec_id)))
    monkeypatch.setattr(agents, "run_agent", _fake_coder_agent)
    state = {"execution_id": "EXE1", "ticket_id": "MM-1", "container_ready": False,
             "target_repos": [{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]}
    asyncio.run(nodes.coder(state))
    assert calls == [("acquire", "EXE1"), ("release", "EXE1")]


def test_coder_does_not_acquire_when_persistent_container_already_covers_it(monkeypatch):
    # prep_container already holds a build-slot spanning this whole phase (container_ready=True) —
    # coder must NOT try to acquire its own, separate hold on top of it.
    calls = []

    async def fake_acquire(exec_id, target_repos=None):
        calls.append(exec_id)
        return "acquired"

    monkeypatch.setattr(nodes, "_acquire_build_slot", fake_acquire)
    monkeypatch.setattr(agents, "run_agent", _fake_coder_agent)
    state = {"execution_id": "EXE1", "ticket_id": "MM-1", "container_ready": True,
             "target_repos": [{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]}
    asyncio.run(nodes.coder(state))
    assert calls == []


def test_coder_releases_on_worker_exception(monkeypatch):
    calls = []

    async def fake_acquire(exec_id, target_repos=None):
        calls.append(("acquire", exec_id))
        return "acquired"

    async def failing_agent(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(nodes, "_acquire_build_slot", fake_acquire)
    monkeypatch.setattr(nodes, "_release_build_slot", lambda exec_id: calls.append(("release", exec_id)))
    monkeypatch.setattr(agents, "run_agent", failing_agent)
    state = {"execution_id": "EXE1", "ticket_id": "MM-1", "container_ready": False,
             "target_repos": [{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]}
    with pytest.raises(RuntimeError):
        asyncio.run(nodes.coder(state))
    assert calls == [("acquire", "EXE1"), ("release", "EXE1")]   # released even though the worker raised


def test_prep_container_releases_slot_on_tag_unresolved(tmp_path, monkeypatch):
    # Ruby target + docker/tool/checkout all present, slot acquired -- but image-cache resolution
    # fails to produce a tag. Nothing came up, so the just-acquired hold must be released immediately,
    # not carried forward as a leaked slot with no container behind it.
    projects_root = tmp_path / "projects"
    (projects_root / "ocean-worker" / ".git").mkdir(parents=True)
    tool_path = tmp_path / "fk-aideveloper"
    (tool_path / "skills" / "ocean-qa-agent" / "tools").mkdir(parents=True)
    (tool_path / "skills" / "ocean-qa-agent" / "tools" / "ruby_image_cache.py").write_text("# stub\n")

    monkeypatch.setattr(config, "PERSISTENT_CONTAINER", True)
    monkeypatch.setattr(config, "PROJECTS_ROOT", projects_root)
    monkeypatch.setattr(config, "FK_AIDEVELOPER_DIR", tool_path)
    monkeypatch.setattr(nodes.shutil, "which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(nodes, "_image_cache_env", lambda: ({}, False))
    monkeypatch.setattr(nodes, "_try_acquire_build_slot", lambda exec_id, target_repos=None: True)
    released = []
    monkeypatch.setattr(nodes, "_release_build_slot", lambda exec_id: released.append(exec_id))

    class _FakeProc:
        returncode = 0

        async def communicate(self):
            return b"", b""   # empty stdout -> no tag resolved

    async def fake_create_subprocess_exec(*a, **kw):
        return _FakeProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    state = {"execution_id": "EXE1", "target_repos": [{"repo": "cloudqwest/ocean-worker", "build_env": "docker"}]}
    out = asyncio.run(nodes.prep_container(state))
    assert out == {"container_ready": False}
    assert released == ["EXE1"]
