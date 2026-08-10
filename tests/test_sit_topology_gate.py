"""sit-topology improvement #3 — one dumb evidence gate, plus #7's lesson feed.

`important-notes/sit-topology-provisioning-improvements.md` §4: *"If you do only two: #2 and #3."*
#3 was blocked on the /tmp → ~ move (T1.4 / plan item A2), because its evidence file lands in
ARTIFACTS_ROOT and measured survival there was 2 of 38 executions. A2 is done, so this is unblocked.

The gate is deliberately stupid: the skill writes `topology.json` with `required_real_services` and
`services_up` (it already runs `docker ps` and `local_callback_subsystem.py status`), and Aquaman
compares two sets. Topology INTELLIGENCE stays in ocean-qa-agent — re-implementing it here would
create a second source of truth that drifts, which is the exact defect recently removed from
nodes.py where three copies of one severity check left the un-migrated copy poisoning the failure
memory. The control plane gets evidence checking only, never domain logic.

Same shape as the two precedents beside it (`_read_unmocked_paths`, `_sut_write_activity`) and as
the junit gate: the control plane does not understand pytest either — it parses the XML and refuses
to take the model's word.
"""
from __future__ import annotations

import inspect
import json

import pytest

from ocean_pipeline import config, lessons, nodes

_EXE = "EXE-topo"


@pytest.fixture
def artifacts(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    d = config.artifacts_dir(_EXE)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _topology(artifacts, **doc):
    (artifacts / "topology.json").write_text(json.dumps(doc))


# --------------------------------------------------------------- the set comparison
@pytest.mark.parametrize("required,up,gap", [
    (["redis"], ["redis"], []),
    (["redis", "global_worker"], ["redis"], ["global_worker"]),
    (["redis"], [], ["redis"]),
    ([], ["redis"], []),
    ([], [], []),
    (["Redis"], ["redis"], []),            # case-insensitive, required side
    (["redis"], ["Redis"], []),            # ...AND the UP side — the required side already lowers,
                                           # so only this direction actually exercises `up_lc`
    (["redis"], ["REDIS", "x"], []),
    ([" redis "], ["redis"], []),          # whitespace-insensitive
    (["redis"], [" redis "], []),
])
def test_the_gap_is_a_plain_set_difference(required, up, gap):
    assert nodes.topology_gap(required, up) == gap


# --------------------------------------------------------------- reading the evidence
def test_a_present_file_is_read(artifacts):
    _topology(artifacts, required_real_services=["redis"], services_up=["redis"])
    req, up, why = nodes._read_topology(_EXE)
    assert (req, up, why) == (["redis"], ["redis"], "")


def test_a_missing_file_is_a_REASON_not_an_empty_pass(artifacts):
    """Absence of evidence is the thing this gate exists to catch. "No services were required" and
    "nobody wrote down which were" must not take the same answer."""
    req, up, why = nodes._read_topology(_EXE)
    assert req == [] and up == []
    assert "no topology.json" in why


def test_an_unreadable_file_is_a_reason(artifacts):
    (artifacts / "topology.json").write_text("{ not json")
    assert "unreadable" in nodes._read_topology(_EXE)[2]
    (artifacts / "topology.json").write_text('["a list, not an object"]')
    assert "not an object" in nodes._read_topology(_EXE)[2]


def test_unknown_topology_is_louder_than_a_missing_key(artifacts):
    """#2's catch-all. Guessing LOW is what produces the 65-minute mystery timeout; guessing high
    costs memory and is loud."""
    _topology(artifacts, **{"class": "unknown_topology",
                            "required_real_services": ["redis"], "services_up": ["redis"]})
    req, up, why = nodes._read_topology(_EXE)
    assert "unknown_topology" in why, "an unknown classification passed silently"
    assert nodes.topology_gap(req, up) == [], "and it is NOT a set-difference miss — a separate signal"


# --------------------------------------------------------------- the gate's verdict
def test_the_gate_returns_could_not_verify_never_passed_and_never_failed():
    """A missing service is a PROVISIONING gap, not a code fault. Calling it `failed` would send the
    coder to fix a diff that was never exercised."""
    src = inspect.getsource(nodes.sit_triage)
    i = src.index("SIT_TOPOLOGY_GAP")
    block = src[i - 1500:i + 400]
    assert '"failure_class": "could_not_verify"' in block
    assert '"automation_result": "passed"' not in block
    assert '"failure_class": "code_fault"' not in block, (
        "a provisioning gap must never be classified as a code fault")


def test_the_gate_sits_with_the_junit_gate_and_before_the_verdict_read():
    """Same class of refusal, same place: the control plane declines to score a run whose
    preconditions it cannot see were met."""
    src = inspect.getsource(nodes.sit_triage)
    assert src.index("SIT_JUNIT_MISSING") < src.index("SIT_TOPOLOGY_GAP") < src.index("verdict_path.exists()")


def test_the_gate_actually_reads_the_evidence_for_this_run():
    """Without this, replacing the read with empty literals leaves every assertion above green: the
    SIT_TOPOLOGY_GAP text still exists in the source, it just becomes unreachable."""
    src = inspect.getsource(nodes.sit_triage)
    assert "_read_topology(exec_id)" in src, "the gate does not read this run's topology evidence"
    assert "topology_gap(topo_required, topo_up)" in src, "the set comparison is not performed"


def test_an_unverified_topology_only_gates_when_something_was_required():
    """A run that declares nothing and requires nothing must not be blocked by the mere absence of
    the file — that would halt every run predating the skill change."""
    src = inspect.getsource(nodes.sit_triage)
    assert "if topo_gap or (topo_unverified and topo_required):" in src


# --------------------------------------------------------------- #7, the lesson feed
def test_the_miss_is_recorded_in_the_cross_ticket_memory():
    """#7: the mechanism already shipped, and this is the failure class that costs the most
    wall-clock — so it is the one worth remembering for the next ticket of the same shape."""
    src = inspect.getsource(nodes.sit_triage)
    assert "lessons.record_failure" in src


def test_the_lesson_call_actually_binds_to_the_real_signature():
    """THE test that matters here. A positional call would raise TypeError, the surrounding `except`
    would swallow it, and the memory would silently never record — the inertness pattern, inside the
    fix meant to remember this failure class. Caught exactly that during implementation."""
    sig = inspect.signature(lessons.record_failure)
    src = inspect.getsource(nodes.sit_triage)
    i = src.index("lessons.record_failure")
    call = src[i:src.index("        except", i)]
    passed = {l.split("=")[0].strip() for l in call.splitlines()
              if "=" in l and not l.strip().startswith("#")}
    required = {p for p, v in sig.parameters.items() if v.default is inspect.Parameter.empty}
    assert required <= passed, f"record_failure would raise TypeError; missing {required - passed}"


def test_a_broken_memory_never_fails_the_station():
    src = inspect.getsource(nodes.sit_triage)
    i = src.index("lessons.record_failure")
    assert "except Exception" in src[i:i + 900], "a memory failure could fail the station"


# --------------------------------------------------------------- #4, preflight the REAL topology
def test_the_preflight_is_sized_against_required_services_not_target_repos():
    """Gap D. `target_repos` is the researcher's Station-0 CANDIDATE list — routinely over-scoped
    with read-only repos, AND blind to services the ticket never changed but the test still needs
    (the ES indexer, the delivery chain). Both errors are live in opposite directions."""
    src = inspect.getsource(nodes.sit_run)
    assert "_read_topology(exec_id)" in src, "sit_run never reads the real topology"
    assert "preflight_scope" in src
    i = src.index("_docker_preflight_reason(")
    call = src[i:src.index(")", i) + 1]
    assert "preflight_scope" in call, f"the budget is still sized against target_repos: {call}"


def test_the_preflight_falls_back_to_target_repos_when_no_topology_exists():
    """Back-compat: a run predating topology.json must still be preflighted, not skipped."""
    src = inspect.getsource(nodes.sit_run)
    assert 'else state.get("target_repos")' in src, "no fallback — older runs lose their preflight"


def test_the_budget_sizer_reads_a_PHASE_ONE_file(tmp_path, monkeypatch):
    """`sit_run` sizes the Docker budget from `required_real_services` BEFORE it dispatches the
    skill — 49 lines before, in the same function. So the file must already exist by then, carrying
    the classifier's half; `services_up` arrives later, in Station 2 Step 6b.

    Moving the whole write into Station 2 broke this: on a first attempt the read found nothing and
    the budget fell back to `target_repos`, which this code's own comment calls "routinely
    over-scoped … and blind to services the ticket never changed but the test still needs". That
    silently reverted sit-topology #4."""
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path)
    d = tmp_path / "EXE-phase1"
    d.mkdir(parents=True)
    # Phase 1 exactly as Station 0 writes it — no `services_up` key at all.
    (d / "topology.json").write_text(json.dumps(
        {"class": "callback_e2e",
         "required_real_services": ["fkrelay", "notification-worker", "tracking-service"]}))

    required, up, why = nodes._read_topology("EXE-phase1")
    assert required == ["fkrelay", "notification-worker", "tracking-service"], (
        "the budget sizer cannot see the classifier's answer, so it falls back to target_repos")
    assert up == [], "phase 1 carries no observation, by design"
    assert why == "", "a phase-1 file is not 'unverified' — Step 6b has simply not run yet"


def test_a_phase_one_file_still_gates_at_triage():
    """The other half of the same contract. If a run never reaches Step 6b, the file stays phase-1
    and `services_up` is empty — so the gate sees every required service as down and returns
    could_not_verify. That is CORRECT: the run did not complete Station 2, so nothing was verified.
    Asserted so nobody 'fixes' it by defaulting `services_up` to the required set."""
    gap = nodes.topology_gap(["fkrelay", "notification-worker"], [])
    assert gap == ["fkrelay", "notification-worker"], (
        "a run that never observed its stack must not read as verified")
