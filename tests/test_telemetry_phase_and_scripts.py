"""Three of the audit's "production changes with NO test at all", covered.

`telemetry._STATUS["coverage_gap"]`, `eval_routing.main()` and `gan_effect.main()` shipped with no
test between them. The two `main()`s are the ones that matter most for a reason peculiar to them:
they are ANALYSIS scripts, so a crash is loud but a *wrong shape* is not — nobody re-derives the
number they print. And a script that cannot even be imported is a measurement nobody can reproduce,
which is precisely the criticism the router-eval baseline was written to answer.

Read-only throughout. `gan_effect.main()` is driven against a tmp artifacts root, not the
operator's; `eval_routing.main()` is never CALLED here (it would spend real SDK sessions) — only
imported and inspected.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from ocean_pipeline import telemetry

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load(name: str):
    """One shared implementation, in conftest — this idiom was written out three times."""
    from conftest import load_module_by_path

    return load_module_by_path(_SCRIPTS / f"{name}.py", f"_script_{name}")


# ------------------------------------------------------------------ telemetry phase annotation
def test_the_coverage_gap_event_is_an_annotation_not_a_terminal_status():
    """B1's coverage gap is emitted INSIDE harsh_review, which opens and closes its own station
    lifecycle around it. Mapping it to a terminal status would close station 5 early, so the real
    outcome line that follows would land on a station the telemetry already considers finished."""
    assert telemetry._STATUS["coverage_gap"] == "note"


def test_every_status_value_is_one_the_consumer_understands():
    """A free-text status is silently non-matching downstream — the same shape as an unmapped
    severity. Enumerated here so a new event with a typo'd status fails at commit time, not in a
    dashboard nobody checks."""
    allowed = {"note", "completed", "failed", "skipped", "blocked", "running", "started"}
    bad = {k: v for k, v in telemetry._STATUS.items() if v not in allowed}
    assert not bad, f"unknown telemetry status value(s): {bad}"


# ------------------------------------------------------------------ the two analysis scripts
def test_eval_routing_imports_and_declares_a_main():
    """Not executed — it drives real SDK sessions. But an import error means the measurement
    behind `research.md`'s MEASURED claim cannot be reproduced by anyone, which is the whole
    reason the corpus and the harness were committed."""
    mod = _load("eval_routing")
    assert callable(mod.main)


def test_eval_routing_does_not_name_a_corpus_ticket_in_the_graded_prompt():
    """The contamination that scored a first version 31/31. Guarded in `test_eval_corpus.py` for
    the worker prompt; asserted here for the HARNESS, which is the other place a corpus ticket can
    appear beside its own answer.

    Scoped to the whole file, docstrings included, deliberately. A docstring is not sent to the
    graded worker, so a corpus row used as a format example is not live contamination — but that
    "harmless here" reasoning is exactly what produced the 31/31 run, and the standing rule it left
    behind ("illustrations must come from outside the corpus") has no exception clause. A
    fictitious id costs nothing and needs no judgement call from the next reader."""
    src = (_SCRIPTS / "eval_routing.py").read_text()
    corpus = _SCRIPTS / "eval_corpus.jsonl"
    if not corpus.exists():
        pytest.skip("corpus not present")
    tickets = {json.loads(l)["ticket_id"] for l in corpus.read_text().splitlines()
               if l.strip() and not l.lstrip().startswith("#")}
    leaked = sorted(t for t in tickets if t in src)
    assert not leaked, f"the eval harness names corpus tickets: {leaked}"


def test_gan_effect_runs_over_an_empty_artifacts_root_without_inventing_a_result(tmp_path):
    """Zero observations must produce zero observations. An analyser that prints a comparison from
    an empty corpus is worse than one that crashes: `qa_gan_residual_gaps` was missing from the
    report for its whole life, so EVERY run landed in the `unknown` arm and both real arms were
    empty — and nothing said so."""
    # Driven as a SUBPROCESS with the env var set — the way a human runs it. An earlier version
    # also imported the module and assigned `mod.artifacts_root`, which did nothing at all: the
    # subprocess below has its own interpreter and never sees those assignments.
    proc = subprocess.run([sys.executable, str(_SCRIPTS / "gan_effect.py")],
                          capture_output=True, text=True, timeout=120,
                          env={**__import__("os").environ,
                               "OCEAN_PIPELINE_ARTIFACTS": str(tmp_path)})
    assert proc.returncode == 0, f"gan_effect crashed on an empty corpus:\n{proc.stderr[-2000:]}"
    out = (proc.stdout + proc.stderr).lower()
    # A real claim, not `"0" in out` — which almost any output satisfies, including a report of
    # results the analyser could not have had.
    assert re.search(r"\b0\b|\bno (runs|data|observations)\b|empty", out), (
        f"an empty corpus produced no statement of its emptiness: {out[:300]!r}")
    assert "comparison" not in out or "0" in out, (
        "the analyser printed a comparison from zero observations")


def test_gan_effect_declares_its_pre_registration_and_flags_the_proxy(tmp_path):
    """A pre-registration is only worth something if it is visible beside the result. This is the
    file where the primary variable was silently substituted once already."""
    # DRIVEN, not grepped. `"primary_is_proxy" in src` is satisfied by the comment explaining it —
    # which is the trap `test_audit_regressions.py` catalogues, and which this file was making
    # while that file condemned it.
    mod = _load("gan_effect")
    root = tmp_path / "artifacts"
    for name, doc in (("EXE-real", {"execution_id": "EXE-real", "final_status": "completed",
                                    "automation_result": "passed"}),
                      ("EXE-proxy", {"execution_id": "EXE-proxy", "final_status": "completed"})):
        d = root / name
        (d / "workspace").mkdir(parents=True)
        (d / "run-report.json").write_text(json.dumps(doc))
    by_id = {r.get("execution_id"): r for r in mod._runs(root)}
    assert by_id, "the analyser found no runs — this assertion would be vacuous"
    assert by_id["EXE-real"]["primary_is_proxy"] is False
    assert by_id["EXE-proxy"]["primary_is_proxy"] is True, (
        "a run scored on the FALLBACK variable is indistinguishable from one scored on the real one")
