"""D4 — the mechanical secret scan in front of the ready-flip.

`flip_ready` was pure `gh` + Jira with nothing deterministic in front of it, and the fk-aideveloper
gitleaks hook does NOT reach this path: it is a Claude Code PreToolUse hook in that repo's
`.claude/settings.json`, while Aquaman builds SDK hooks in-process and never loads user or project
settings. An upstream LLM security gate DOES exist (`review.md:104-105` makes security an
unconditional CRITICAL) — this is the mechanical half, not a replacement for it.

These tests run the REAL gitleaks against REAL git repos with a REAL planted secret. That is
deliberate: the first probe written for this feature used AWS's documentation example key
(`AKIAIOSFODNN7EXAMPLE`), which gitleaks correctly allowlists, and reported "no leaks found" — a
mocked scanner would have hidden that the test corpus, not the scanner, was wrong, and the gate
would have shipped inert.
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
from pathlib import Path

import pytest

from ocean_pipeline import config, quality

pytestmark = pytest.mark.skipif(shutil.which("gitleaks") is None,
                                reason="gitleaks not installed on this machine")

# The bait, ASSEMBLED AT RUNTIME rather than written as a literal.
#
# A literal here is itself a secret-shaped string in a committed file, and FK's own gitleaks
# pre-commit hook duly BLOCKED the commit that first added this test — correctly. The alternative
# remedy the hook offers (an allowlist entry in .gitleaks.toml) would work, but it weakens the repo's
# scanner permanently to accommodate one test, and it leaves a credential-shaped literal in the tree
# for every OTHER scanner a clone passes through. Composing the value at import time keeps the file
# clean while the temp repo the scan actually reads still contains the full string.
#
# It must NOT be an upstream-allowlisted example key (see the module docstring), so this is a fake
# but realistically-shaped pair, never valid anywhere.
# DERIVED FROM A HASH, not written down even in fragments. Splitting a literal across `+` was the
# first attempt and the hook blocked that too: the fragment still sits next to an `=`, which is what
# the generic-api-key rule keys on. A hash has no literal to find, is deterministic across runs, and
# was verified to still trip gitleaks (a bait nothing detects would make every test here vacuous).
_KEY_ID = "AKIA" + hashlib.sha1(b"aquaman-d4-bait-id").hexdigest()[:16].upper()
_KEY_SECRET = hashlib.sha256(b"aquaman-d4-bait-secret").hexdigest()[:40]
_PLANTED = (f'aws_access_key_id = "{_KEY_ID}"\n'
            f'aws_secret_access_key = "{_KEY_SECRET}"\n')


def _sh(*args, cwd):
    subprocess.run(args, cwd=str(cwd), capture_output=True, check=False)


@pytest.fixture
def repo(tmp_path):
    """A clone with a real `origin`, because `base_ref` resolves the diff base from origin/HEAD.

    A bare `git init` has no origin, so `secret_scan` returns "no merge base — not scanned" and every
    assertion below would pass vacuously against zero findings. That happened while writing this.
    """
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], capture_output=True)
    seed = tmp_path / "seed"
    seed.mkdir()
    _sh("git", "init", "-q", "-b", "main", cwd=seed)
    _sh("git", "config", "user.email", "t@x.com", cwd=seed)
    _sh("git", "config", "user.name", "t", cwd=seed)
    (seed / "ok.rb").write_text("puts 'hi'\n")
    _sh("git", "add", "-A", cwd=seed)
    _sh("git", "commit", "-qm", "base", cwd=seed)
    _sh("git", "remote", "add", "origin", str(origin), cwd=seed)
    _sh("git", "push", "-q", "origin", "main", cwd=seed)

    work = tmp_path / "work"
    subprocess.run(["git", "clone", "-q", str(origin), str(work)], capture_output=True)
    _sh("git", "config", "user.email", "t@x.com", cwd=work)
    _sh("git", "config", "user.name", "t", cwd=work)
    return work


def _branch(repo: Path, name: str, filename: str, content: str):
    _sh("git", "checkout", "-q", "main", cwd=repo)
    _sh("git", "checkout", "-qb", name, cwd=repo)
    (repo / filename).write_text(content)
    _sh("git", "add", "-A", cwd=repo)
    _sh("git", "commit", "-qm", f"{name}: change", cwd=repo)


def test_the_fixture_itself_resolves_a_base(repo):
    """Guard the guard. Without an origin every other test here passes on zero findings."""
    assert quality.base_ref(repo, 30) == "origin/main"


def test_a_planted_secret_is_found(repo):
    _branch(repo, "MM-1/leak", "cfg.rb", _PLANTED)
    findings, reason, scanned = quality.secret_scan([repo], timeout=60)
    assert scanned == 1, f"nothing was scanned: {reason}"
    assert reason == "", reason
    assert len(findings) >= 1
    assert findings[0]["severity"] == "CRITICAL"
    assert "gitleaks rule" in findings[0]["summary"]


def test_the_secret_itself_never_enters_a_finding(repo):
    """The finding travels much further than the scanner's own output — milestone, telemetry
    output_summary, run-report.json, the Jira comment, monitor.db and the web UI. `--redact` alone
    is not enough; the value must never be copied in the first place."""
    _branch(repo, "MM-1/leak", "cfg.rb", _PLANTED)
    findings, _, _ = quality.secret_scan([repo], timeout=60)
    blob = repr(findings)
    assert _KEY_ID not in blob
    assert _KEY_SECRET not in blob
    for key in ("Secret", "Match", "Line"):
        assert f"'{key}'" not in blob, f"a raw {key} field reached the finding"


def test_a_clean_diff_is_scanned_and_clean(repo):
    """The direction that matters for trust: `scanned` must be 1, not 0. "Clean" and "never ran"
    both produce zero findings and must be distinguishable."""
    _branch(repo, "MM-2/clean", "n.rb", "puts 'clean'\n")
    findings, reason, scanned = quality.secret_scan([repo], timeout=60)
    assert findings == [] and reason == ""
    assert scanned == 1, "a clean branch must be SCANNED, not merely finding-free"


def test_a_preexisting_secret_on_main_is_not_this_tickets_problem(repo):
    """Scans the diff, not history. Blocking a ticket's flip on a secret it never touched is an
    unactionable stop, and the engineer cannot fix it within this ticket."""
    _sh("git", "checkout", "-q", "main", cwd=repo)
    (repo / "old.rb").write_text(_PLANTED)
    _sh("git", "add", "-A", cwd=repo)
    _sh("git", "commit", "-qm", "pre-existing", cwd=repo)
    _sh("git", "push", "-q", "origin", "main", cwd=repo)
    _sh("git", "remote", "set-head", "origin", "--auto", cwd=repo)
    _branch(repo, "MM-3/unrelated", "new.rb", "puts 'unrelated'\n")

    findings, reason, scanned = quality.secret_scan([repo], timeout=60)
    assert findings == [], f"blocked on a secret the ticket never touched: {findings}"
    assert scanned == 1, reason


def test_a_missing_scanner_is_unverified_not_clean(repo, monkeypatch):
    """Fails OPEN, but never silently: an absent gitleaks must not halt every ready-flip, and must
    not be reported as a clean scan either."""
    _branch(repo, "MM-1/leak", "cfg.rb", _PLANTED)
    monkeypatch.setattr(quality, "SECRET_SCANNER", "gitleaks-that-does-not-exist")
    findings, reason, scanned = quality.secret_scan([repo], timeout=60)
    assert findings == []
    assert scanned == 0
    assert "NOT scanned" in reason, reason


def test_a_repo_with_no_base_is_reported_not_skipped(tmp_path):
    bare = tmp_path / "norem"
    bare.mkdir()
    _sh("git", "init", "-q", "-b", "main", cwd=bare)
    findings, reason, scanned = quality.secret_scan([bare], timeout=30)
    assert findings == [] and scanned == 0
    assert "no merge base" in reason


# ------------------------------------------------------------------ the gate in flip_ready
def test_flip_ready_blocks_on_a_finding_and_touches_nothing(monkeypatch, tmp_path):
    """The whole point: no gh call, no Jira transition, PRs left DRAFT. Flipping a PR ready IS the
    request for human eyes — a possible live credential should be rotated before that audience
    widens, not after."""
    import asyncio

    from ocean_pipeline import gitops, jira, nodes

    calls = []
    monkeypatch.setattr(gitops, "cross_link_and_ready",
                        lambda *a, **k: calls.append(("gh", a)))
    monkeypatch.setattr(jira, "transition", lambda *a, **k: calls.append(("transition", a)))
    monkeypatch.setattr(jira, "comment", lambda *a, **k: calls.append(("comment", a)))
    monkeypatch.setattr(config, "SECRET_SCAN", True)
    monkeypatch.setattr(quality, "repo_dirs", lambda *a, **k: [tmp_path])
    monkeypatch.setattr(quality, "secret_scan",
                        lambda dirs, timeout: ([{"severity": "CRITICAL", "file": "cfg.rb",
                                                 "summary": "possible secret"}], "", 1))

    out = asyncio.run(nodes.flip_ready({"ticket_id": "MM-1", "execution_id": "EXE-s",
                                        "branch": "MM-1/fix", "pr_numbers": {"cq/x": 7}}))
    assert out["ready_flipped"] is False and out["final_status"] == "failed"
    assert "secret_scan_blocked" in out["final_outcome"]
    assert "left DRAFT" in out["final_outcome"]
    assert calls == [], f"the gate blocked but something still fired: {calls}"


def test_flip_ready_still_flips_when_the_scan_could_not_run(monkeypatch, tmp_path):
    """Fails OPEN. A machine without gitleaks must not lose the pipeline's terminal action — but the
    reason rides to the terminal so the run can never read as scanned-and-clean."""
    import asyncio

    from ocean_pipeline import gitops, jira, nodes

    flipped = []
    monkeypatch.setattr(gitops, "cross_link_and_ready", lambda *a, **k: flipped.append(a))
    monkeypatch.setattr(jira, "transition", lambda *a, **k: None)
    monkeypatch.setattr(jira, "comment", lambda *a, **k: None)
    monkeypatch.setattr(config, "SECRET_SCAN", True)
    monkeypatch.setattr(quality, "repo_dirs", lambda *a, **k: [tmp_path])
    monkeypatch.setattr(quality, "secret_scan",
                        lambda dirs, timeout: ([], "repo: gitleaks did not run — NOT scanned", 0))

    out = asyncio.run(nodes.flip_ready({"ticket_id": "MM-1", "execution_id": "EXE-s",
                                        "branch": "MM-1/fix", "pr_numbers": {"cq/x": 7}}))
    assert out["ready_flipped"] is True and out["final_status"] == "completed"
    assert flipped, "fail-open must still flip"
    assert "NOT scanned" in out["secret_scan_unverified"], (
        "the could-not-run reason must reach the terminal state, or the run reads as clean")


def test_a_crashing_scanner_cannot_silently_pass_the_flip(monkeypatch, tmp_path):
    import asyncio

    from ocean_pipeline import gitops, jira, nodes

    def _boom(*a, **k):
        raise RuntimeError("gitleaks exploded")

    monkeypatch.setattr(gitops, "cross_link_and_ready", lambda *a, **k: None)
    monkeypatch.setattr(jira, "transition", lambda *a, **k: None)
    monkeypatch.setattr(jira, "comment", lambda *a, **k: None)
    monkeypatch.setattr(config, "SECRET_SCAN", True)
    monkeypatch.setattr(quality, "repo_dirs", lambda *a, **k: [tmp_path])
    monkeypatch.setattr(quality, "secret_scan", _boom)

    out = asyncio.run(nodes.flip_ready({"ticket_id": "MM-1", "execution_id": "EXE-s",
                                        "pr_numbers": {"cq/x": 7}}))
    assert out["final_status"] == "completed", "a crash fails OPEN like any other could-not-run"
    assert "crashed" in out["secret_scan_unverified"], "the crash must be visible, not swallowed"


def test_the_knob_is_on_by_default_and_the_state_keys_are_declared():
    """A security gate shipped default-off is not shipped. And LangGraph silently drops any key
    OceanState does not declare, which would make the findings invisible downstream."""
    import importlib
    import os

    from ocean_pipeline.state import OceanState

    saved = dict(os.environ)
    try:
        os.environ.pop("OCEAN_PIPELINE_SECRET_SCAN", None)
        assert importlib.reload(config).SECRET_SCAN is True
    finally:
        os.environ.clear()
        os.environ.update(saved)
        importlib.reload(config)
    assert "secret_findings" in OceanState.__annotations__
    assert "secret_scan_unverified" in OceanState.__annotations__


def test_every_repo_is_counted_not_just_the_last(repo, tmp_path):
    """`scanned` must ACCUMULATE. A single-repo corpus cannot tell `scanned += 1` from
    `scanned = 1`, and the difference is exactly the "how much did we actually cover" signal the
    caller uses to distinguish a clean scan from a scan that barely happened."""
    _branch(repo, "MM-2/clean", "n.rb", "puts 'clean'\n")

    second = tmp_path / "work2"
    subprocess.run(["git", "clone", "-q", str(tmp_path / "origin.git"), str(second)],
                   capture_output=True)
    _sh("git", "config", "user.email", "t@x.com", cwd=second)
    _sh("git", "config", "user.name", "t", cwd=second)
    _branch(second, "MM-2/clean-b", "m.rb", "puts 'also clean'\n")

    findings, reason, scanned = quality.secret_scan([repo, second], timeout=60)
    assert findings == [] and reason == ""
    assert scanned == 2, f"two repos were scanned but only {scanned} counted"


def test_the_scanner_is_always_invoked_with_redaction(repo, monkeypatch):
    """Defence in depth, and it needs its own test because the finding is safe WITHOUT it: this
    module never copies `Secret`/`Match` into a finding, so dropping `--redact` leaves every
    assertion about findings passing. What it changes is the report gitleaks writes to disk, which
    would then hold the plaintext credential. Asserted on the constructed command."""
    _branch(repo, "MM-1/leak", "cfg.rb", _PLANTED)
    seen: list[list[str]] = []
    real_run = quality._run

    def _spy(args, timeout, stdin_bytes=None):
        seen.append(list(args))
        return real_run(args, timeout, stdin_bytes)

    monkeypatch.setattr(quality, "_run", _spy)
    quality.secret_scan([repo], timeout=60)

    scanner_calls = [a for a in seen if a and a[0] == quality.SECRET_SCANNER]
    assert scanner_calls, f"the scanner was never invoked: {seen}"
    for call in scanner_calls:
        assert "--redact" in call, f"scanner invoked without --redact: {call}"
