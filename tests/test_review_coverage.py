"""B1 — the multi-repo review coverage gate.

`harsh_reviewer` reviews ONE tree (`cwd=Path(state["worktree_dir"])`) while `open_pr` and
`flip_ready` act on EVERY slug in `_service_slugs(state)`. Nothing in between noticed a repo that
receives a pull request without anything adversarial ever reading it.

Coverage is derived FROM DISK, not from the coder's self-report, and that is the load-bearing design
decision: `CoderVerdict.repo` is comma-joined 1..N while every instruction the coder gets is
singular, so the dominant failure shape is "coder touched 2, reported 1" — which a gate keyed on
`len(_service_slugs) >= 2` would be silent about.

Design plan, including the six adversarial review rounds behind it and the costs accepted:
important-notes/MM-14816-multi-repo-review-gate-plan.md
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from ocean_pipeline import config, graph, nodes


def _sh(*args, cwd):
    subprocess.run(args, cwd=str(cwd), capture_output=True, check=False)


def _repo(root: Path, name: str, origin_slug: str, branch: str = "") -> Path:
    """A real git repo with a real `origin` URL and, optionally, a local branch."""
    d = root / name
    d.mkdir(parents=True)
    _sh("git", "init", "-q", "-b", "main", cwd=d)
    _sh("git", "config", "user.email", "t@x.com", cwd=d)
    _sh("git", "config", "user.name", "t", cwd=d)
    _sh("git", "remote", "add", "origin", f"git@github.com:{origin_slug}.git", cwd=d)
    (d / "f.txt").write_text("x\n")
    _sh("git", "add", "-A", cwd=d)
    _sh("git", "commit", "-qm", "base", cwd=d)
    if branch:
        _sh("git", "branch", branch, cwd=d)
    return d


@pytest.fixture
def run_dir(tmp_path, monkeypatch):
    """`ARTIFACTS_ROOT/<exec>/workspace`, so `workspace.parent` is THIS run's dir — the literal
    pinning that keeps the sibling scan out of other runs' clones."""
    monkeypatch.setattr(config, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    d = tmp_path / "artifacts" / "EXE-cov" / "workspace"
    d.mkdir(parents=True)
    return d


def _state(**kw):
    base = {"ticket_id": "MM-1", "execution_id": "EXE-cov", "branch": "MM-1/fix"}
    base.update(kw)
    return base


# --------------------------------------------------------------- _origin_slug
def test_origin_slug_never_leaks_the_remote_url(tmp_path):
    """A real origin can carry credentials. This must return a SLUG or "" — never the URL — and the
    parse-failure branch matters most: `gitops.repo_slug` returns any `/`-bearing string unchanged,
    so a failed parse would turn a token-bearing URL into a "slug" flowing to every sink."""
    d = _repo(tmp_path, "a", "cloudqwest/ocean-service")
    assert nodes._origin_slug(d) == "cloudqwest/ocean-service"

    _sh("git", "remote", "set-url", "origin",
        "https://user:s3cr3t-token@github.example.com/cloudqwest/ocean-service.git", cwd=d)
    got = nodes._origin_slug(d)
    assert got == "cloudqwest/ocean-service"
    assert "s3cr3t-token" not in got and "@" not in got and "://" not in got

    # Unparseable -> "", never a half-parsed string.
    _sh("git", "remote", "set-url", "origin", "weird-single-token", cwd=d)
    assert nodes._origin_slug(d) == ""

    # No origin at all, and a non-repo directory.
    plain = tmp_path / "plain"
    plain.mkdir()
    assert nodes._origin_slug(plain) == ""


def test_origin_slug_is_case_normalised_against_the_report(tmp_path):
    """`gitops.repo_slug` does not lowercase, so an origin/report case mismatch would otherwise be a
    permanent gap no bounce could ever clear."""
    d = _repo(tmp_path, "a", "CloudQwest/Ocean-Service", branch="MM-1/fix")
    _, covered, gap, unverified = nodes._review_coverage(
        _state(service_repo="cloudqwest/ocean-service", worktree_dir=str(d)), [d])
    assert unverified == ""
    assert gap == [], f"case mismatch produced a permanent gap: {gap}"
    assert covered == ["CloudQwest/Ocean-Service"]


# --------------------------------------------------------------- _branch_repos
def test_a_tag_does_not_manufacture_a_repo(tmp_path, run_dir):
    """`show-ref --verify refs/heads/<b>`, not `rev-parse --verify <b>`: executed, `rev-parse` also
    matches a TAG of the same name."""
    d = _repo(run_dir.parent, "tagged", "cloudqwest/x")
    _sh("git", "tag", "MM-1/fix", cwd=d)
    assert nodes._branch_repos(_state()) == []
    # And with a real branch it IS found.
    _sh("git", "branch", "MM-1/fix", cwd=d)
    assert [p.name for p in nodes._branch_repos(_state())] == ["tagged"]


def test_a_stale_local_branch_does_enter_required(tmp_path, run_dir):
    """Pinned so nobody "fixes" it later. Over-inclusion produces a loud stop; the alternative
    (does HEAD carry the branch) would MISS a repo the coder pushed then checked out elsewhere."""
    _repo(run_dir.parent, "stale", "cloudqwest/stale", branch="MM-1/fix")
    _sh("git", "checkout", "-q", "main", cwd=run_dir.parent / "stale")
    assert [p.name for p in nodes._branch_repos(_state())] == ["stale"]


def test_no_branch_scans_nothing(run_dir):
    _repo(run_dir.parent, "a", "cloudqwest/a", branch="MM-1/fix")
    assert nodes._branch_repos(_state(branch="")) == []


# --------------------------------------------------------------- coverage
def test_coverage_is_read_from_git_origin_not_from_state(tmp_path, run_dir):
    """Swapping which tree `worktree_dir` points at flips (covered, gap) with `service_repo`
    UNCHANGED. A state-derived implementation cannot pass this."""
    a = _repo(run_dir.parent, "a", "cloudqwest/repo-a", branch="MM-1/fix")
    b = _repo(run_dir.parent, "b", "cloudqwest/repo-b", branch="MM-1/fix")
    st = _state(service_repo="cloudqwest/repo-a")

    _, cov_a, gap_a, _ = nodes._review_coverage(st, [a])
    _, cov_b, gap_b, _ = nodes._review_coverage(st, [b])
    assert cov_a == ["cloudqwest/repo-a"] and gap_a == ["cloudqwest/repo-b"]
    assert cov_b == ["cloudqwest/repo-b"] and gap_b == ["cloudqwest/repo-a"]


def test_under_reported_repo_is_seen_from_disk(tmp_path, run_dir):
    """THE dominant shape: coder touched 2, reported 1. A `service_repo`-derived implementation
    cannot pass — that is the whole reason coverage comes from disk."""
    a = _repo(run_dir.parent, "a", "cloudqwest/repo-a", branch="MM-1/fix")
    _repo(run_dir.parent, "b", "cloudqwest/repo-b", branch="MM-1/fix")
    branch_repos, covered, gap, _ = nodes._review_coverage(
        _state(service_repo="cloudqwest/repo-a", worktree_dir=str(a)), [a])
    assert sorted(branch_repos) == ["cloudqwest/repo-a", "cloudqwest/repo-b"]
    assert gap == ["cloudqwest/repo-b"], "the unreported repo was not seen from disk"
    assert covered == ["cloudqwest/repo-a"]


def test_identity_mismatch_at_one_repo_is_a_gap(tmp_path, run_dir):
    """The case a `len(required) <= 1` short-circuit would miss: nodes.py falls back for
    `service_repo` and `worktree_dir` INDEPENDENTLY, so a rework can leave the reviewer in repo A
    while `service_repo` says B."""
    a = _repo(run_dir.parent, "a", "cloudqwest/repo-a", branch="MM-1/fix")
    _, _, gap, _ = nodes._review_coverage(_state(service_repo="cloudqwest/repo-b"), [a])
    assert gap == ["cloudqwest/repo-b"]


def test_every_repo_open_pr_acts_on_is_in_required(tmp_path, run_dir):
    """The invariant, asserted against `_service_slugs` directly — including the coder-omitted-`repo`
    state that produced a silent hole in an earlier draft of the design (`open_pr` opened PRs on
    both slugs while a service_repo-only `required` was empty, so BOTH shipped unreviewed)."""
    a = _repo(run_dir.parent, "a", "cloudqwest/repo-a", branch="MM-1/fix")
    st = _state(service_repo="", worktree_dir=str(a),
                target_repos=[{"repo": "cloudqwest/repo-a"}, {"repo": "cloudqwest/repo-b"}])
    _, _, gap, _ = nodes._review_coverage(st, [a])
    for slug in nodes._service_slugs(st):
        assert slug in gap or slug in ["cloudqwest/repo-a"], (
            f"{slug} gets a PR from open_pr but is in neither covered nor gap")
    assert "cloudqwest/repo-b" in gap


def test_a_branchless_pass_produces_no_gap(tmp_path, run_dir):
    """The regression the `_service_slugs` union created. With no branch, `_service_slugs` falls
    through to the over-scoped `target_repos`, so a non-empty gap would mislabel the run (the real
    reason is review_budget_exhausted_no_diff) AND shrink the coder's retry budget from
    MAX_REVIEW_ITERATIONS to this gate's cap of 1. With no branch nothing can ship anyway."""
    a = _repo(run_dir.parent, "a", "cloudqwest/repo-a")
    st = _state(branch="", service_repo="",
                target_repos=[{"repo": "cloudqwest/x"}, {"repo": "cloudqwest/y"}])
    _, _, gap, unverified = nodes._review_coverage(st, [a])
    assert gap == [], "a branchless pass must not produce a coverage gap"
    assert unverified, "and it must say why coverage was not derived"


def test_coverage_unverified_is_loud_not_clean(tmp_path, run_dir):
    """Fails OPEN when the REVIEWED dir yields no slug — matching graph.py's stated sibling
    precedent, "clean, or could-not-run (fails OPEN, loudly)". Failing closed would turn a broken
    git into a zero-PR halt on every run."""
    _repo(run_dir.parent, "a", "cloudqwest/repo-a", branch="MM-1/fix")
    plain = run_dir.parent / "not-a-repo"
    plain.mkdir()
    _, covered, gap, unverified = nodes._review_coverage(
        _state(service_repo="cloudqwest/repo-a"), [plain])
    assert covered == [] and gap == []
    assert "no origin slug" in unverified


def test_empty_slugs_never_create_a_permanent_gap(tmp_path, run_dir):
    """`_origin_slug` returns "" on doubt; an unfiltered "" would be a gap no bounce could clear."""
    a = _repo(run_dir.parent, "a", "cloudqwest/repo-a", branch="MM-1/fix")
    _sh("git", "remote", "set-url", "origin", "nonsense", cwd=run_dir.parent / "a")
    b = _repo(run_dir.parent, "b", "cloudqwest/repo-b", branch="MM-1/fix")
    branch_repos, _, gap, _ = nodes._review_coverage(
        _state(service_repo="cloudqwest/repo-b", worktree_dir=str(b)), [b])
    assert "" not in branch_repos and "" not in gap
    assert gap == [], f"an unparseable origin leaked an empty slug into the gap: {gap}"
    del a


# --------------------------------------------------------------- routing
def test_after_review_refuses_both_approve_paths():
    """The gate sits ABOVE the plain-approve return AND dominates the budget-exhausted path."""
    st = _state(review_verdict="APPROVE", review_coverage_gap=["cloudqwest/b"])
    assert graph.after_review(st) == "rework"
    st_exhausted = _state(review_verdict="CHANGES_REQUIRED",
                          review_iteration=config.MAX_REVIEW_ITERATIONS,
                          review_coverage_stopped=True)
    assert graph.after_review(st_exhausted) == "stop"


def test_gate_off_still_records_the_gap(monkeypatch):
    """Knob OFF records the gap but neither stops nor bounces — so it can be switched off without
    losing the measurement that says whether it was worth having."""
    monkeypatch.setattr(config, "MULTI_REPO_REVIEW_GATE", False)
    st = _state(review_verdict="APPROVE", review_coverage_gap=["cloudqwest/b"])
    assert graph.after_review(st) == "approve"
    assert st["review_coverage_gap"] == ["cloudqwest/b"]


def test_one_bounce_then_stop(tmp_path, run_dir, monkeypatch):
    """It cannot loop past its own cap. This is the ONLY bound — the check sits above the
    MAX_REVIEW_ITERATIONS test and "rework" maps straight to `coder`."""
    monkeypatch.setattr(config, "MULTI_REPO_REVIEW_GATE", True)
    monkeypatch.setattr(config, "MAX_COVERAGE_ATTEMPTS", 1)
    st = _state(review_coverage_gap=["cloudqwest/b"], review_coverage_attempts=1)
    assert graph.after_review({**st, "review_coverage_stopped": False}) == "rework"
    assert graph.after_review({**st, "review_coverage_attempts": 2,
                               "review_coverage_stopped": True}) == "stop"


# --------------------------------------------------------------- terminal labelling
def test_a_coverage_stop_labels_final_outcome_not_just_reason():
    """Asserting `reason` alone passes while the human-facing string still reads
    "no diff for the reviewer to approve" — both halves false."""
    src = Path(nodes.__file__).read_text()
    assert src.count('state.get("review_coverage_stopped")') >= 3, (
        "the coverage stop is not labelled in all three stop_run cascades "
        "(reason / pr_note / outcome_prefix)")
    assert "review_coverage_gap" in src and "review_coverage_stopped" in src
    stop_run = src[src.index("async def stop_run"):]
    head = stop_run.split("return {")[0]
    assert 'state.get("review_coverage_gap")' not in head.split("pr_note = (f\"no PR opened")[0], (
        "a stop label keys on the GAP — a stale gap from a bounced pass will mislabel a later stop")


def test_prep_rework_resets_three_of_six():
    """Budget and decision reset; the OBSERVATIONS and the infra fault do not — an unverified
    coverage derivation that survives a rework should still be visible at the end of the run."""
    import asyncio
    out = asyncio.run(nodes.prep_rework(_state(
        coding_attempts=1, review_coverage_attempts=2, review_coverage_gap=["a/b"],
        review_coverage_stopped=True, review_coverage_unverified="git was broken")))
    assert out["review_coverage_attempts"] == 0
    assert out["review_coverage_gap"] == []
    assert out["review_coverage_stopped"] is False
    assert "review_coverage_unverified" not in out


def test_state_declares_every_coverage_key():
    """LangGraph silently DROPS undeclared keys — this repo has been bitten four times."""
    from ocean_pipeline.state import OceanState
    for key in ("review_branch_repos", "review_repos_covered", "review_coverage_gap",
                "review_coverage_unverified", "review_coverage_attempts",
                "review_coverage_stopped"):
        assert key in OceanState.__annotations__, f"{key} is undeclared"


def test_the_report_surfaces_the_disk_derived_instrument(tmp_path):
    """`review_branch_repos` is recorded as its OWN key, not reconstructed from covered+gap — that
    union is contaminated by `service_repo` and cannot answer "what did the disk show?"."""
    from ocean_pipeline import report
    import json
    report._meta.clear()
    report._rows.clear()
    report.start("MM-1", "EXE-cov-report")
    report.record("harsh_reviewer", 1.0, {})
    report.finish({"final_status": "failed",
                   "review_branch_repos": ["cloudqwest/a", "cloudqwest/b"],
                   "review_repos_covered": ["cloudqwest/a"],
                   "review_coverage_gap": ["cloudqwest/b"]}, tmp_path)
    doc = json.loads((tmp_path / "run-report.json").read_text())
    assert doc["review_branch_repos"] == ["cloudqwest/a", "cloudqwest/b"]
    assert doc["review_coverage_gap"] == ["cloudqwest/b"]
    assert "review_coverage_unverified" in doc


# --------------------------------------------------------------- the budget (pure)
def test_the_budget_bounces_once_then_stops(monkeypatch):
    """Drives the NODE's own computation, not the router. Every routing test above sets
    `review_coverage_stopped` by hand, so without this both the cap and the knob check were
    unreachable — a mutation sweep proved it by reverting each and staying green."""
    monkeypatch.setattr(config, "MULTI_REPO_REVIEW_GATE", True)
    monkeypatch.setattr(config, "MAX_COVERAGE_ATTEMPTS", 1)
    gap = ["cloudqwest/b"]

    attempts, stopped = nodes._coverage_budget(_state(), gap)
    assert (attempts, stopped) == (1, False), "the FIRST gap must bounce, not stop"

    attempts, stopped = nodes._coverage_budget(_state(review_coverage_attempts=1), gap)
    assert (attempts, stopped) == (2, True), "the second gap must stop, not loop"


def test_a_clean_pass_neither_counts_nor_stops():
    attempts, stopped = nodes._coverage_budget(_state(review_coverage_attempts=1), [])
    assert (attempts, stopped) == (1, False)


def test_the_knob_off_never_stops_however_many_bounces(monkeypatch):
    """Knob OFF must be incapable of terminating a run, at any attempt count."""
    monkeypatch.setattr(config, "MULTI_REPO_REVIEW_GATE", False)
    monkeypatch.setattr(config, "MAX_COVERAGE_ATTEMPTS", 1)
    for prior in (0, 1, 2, 99):
        _, stopped = nodes._coverage_budget(_state(review_coverage_attempts=prior),
                                            ["cloudqwest/b"])
        assert stopped is False, f"knob-off stopped the run at attempts={prior}"


def test_the_budget_is_actually_used_by_the_reviewer():
    """Guard against the helper being extracted and then orphaned — the inertness shape again."""
    import inspect
    assert "_coverage_budget(state, gap)" in inspect.getsource(nodes.harsh_reviewer)
