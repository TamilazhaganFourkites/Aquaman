"""Deterministic git/GitHub operations, run as plain code by LangGraph nodes.

Opening the draft PR, cross-linking the test-automation PR, and flipping to
ready-for-review are exact, repeatable commands — not judgement calls. Running them
here (rather than prompting the coder agent to do it) is a core part of making the
pipeline *controlled by LangGraph*: the process is deterministic, unit-testable, and
produces hard evidence (a real PR number) instead of an agent's claim that it opened one.

Branches are always pushed to the upstream repo (never a fork — see the guardrails), so
these functions address a repo by its `<org>/<name>` slug and never need the local clone
path. They shell out to `gh`; the caller is responsible for `gh auth` (preflight checks it).
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile

from . import config


class GitOpError(RuntimeError):
    """A git/gh command failed. Carries the command + stderr so the run log is actionable."""


def repo_slug(repo: str) -> str:
    """Normalize a repo reference to `<org>/<name>`. A bare name gets the default org."""
    repo = (repo or "").strip()
    if not repo:
        return ""
    return repo if "/" in repo else f"{config.DEFAULT_REPO_ORG}/{repo}"


def _gh(args: list[str]) -> str:
    try:
        proc = subprocess.run(["gh", *args], capture_output=True, text=True,
                              timeout=config.GH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        raise GitOpError(f"`gh {' '.join(args)}` timed out after "
                         f"{config.GH_TIMEOUT_SECONDS}s (hung process or network stall)")
    if proc.returncode != 0:
        raise GitOpError(f"`gh {' '.join(args)}` failed (exit {proc.returncode}): "
                         f"{proc.stderr.strip() or proc.stdout.strip()}")
    return proc.stdout.strip()


def sync_local_checkout(slug: str) -> str:
    """G2: fast-forward the sibling local checkout at `<PROJECTS_ROOT>/<name>` to origin's
    default-branch tip, ONCE, before the analysis stations read it — so researcher/SME/dep-resolver/
    reachability all analyze current code instead of a stale base. (The coder never depends on this;
    it clones fresh into its own workspace.)

    Best-effort and deterministic: returns a short status string, never raises. Skips cleanly if the
    checkout is absent (coder clones fresh), is not a git repo, has a dirty tree, or is on a feature
    branch — we only fast-forward the default branch, never clobber local work.
    """
    name = slug.split("/")[-1]
    repo_dir = config.PROJECTS_ROOT / name
    if not (repo_dir / ".git").exists():
        return f"skip: no local checkout at {repo_dir}"

    def _git(args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(repo_dir), *args],
                              capture_output=True, text=True, timeout=config.GH_TIMEOUT_SECONDS)
    try:
        # Only TRACKED modifications are "local work" a fast-forward could clobber. `git merge
        # --ff-only` never touches UNtracked files, so untracked tooling artifacts (.vscode/,
        # .claude/worktree/, .fourkites_docker_build_hash) must NOT block the sync — blocking on them
        # left the checkout on a stale base, so prep_image keyed the pre-warm image to an OLD
        # Gemfile.lock and the coder (cloning the current ref fresh) ALWAYS cache-missed and rebuilt
        # mid-station (EXE-928fd700). Check tracked changes only.
        if _git(["status", "--porcelain", "--untracked-files=no"]).stdout.strip():
            return f"skip: {name} checkout has tracked local changes (left as-is)"
        # Resolve origin's default branch (e.g. develop/main) FIRST, from the local origin/HEAD; if
        # unset, ask the remote once (`set-head --auto` is a light ls-remote, not a full fetch).
        head = _git(["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"]).stdout.strip()
        default = head.rsplit("/", 1)[-1] if head else ""
        if not default:
            _git(["remote", "set-head", "origin", "--auto"])
            default = (_git(["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"])
                       .stdout.strip().rsplit("/", 1)[-1])
        if not default:
            return f"skip: {name} default branch unresolved"
        # Fetch ONLY the default branch — NOT `git fetch origin` (all refs), which aborts on a
        # pre-existing origin D/F ref conflict (e.g. refs/heads/test vs refs/heads/test/unit-2), skips
        # the whole sync, and leaves prep_image to build a STALE Gemfile.lock (MM-14622 / EXE-2fe18c28:
        # a plain `git pull`/targeted fetch works by hand; the all-refs fetch here did not).
        if _git(["fetch", "--quiet", "origin", default]).returncode != 0:
            return f"skip: {name} fetch of origin/{default} failed"
        current = _git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip()
        if current != default:
            return f"skip: {name} on '{current}', not default '{default}' (left as-is)"
        ff = _git(["merge", "--ff-only", f"origin/{default}"])
        if ff.returncode != 0:
            return f"skip: {name} not fast-forwardable to origin/{default}"
        return f"synced {name} → origin/{default} tip"
    except subprocess.TimeoutExpired:
        return f"skip: {name} git sync timed out"
    except OSError as e:
        return f"skip: {name} git sync error ({e})"


# A PR in one of these states is finished with — reusing it as this run's deliverable means
# reporting work against something a human already declined or already landed.
_DEAD_PR_STATES = frozenset({"CLOSED", "MERGED"})


def find_pr_for_branch(slug: str, branch: str) -> int | None:
    """An OPEN PR number for `branch`, or None. The idempotency guard for the rework loop.

    It asked for `--state all` and `--json number,state` and then THREW THE STATE AWAY, so a
    CLOSED PR was reused as the deliverable. That is not hypothetical: PR #3088 was declined
    (MM-14060-20260801-233046.log:110) and three separate later runs reported it as their result
    (0801-104256:474, 0802-071612:580, 0803-153623:416) — the pipeline told a human it had
    delivered a PR that same human had already rejected.

    Still queries `--state all` rather than `--state open`: the caller needs to distinguish "no PR
    exists" from "a dead one does", because those want different actions and only one of them is
    worth logging. See `open_draft_pr`."""
    return _find_pr(slug, branch)[0]


def _find_pr(slug: str, branch: str) -> tuple[int | None, list[dict]]:
    """(reusable open PR number or None, every PR found for the branch)."""
    out = _gh(["pr", "list", "--repo", slug, "--head", branch, "--state", "all",
               "--json", "number,state"])
    prs = [p for p in json.loads(out or "[]") if isinstance(p, dict)]
    live = [p for p in prs if str(p.get("state", "")).upper() not in _DEAD_PR_STATES]
    return (int(live[0]["number"]) if live else None), prs


def open_draft_pr(slug: str, branch: str, title: str, body: str) -> int:
    """Idempotently open (or reuse) a DRAFT PR for `branch` on `slug`; return its number.

    Reuse means an OPEN PR. If every PR for this branch is closed or merged, a NEW one is opened —
    dropping the state filter would otherwise silently hand back a declined PR as the deliverable
    (see `find_pr_for_branch`). This is the "closed -> open a new one" branch that filtering
    requires: without it the state filter would just turn a wrong answer into no answer."""
    existing, all_prs = _find_pr(slug, branch)
    if existing is not None:
        return existing
    if all_prs:
        # Say so. A silent re-open looks identical to a first-ever open in the log, and the fact
        # that a human already closed a PR for this exact branch is the most interesting thing
        # about the run at that moment.
        dead = ", ".join(f"#{p.get('number')} {p.get('state')}" for p in all_prs)
        print(f"[gitops] {slug} {branch}: existing PR(s) are finished with ({dead}) — "
              f"opening a NEW draft PR rather than reporting a closed one as the deliverable")
    _gh(["pr", "create", "--repo", slug, "--head", branch, "--draft",
         "--title", title, "--body", body])
    num = find_pr_for_branch(slug, branch)
    if num is None:
        raise GitOpError(f"opened a PR on {slug} for {branch} but could not read its number back")
    return num


def cross_link_and_ready(slug: str, pr_number: int, test_pr_url: str = "") -> None:
    """Cross-link the test-automation PR into the service PR body (idempotent), then flip the
    service PR to ready-for-review. Never merges and never deploys — that is the human boundary.

    Reads/writes the body via `gh api` (plain REST) rather than `gh pr view`/`gh pr edit`: those
    subcommands request the deprecated `projectCards` field as part of their default GraphQL
    query, which GitHub now rejects outright on repos where "Projects (classic)" has been sunset
    (`GraphQL: Projects (classic) is being deprecated ... (repository.pullRequest.projectCards)`).
    The REST endpoint has no such field and isn't affected.
    """
    if test_pr_url:
        body = _gh(["api", f"repos/{slug}/pulls/{pr_number}", "--jq", ".body"])
        if test_pr_url not in body:
            new_body = body + f"\n\n---\nTest-automation PR: {test_pr_url}\n"
            # Written via a temp file + `@path`, not inline `body=<text>`: passing arbitrary PR-body
            # text inline would misfire if the body ever happened to start with `@`, and a file also
            # sidesteps any command-line length limit for an unusually long body.
            #
            # `-F`, NOT `-f`. The `@file` form is documented ONLY under `-F/--field`; `-f/--raw-field`
            # sends the value LITERALLY. With `-f` this PATCH replaced the entire PR body with the
            # ~50-char string "@/var/folders/…/tmpXXXX.md" — and did it SILENTLY, because the API
            # returns 200 OK for a body that is merely wrong. It never converged either: each re-run
            # PATCHed a NEW tempfile path over the last one. Verified against gh 2.68.1.
            #
            # The three lines above were already the correct rationale for a file + `@path`; the flag
            # underneath them just didn't implement it. Fired zero times so far — gated by
            # `if test_pr_url` and `if test_pr_url not in body`, and the one run that has ever reached
            # flip_ready (EXE-0087f45b/MM-14312) had no test PR. Armed, not triggered.
            with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as f:
                f.write(new_body)
                body_path = f.name
            try:
                _gh(["api", f"repos/{slug}/pulls/{pr_number}", "-X", "PATCH",
                     "-F", f"body=@{body_path}"])
            finally:
                os.unlink(body_path)
    _gh(["pr", "ready", str(pr_number), "--repo", slug])  # safe no-op if already ready
