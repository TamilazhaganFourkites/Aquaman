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
import subprocess

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


def find_pr_for_branch(slug: str, branch: str) -> int | None:
    """Return an existing PR number for `branch` (any state), or None. Idempotency guard so a
    rework loop or a re-run never opens a second PR for the same branch."""
    out = _gh(["pr", "list", "--repo", slug, "--head", branch, "--state", "all",
               "--json", "number,state"])
    prs = json.loads(out or "[]")
    return int(prs[0]["number"]) if prs else None


def open_draft_pr(slug: str, branch: str, title: str, body: str) -> int:
    """Idempotently open (or reuse) a DRAFT PR for `branch` on `slug`; return its number."""
    existing = find_pr_for_branch(slug, branch)
    if existing is not None:
        return existing
    _gh(["pr", "create", "--repo", slug, "--head", branch, "--draft",
         "--title", title, "--body", body])
    num = find_pr_for_branch(slug, branch)
    if num is None:
        raise GitOpError(f"opened a PR on {slug} for {branch} but could not read its number back")
    return num


def cross_link_and_ready(slug: str, pr_number: int, test_pr_url: str = "") -> None:
    """Cross-link the test-automation PR into the service PR body (idempotent), then flip the
    service PR to ready-for-review. Never merges and never deploys — that is the human boundary."""
    if test_pr_url:
        body = _gh(["pr", "view", str(pr_number), "--repo", slug, "--json", "body", "-q", ".body"])
        if test_pr_url not in body:
            link = f"\n\n---\nTest-automation PR: {test_pr_url}\n"
            _gh(["pr", "edit", str(pr_number), "--repo", slug, "--body", body + link])
    _gh(["pr", "ready", str(pr_number), "--repo", slug])  # safe no-op if already ready
