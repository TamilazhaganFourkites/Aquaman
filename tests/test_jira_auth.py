"""Jira auth scheme — Atlassian Cloud needs Basic, not Bearer.

`jira.py` sent `Authorization: Bearer <token>` against fourkites.atlassian.net, which returns
**403 Forbidden**: Bearer is for OAuth access tokens and Server/DC personal access tokens, while a
Cloud API token authenticates as Basic `<email>:<token>`.

This was silent, and that is the real severity. Every writer in the module is best-effort
("any error -> no-op"), so a 403 meant `transition()` and `comment()` did nothing while the pipeline
reported the ticket moved to In Review and the PR link posted. Found while running the F5 routing
eval, which surfaced it only because IT reports fetch failures instead of swallowing them.
"""
from __future__ import annotations

import base64
from pathlib import Path

from ocean_pipeline import jira


def test_basic_when_an_email_is_configured(monkeypatch):
    monkeypatch.setattr(jira, "JIRA_EMAIL", "a@b.com")
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    header = jira._auth_header()
    scheme, _, payload = header.partition(" ")
    assert scheme == "Basic"
    assert base64.b64decode(payload).decode() == "a@b.com:tok"


def test_bearer_when_no_email(monkeypatch):
    """Server/DC and OAuth deployments still work — this is a fallback, not a replacement."""
    monkeypatch.setattr(jira, "JIRA_EMAIL", "")
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    assert jira._auth_header() == "Bearer tok"


def test_the_token_is_never_logged_in_the_clear():
    """The header is built in one place so a debug print cannot accidentally carry it."""
    src = open(jira.__file__).read()
    assert src.count("_auth_header()") >= 1
    assert 'f"Bearer {JIRA_API_TOKEN}"' in src, "the fallback moved out of _auth_header"
    assert src.count('f"Bearer {JIRA_API_TOKEN}"') == 1, "the token is interpolated in >1 place"


def test_writes_still_swallow_errors_but_the_scheme_is_now_right():
    """The best-effort contract stays — a Jira outage must not fail a run. What changes is that the
    common case is no longer a guaranteed 403.

    `"except" in source` is the weakest form of this and it passed on pre-fix code too (both writers
    already had `except Exception`), so it certifies nothing about THIS change. Kept, because the
    contract it names is real, but paired below with the assertion that actually discriminates."""
    import inspect
    for fn in (jira.transition, jira.comment):
        assert "except" in inspect.getsource(fn), f"{fn.__name__} would now raise into a station"


def test_every_authenticated_request_goes_through_the_one_auth_builder():
    """THE assertion the fix needs. `_auth_header()` can be perfectly correct and change nothing if
    a caller still hand-builds its own header — which is exactly how the module got here: one
    hardcoded `Bearer` string, every writer silently 403ing, and every writer swallowing it.

    Checked structurally, not by substring: find every dict literal in jira.py that has an
    `Authorization` key, and require its value to be a call to `_auth_header`."""
    import ast

    tree = ast.parse(Path(jira.__file__).read_text())
    seen = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if not (isinstance(key, ast.Constant) and key.value == "Authorization"):
                continue
            seen += 1
            assert isinstance(value, ast.Call) and getattr(value.func, "id", "") == "_auth_header", (
                f"jira.py line {getattr(value, 'lineno', '?')} builds an Authorization header "
                f"without _auth_header() — the scheme fix does not reach this request")
    assert seen >= 1, "no Authorization header found in jira.py — did the request builder move?"


def test_a_token_without_an_email_is_NOT_enabled_on_cloud(monkeypatch, capsys):
    """The other half of the Bearer/403 fix, and without it the first half changes nothing.

    `_auth_header` sends Bearer when no email is set, and Atlassian Cloud answers Bearer with 403.
    `_enabled()` tested only for a token, so a token-only operator passed it, every writer built a
    request that could only fail, and every writer swallows its errors — which is precisely how the
    Bearer/403 defect stayed invisible. A writer that can only 403 is not enabled."""
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira, "JIRA_EMAIL", "")
    monkeypatch.setattr(jira, "JIRA_BASE_URL", "https://fourkites.atlassian.net")
    monkeypatch.setattr(jira, "_WARNED_ONCE", False)

    assert jira._enabled() is False, (
        "a token-only operator on Cloud is reported as enabled, so every write silently 403s")
    out = capsys.readouterr().out
    assert "JIRA_EMAIL" in out, "the misconfiguration is not named, so nobody can act on it"


def test_the_warning_is_said_once_not_per_write(monkeypatch, capsys):
    """These writers are best-effort and run per station; a warning on every call is noise that
    gets filtered, which is how a real one gets missed."""
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira, "JIRA_EMAIL", "")
    monkeypatch.setattr(jira, "JIRA_BASE_URL", "https://fourkites.atlassian.net")
    monkeypatch.setattr(jira, "_WARNED_ONCE", False)

    for _ in range(3):
        jira._enabled()
    # Count the MESSAGE, not a token inside it — "JIRA_EMAIL" appears twice in one emission.
    assert capsys.readouterr().out.count("CONFIGURED BUT UNUSABLE") == 1


def test_server_or_dc_deployments_stay_enabled_without_an_email(monkeypatch):
    """Bearer is CORRECT for Server/DC and OAuth, which have no email — so requiring one
    unconditionally (as the monitor's own predicate does) would disable Jira for a deployment
    where it works. The email is required only where Bearer is guaranteed to fail."""
    monkeypatch.setattr(jira, "JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira, "JIRA_EMAIL", "")
    monkeypatch.setattr(jira, "JIRA_BASE_URL", "https://jira.internal.example.com")
    assert jira._enabled() is True
