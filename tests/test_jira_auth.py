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
    common case is no longer a guaranteed 403."""
    import inspect
    for fn in (jira.transition, jira.comment):
        assert "except" in inspect.getsource(fn), f"{fn.__name__} would now raise into a station"
