"""Optional Langfuse tracing — self-hosted (FK: https://langfuse.fourkites.com).

Open-source, runs on FK infra, so run data stays in-house — this is the FK
alternative to LangSmith/smith.langchain.com (which we deliberately do not use).

Fully no-op unless BOTH are true: the `langfuse` package is installed and the
LANGFUSE_* credentials are present. When active, LangGraph's LangChain callback
system reports every node as a span in the Langfuse UI.

Credentials follow the FK "Prompt Testing Guide" convention: put them in
`~/.fourkites-secrets.env` (LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY /
LANGFUSE_BASE_URL) — this module loads that file if present, without overriding
anything already exported.
"""
from __future__ import annotations

import os
from pathlib import Path

_FK_SECRETS = Path.home() / ".fourkites-secrets.env"
_DEFAULT_HOST = "https://langfuse.fourkites.com"


def _load_fk_secrets() -> None:
    if not _FK_SECRETS.exists():
        return
    for raw in _FK_SECRETS.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def _ensure_host() -> None:
    # FK docs use LANGFUSE_BASE_URL; the SDK reads LANGFUSE_HOST. Bridge them.
    base = os.environ.get("LANGFUSE_BASE_URL")
    if base and not os.environ.get("LANGFUSE_HOST"):
        os.environ["LANGFUSE_HOST"] = base
    os.environ.setdefault("LANGFUSE_HOST", _DEFAULT_HOST)


def callback_handler():
    """Return a Langfuse LangChain CallbackHandler, or None if unconfigured/unavailable."""
    _load_fk_secrets()
    _ensure_host()
    if not (os.environ.get("LANGFUSE_PUBLIC_KEY") and os.environ.get("LANGFUSE_SECRET_KEY")):
        return None
    try:  # langfuse v3
        from langfuse.langchain import CallbackHandler
        return CallbackHandler()
    except Exception:
        pass
    try:  # langfuse v2
        from langfuse.callback import CallbackHandler
        return CallbackHandler(
            public_key=os.environ["LANGFUSE_PUBLIC_KEY"],
            secret_key=os.environ["LANGFUSE_SECRET_KEY"],
            host=os.environ.get("LANGFUSE_HOST", _DEFAULT_HOST),
        )
    except Exception:
        return None


def host() -> str:
    _ensure_host()
    return os.environ.get("LANGFUSE_HOST", _DEFAULT_HOST)


def flush() -> None:
    """Best-effort flush so traces reach Langfuse before the process exits."""
    try:
        from langfuse import get_client
        get_client().flush()
    except Exception:
        pass
