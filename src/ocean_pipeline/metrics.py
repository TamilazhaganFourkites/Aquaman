"""Per-run cost / token / tool-call accumulator, surfaced in the runner log.

Best-effort: the Claude Agent SDK reports usage on its final ResultMessage; if a
field isn't present (e.g. CLI-auth runs that don't surface cost), it's simply
omitted from the log rather than shown as zero.
"""
from __future__ import annotations

_run = {"cost": 0.0, "input": 0, "output": 0, "tools": 0, "stations": 0}


def reset() -> None:
    _run.update(cost=0.0, input=0, output=0, tools=0, stations=0)


def add(cost: float = 0.0, input_tokens: int = 0, output_tokens: int = 0, tools: int = 0) -> None:
    _run["cost"] += cost or 0.0
    _run["input"] += input_tokens or 0
    _run["output"] += output_tokens or 0
    _run["tools"] += tools or 0
    _run["stations"] += 1


def totals() -> dict:
    return dict(_run)


def _k(n: int) -> str:
    return f"{n / 1000:.0f}k" if n >= 1000 else str(n)


def fmt(cost: float, input_tokens: int, output_tokens: int, tools: int | None = None) -> str:
    """One compact usage string, e.g. '52k tokens · 12 tool calls'. No cost/$ is shown or
    recorded anywhere; the `cost` arg is accepted for call-site compatibility and ignored."""
    tok = (input_tokens or 0) + (output_tokens or 0)
    parts: list[str] = []
    if tok:
        parts.append(f"{_k(tok)} tokens")
    if tools:
        parts.append(f"{tools} tool calls")
    return " · ".join(parts)
