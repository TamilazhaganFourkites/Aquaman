# Aquaman Batch Monitor

A small local web app that runs a **sequential** batch of `ocean-pipeline` tickets and
shows real, live station-by-station progress in a browser.

This is not a second control plane. It does exactly two things:

1. Spawns `ocean-pipeline <ticket>` as a subprocess, **one ticket at a time** — never
   concurrently. Station 6 (local SIT) needs exclusive Docker/port/repo-checkout access,
   the same reason Aquaman's own `run-batch.sh` is sequential. This app never introduces
   a `max_concurrent`-style setting.
2. Parses that subprocess's own stdout, using the exact contract already printed by
   `ocean_pipeline/ui.py` (`banner` / `station_start` / `step` / `summary`) — no separate
   source of truth, no re-implementation of any graph/node logic.

## Setup

```bash
cd Aquaman/monitor
# fastapi + uvicorn were installed into Aquaman/.venv directly — reuse it:
source ../.venv/bin/activate
```

## Run

```bash
export AQUAMAN_BIN=$(pwd)/../.venv/bin/ocean-pipeline   # absolute path to the console script
export AQUAMAN_DIR=$(pwd)/..                             # Aquaman repo root (subprocess cwd)
uvicorn app:app --port 8799
```

Open `http://localhost:8799/`, paste in 3+ ticket IDs (one per line or comma-separated),
and click **Start batch**.

## What you'll see

- Each ticket gets its own card. Stations light up live as `ocean-pipeline`'s own console
  output streams in — `▶` when a station starts, `✓`/`✗` with elapsed time when it finishes.
- **The full raw output is also visible in two other places, not just the UI**: it's echoed
  live to the terminal you ran `uvicorn` from (prefixed `[TICKET] ...`), and appended to
  `monitor/logs/<TICKET>-<timestamp>.log` as it streams — so you can `tail -f` a ticket's
  log the same way you would a plain `ocean-pipeline` run. The UI only shows the parsed,
  summarized station view; the terminal/log file has everything, unfiltered.
- **QA review / RCA review / human-approval gates pause the batch for real** — the
  underlying `ocean-pipeline` process exits at a pause (there's no long-lived process to
  keep waiting), so this app surfaces a decision panel and, on your click, spawns a fresh
  `ocean-pipeline --resume <EXE-id> --qa ...` (or `--approve`/`--reject`) — exactly the
  commands you'd type by hand, just triggered from the UI.
- A ticket only starts once the previous one has fully finished or is sitting at a gate
  waiting on you — the batch will not silently skip ahead.

## Known limits (read before demoing)

- State is in-memory only — restarting `uvicorn` loses all batch history (Aquaman's own
  LangGraph checkpoint is unaffected; only this app's view of "what happened" resets).
- No auth — this binds to localhost for local/demo use, not for deploying anywhere shared.
- `--context` per ticket isn't wired into the UI yet (the API always calls plain
  `ocean-pipeline <ticket>`); add a per-ticket context field to `CreateBatchBody` if needed.
- `LABEL_TO_NODE` in `app.py` is a hand-copied mirror of `ocean_pipeline/ui.py`'s own
  `_LABELS` dict. If that dict changes, update this one too — see the comment in `app.py`.
