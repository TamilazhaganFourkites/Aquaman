# Aquaman Batch Monitor

A small local web app that runs a batch of `ocean-pipeline` tickets **concurrently** (up
to `AQUAMAN_MAX_CONCURRENT` at once, default 3) and shows real, live station-by-station
progress for each in a browser.

This is not a second control plane. It does exactly two things:

1. Spawns `ocean-pipeline <ticket>` as a subprocess — up to `AQUAMAN_MAX_CONCURRENT` at
   once, shared across manual batches and the auto-queue. This used to be strictly one
   ticket at a time: Station 6 (local SIT) binds fixed ports and a shared local repo
   checkout, a genuine collision risk with no protection outside this app. It no longer
   needs to be — `ocean_pipeline` itself now carries machine-wide protection for exactly
   that (a flock-based SIT-stage slot, `MAX_CONCURRENT_SIT`, default 1, plus a build-slot
   for the Docker-heavy stages, `MAX_CONCURRENT_BUILDS`, default 2), so multiple tickets'
   earlier stages run genuinely concurrently while their Docker-heavy moments queue
   safely behind each other instead of colliding. Aquaman's own `run-batch.sh` CLI script
   is still deliberately sequential — a design choice there, not a limitation here.
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
export AQUAMAN_MAX_CONCURRENT=3                          # optional, defaults to 3
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
- Up to `AQUAMAN_MAX_CONCURRENT` tickets run at once (across manual batches and the
  auto-queue combined) — the rest queue and start the moment a slot frees, not once
  every earlier ticket has fully finished.

## Known limits (read before demoing)

- State is in-memory only — restarting `uvicorn` loses all batch history (Aquaman's own
  LangGraph checkpoint is unaffected; only this app's view of "what happened" resets).
- No auth — this binds to localhost for local/demo use, not for deploying anywhere shared.
- `--context` per ticket isn't wired into the UI yet (the API always calls plain
  `ocean-pipeline <ticket>`); add a per-ticket context field to `CreateBatchBody` if needed.
- `LABEL_TO_NODE` in `app.py` is a hand-copied mirror of `ocean_pipeline/ui.py`'s own
  `_LABELS` dict. If that dict changes, update this one too — see the comment in `app.py`.
