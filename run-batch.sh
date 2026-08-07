#!/usr/bin/env bash
# run-batch.sh — run a list of Ocean/MM tickets through the Aquaman pipeline, SEQUENTIALLY.
#
# Sequential is deliberate, not lazy: Station 6 (local SIT) contends for Docker, MockServer :1080,
# service ports, and the local repo checkouts — parallel runs collide there and corrupt each other.
#
# Usage:
#   ./run-batch.sh MM-101 MM-102 MM-103 ...          # tickets as args
#   ./run-batch.sh -f tickets.txt                    # one ticket id per line (# comments / blanks ok)
#   CONTEXT="only affects SCAC=ABCD" ./run-batch.sh MM-101 MM-102   # pass --context to every run
#
# Per run: its own log under ./run-logs/<TICKET>-<ts>.log and its own Langfuse trace (named by the
# ticket key). The batch NEVER aborts on a single failure. A green/red summary prints at the end.
#
# UNATTENDED NOTE: if the human-approval / QA-review gate is enabled, a run PAUSES at an interrupt()
# and returns — the batch marks it "paused" and moves on (resume it later with --resume). For a fully
# hands-off batch, leave those gates in auto mode (e.g. unset OCEAN_PIPELINE_REQUIRE_APPROVAL).

set -uo pipefail

REPO="$HOME/Documents/projects/Aquaman"
SECRETS="$HOME/.fourkites-secrets.env"
LOGDIR="$REPO/run-logs"
TS="$(date +%Y%m%d-%H%M%S)"

# --- collect tickets (positional args, or -f <file>) ---
tickets=()
if [[ "${1:-}" == "-f" ]]; then
  [[ -f "${2:-}" ]] || { echo "file not found: ${2:-}"; exit 1; }
  while IFS= read -r line; do
    line="${line%%#*}"                 # strip trailing comment
    line="$(echo "$line" | xargs)"     # trim whitespace
    [[ -n "$line" ]] && tickets+=("$line")
  done < "$2"
else
  tickets=("$@")
fi
[[ ${#tickets[@]} -gt 0 ]] || { echo "usage: $0 MM-101 MM-102 ...   |   $0 -f tickets.txt"; exit 1; }

# --- environment: load creds (telemetry + Langfuse), activate the venv ---
if [[ -f "$SECRETS" ]]; then set -a; . "$SECRETS"; set +a; fi   # LANGFUSE_* / RCA_TOKEN
cd "$REPO" || { echo "repo not found: $REPO"; exit 1; }
# shellcheck disable=SC1091
[[ -f .venv/bin/activate ]] && . .venv/bin/activate
mkdir -p "$LOGDIR"
command -v ocean-pipeline >/dev/null 2>&1 || {
  echo "ocean-pipeline not on PATH — run: pip install -e . (inside the venv)"; exit 1; }

LF_HOST="${LANGFUSE_BASE_URL:-${LANGFUSE_HOST:-https://langfuse.fourkites.com}}"
echo "════════════════════════════════════════════════════════════"
echo "  Aquaman batch — ${#tickets[@]} ticket(s)   ·   run $TS"
echo "  logs:     $LOGDIR/<TICKET>-$TS.log"
echo "  Langfuse: $LF_HOST   (one trace per ticket, named by the key)"
[[ -n "${RCA_TOKEN:-}" ]] && echo "  telemetry: aidev_db ON" || echo "  telemetry: aidev_db OFF (RCA_TOKEN unset — no ClickHouse rows)"
echo "════════════════════════════════════════════════════════════"

results=()
i=0
for t in "${tickets[@]}"; do
  i=$((i + 1))
  log="$LOGDIR/${t}-${TS}.log"
  echo
  echo "▶ [$i/${#tickets[@]}] $t"
  echo "    live:  tail -f $log"
  start="$(date +%s)"
  if [[ -n "${CONTEXT:-}" ]]; then
    # `--context=<value>`, one argv element — the same fix monitor/app.py carries. As a separate
    # argument, a CONTEXT that argparse reads as an option string (e.g. "--strict-unmocked")
    # dies with `expected one argument` and the ticket never launches.
    ocean-pipeline "$t" --context="$CONTEXT" >"$log" 2>&1
  else
    ocean-pipeline "$t" >"$log" 2>&1
  fi
  rc=$?
  dur=$(( $(date +%s) - start ))
  exe="$(grep -oE 'EXE-[a-f0-9]+' "$log" | head -1)"
  outcome="$(grep -iE 'RESULT:|final_outcome|ready-for-review|sit_failed|could_not_verify|rca_report' "$log" | tail -1 | sed 's/^[[:space:]]*//')"

  if grep -qiE '\-\-resume .*(--approve|--qa)|paused' "$log"; then
    status="⏸ paused"          # stopped at a human/QA gate — resume to continue
  elif [[ $rc -eq 0 ]]; then
    status="✅ ok"
  else
    status="❌ fail(rc=$rc)"
  fi
  printf "    %s  ·  %dm%02ds  ·  %s\n" "$status" $((dur/60)) $((dur%60)) "${exe:-no-exe}"
  [[ -n "$outcome" ]] && echo "      $outcome"
  results+=("$(printf '%s\t%s\t%dm%02ds\t%s\t%s' "$t" "$status" $((dur/60)) $((dur%60)) "${exe:-—}" "$log")")
done

echo
echo "════════════════════════════════════════════════════════════"
echo "  SUMMARY   ($TS)"
echo "════════════════════════════════════════════════════════════"
printf '%-14s %-12s %-8s %-14s %s\n' "TICKET" "STATUS" "TIME" "EXECUTION" "LOG"
for r in "${results[@]}"; do
  IFS=$'\t' read -r a b c d e <<<"$r"
  printf '%-14s %-12s %-8s %-14s %s\n' "$a" "$b" "$c" "$d" "$e"
done
echo
echo "Track each run:"
echo "  • Langfuse (per-step node tree, per ticket): $LF_HOST  — filter/search by the ticket key"
echo "  • replay a run's log:   less $LOGDIR/<TICKET>-$TS.log"
echo "  • resume a paused/crashed run:   ocean-pipeline --resume <EXE-id>   (add --approve / --qa … for a gate)"
