#!/bin/bash
# Runs the RiskCoin batch pipeline. cron calls this every hour (see: crontab -l),
# but the batch itself runs at most once per day (step 3). Use --force to run anyway.
set -euo pipefail

# The project folder is the folder above this script, so no path needs editing
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$PROJECT_DIR/venv/bin/python"
DB_PORT=5433  # same as POSTGRESQL_PORT in .env

LOG_DIR="$PROJECT_DIR/logs"
LOCK_DIR="/tmp/riskcoin_pipeline.lock"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y-%m-%d).log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG_FILE"
}

# 1. Never let two runs overlap.
#    mkdir is atomic: it either creates the lock folder or fails because it exists.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    # The lock holds the PID of the run that took it. If that process is gone
    # (killed, crashed), the lock is stale and it is safe to take it over.
    if kill -0 "$(cat "$LOCK_DIR/pid" 2>/dev/null)" 2>/dev/null; then
        log "Previous run still in progress, skipping."
        exit 0
    fi
    log "Removing stale lock left by a run that never finished."
    rm -f "$LOCK_DIR/pid"
    rmdir "$LOCK_DIR"
    mkdir "$LOCK_DIR"
fi
echo $$ > "$LOCK_DIR/pid"
trap 'rm -f "$LOCK_DIR/pid"; rmdir "$LOCK_DIR"' EXIT

# 2. Make sure PostgreSQL (Docker) is reachable before doing any work
if ! nc -z localhost "$DB_PORT" 2>/dev/null; then
    log "PostgreSQL is not reachable on localhost:$DB_PORT. Is Docker running?"
    exit 1
fi

# 3. At most one batch run per calendar day. The batch makes one CoinGecko call
#    per coin (about 100), so running it hourly would blow through the free plan.
#    cron still calls this every hour, so if the Mac was asleep, the next hour
#    it is awake catches up.
LAST_RUN_FILE="$LOG_DIR/.last_run_date"
TODAY="$(date +%Y-%m-%d)"
if [ "${1:-}" != "--force" ] && [ "$(cat "$LAST_RUN_FILE" 2>/dev/null)" = "$TODAY" ]; then
    log "Batch already ran today, skipping."
    exit 0
fi

# 4. Run the pipeline from the project folder
cd "$PROJECT_DIR"
log "===== run started ====="

if "$PYTHON" -u main.py >> "$LOG_FILE" 2>&1; then
    echo "$TODAY" > "$LAST_RUN_FILE"
    log "===== run finished OK ====="
else
    exit_code=$?
    # Count failed runs too, so a coin that keeps failing can't trigger 100+ API
    # calls every hour. Failed coins are retried tomorrow; their watermarks make
    # sure no data is skipped.
    echo "$TODAY" > "$LAST_RUN_FILE"
    log "===== run FAILED (exit code $exit_code) ====="
    exit "$exit_code"
fi
