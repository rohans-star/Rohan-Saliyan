#!/bin/bash
# regen.sh — Daily wrapper for NL Bucketing Dashboard regenerator
# Scheduled to run at 00:05 Asia/Dubai via cron.
#
# Setup:
#   1. Place this file and regenerate_dashboard.py in /opt/nl_dashboard/
#   2. chmod +x /opt/nl_dashboard/regen.sh
#   3. Create service account JSON: /opt/nl_dashboard/sa.json
#      with BigQuery Data Viewer + BigQuery Job User on the relevant datasets
#   4. Install crontab entry (see end of this file)
#
# Logs:
#   /var/log/nl_dashboard_regen.log
#
# Exit codes:
#   0  success
#   1  BQ query failure
#   2  file/template error
#   3  setup error (paths missing, etc.)

set -euo pipefail

# ── Configuration ────────────────────────────────────────────
SCRIPT_DIR="${SCRIPT_DIR:-/opt/nl_dashboard}"
TEMPLATE="${TEMPLATE:-$SCRIPT_DIR/NL_Bucketing_Enhanced_11.template.html}"
OUTPUT="${OUTPUT:-/var/www/html/NL_Bucketing_Enhanced_11.html}"
SA_JSON="${SA_JSON:-$SCRIPT_DIR/sa.json}"
PYTHON="${PYTHON:-/usr/bin/python3}"
LOG_FILE="${LOG_FILE:-/var/log/nl_dashboard_regen.log}"
LOCK_FILE="${LOCK_FILE:-/var/run/nl_dashboard_regen.lock}"

# Force Dubai timezone for all timestamps in logs
export TZ="Asia/Dubai"
export REGEN_LOG="$LOG_FILE"

# ── Pre-flight checks ────────────────────────────────────────
{
  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo "  Regen kickoff: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "═══════════════════════════════════════════════════════════"
} >> "$LOG_FILE"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "[FATAL] Template missing: $TEMPLATE" | tee -a "$LOG_FILE"
  exit 3
fi
if [[ ! -f "$SCRIPT_DIR/regenerate_dashboard.py" ]]; then
  echo "[FATAL] regenerate_dashboard.py missing in $SCRIPT_DIR" | tee -a "$LOG_FILE"
  exit 3
fi
if [[ ! -f "$SA_JSON" ]]; then
  echo "[WARN] Service account JSON missing — will try ADC" | tee -a "$LOG_FILE"
  SA_ARG=""
else
  SA_ARG="--service-account $SA_JSON"
fi

# ── Lock to prevent overlapping runs ─────────────────────────
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
  echo "[FATAL] Another regen run is already in progress — aborting" | tee -a "$LOG_FILE"
  exit 3
fi

# ── Run the regenerator ──────────────────────────────────────
START_TS=$(date +%s)
if "$PYTHON" "$SCRIPT_DIR/regenerate_dashboard.py" \
     --template "$TEMPLATE" \
     --output "$OUTPUT" \
     $SA_ARG >> "$LOG_FILE" 2>&1; then
  END_TS=$(date +%s)
  ELAPSED=$((END_TS - START_TS))
  echo "[OK] Regen completed in ${ELAPSED}s" | tee -a "$LOG_FILE"

  # Optional: notify monitoring (uncomment + configure if you have a healthcheck endpoint)
  # curl -fsS -m 10 --retry 3 "https://hc-ping.com/<your-uuid>" > /dev/null 2>&1 || true

  exit 0
else
  EXIT=$?
  echo "[FAIL] Regen failed (exit=$EXIT)" | tee -a "$LOG_FILE"

  # Optional: alert on failure
  # curl -fsS -m 10 --retry 3 "https://hc-ping.com/<your-uuid>/fail" > /dev/null 2>&1 || true

  exit "$EXIT"
fi

# ── Crontab installation ─────────────────────────────────────
# Run as root or the user owning $OUTPUT:
#
#   crontab -e
#
# Add this line (00:05 Asia/Dubai = 20:05 UTC, since Dubai is UTC+4 year-round):
#
#   CRON_TZ=Asia/Dubai
#   5 0 * * * /opt/nl_dashboard/regen.sh
#
# Note: CRON_TZ requires cron version that supports it (Vixie cron 4.1+, all modern Linux).
# If your cron doesn't support CRON_TZ, use UTC equivalent instead:
#
#   5 20 * * * /opt/nl_dashboard/regen.sh
