#!/bin/bash
# Install daily cron job for BSV top 1000 snapshot collector.
# Run once: bash install_cron.sh

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$PROJECT_DIR/.venv/bin/python"
LOG="$PROJECT_DIR/data/collector.log"

CRON_LINE="0 6 * * * cd $PROJECT_DIR && $PYTHON collector.py >> $LOG 2>&1"

# Check if already installed
if crontab -l 2>/dev/null | grep -q "$PROJECT_DIR/collector.py"; then
    echo "Cron job already installed."
    crontab -l | grep "collector.py"
    exit 0
fi

# Append to existing crontab (or create new)
(crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
echo "Installed daily cron job (runs at 06:00 local time):"
echo "  $CRON_LINE"
echo ""
echo "Log file: $LOG"
echo "To view: crontab -l"
echo "To remove: crontab -e (and delete the line)"
