#!/bin/bash

################################################################################
# Cleanup Old Logs Script V2.0 (Phase 2)
# Purpose: Remove logs and retry state files older than 30 days
################################################################################

BASE_DIR="/opt/dw/staging/extract"
LOG_DIR="${BASE_DIR}/logs"
LOCK_DIR="${BASE_DIR}/locks"
DAYS_TO_KEEP=30

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting cleanup..."

# Cleanup old log files
find "${LOG_DIR}" -name "*.log" -type f -mtime +${DAYS_TO_KEEP} -delete
echo "✓ Cleaned up log files older than ${DAYS_TO_KEEP} days"

# Cleanup old retry state files
find "${LOCK_DIR}" -name "retry_count_*.txt" -type f -mtime +${DAYS_TO_KEEP} -delete
echo "✓ Cleaned up retry state files older than ${DAYS_TO_KEEP} days"

# Cleanup old lock files (should not happen, but just in case)
find "${LOCK_DIR}" -name "scraper_*.lock" -type f -mtime +1 -delete
echo "✓ Cleaned up stale lock files"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cleanup completed"
