#!/bin/bash
# Run All Scrapers + Merge to 2 CSV files

CONFIG_PATH="/opt/dw/staging/config.xml"
DATE=$(date +%Y-%m-%d)
BASE_DIR="/opt/dw/staging/extract"
DB_HOST="localhost"
DB_USER="khanh"
DB_PASS="khanh123!"
DB_NAME="db_control"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2"; }

log "INFO" "=========================================="
log "INFO" "Master Scraper Runner + Merger V2.0"
log "INFO" "=========================================="

cd $BASE_DIR || exit 1
source venv/bin/activate

# Get sources grouped by website
topcv_sources=$(mysql -h $DB_HOST -u $DB_USER -p"$DB_PASS" $DB_NAME -sN -e \
    "SELECT src_id FROM extract_config WHERE enabled=TRUE AND src_id LIKE 'topcv_%' ORDER BY src_id")

jobsgo_sources=$(mysql -h $DB_HOST -u $DB_USER -p"$DB_PASS" $DB_NAME -sN -e \
    "SELECT src_id FROM extract_config WHERE enabled=TRUE AND src_id LIKE 'jobsgo_%' ORDER BY src_id")

success=0
failed=0

# === SCRAPE TOPCV SOURCES ===
log "INFO" "=== SCRAPING TOPCV SOURCES ==="
for src_id in $topcv_sources; do
    log "INFO" "Processing: $src_id"
    
    # Check if already done
    existing=$(mysql -h $DB_HOST -u $DB_USER -p"$DB_PASS" $DB_NAME -sN -e \
        "SELECT COUNT(*) FROM extract_log WHERE src_id='$src_id' AND date='$DATE' AND status='Success'")
    
    if [ "$existing" -gt 0 ]; then
        log "INFO" "✓ Already scraped - SKIPPING"
        ((success++))
        continue
    fi
    
    python3 scripts/topcv_scraper_v5.py --config $CONFIG_PATH --source_id $src_id --date $DATE
    
    if [ $? -eq 0 ]; then
        log "INFO" "✓ SUCCESS"
        ((success++))
    else
        log "ERROR" "✗ FAILED"
        ((failed++))
    fi
    
    sleep 60
done

# === SCRAPE JOBSGO SOURCES ===
log "INFO" "=== SCRAPING JOBSGO SOURCES ==="
for src_id in $jobsgo_sources; do
    log "INFO" "Processing: $src_id"
    
    existing=$(mysql -h $DB_HOST -u $DB_USER -p"$DB_PASS" $DB_NAME -sN -e \
        "SELECT COUNT(*) FROM extract_log WHERE src_id='$src_id' AND date='$DATE' AND status='Success'")
    
    if [ "$existing" -gt 0 ]; then
        log "INFO" "✓ Already scraped - SKIPPING"
        ((success++))
        continue
    fi
    
    python3 scripts/jobsgo_scraper_v1.py --config $CONFIG_PATH --source_id $src_id --date $DATE
    
    if [ $? -eq 0 ]; then
        log "INFO" "✓ SUCCESS"
        ((success++))
    else
        log "ERROR" "✗ FAILED"
        ((failed++))
    fi
    
    sleep 60
done

deactivate

# === MERGE CSV FILES ===
log "INFO" "=========================================="
log "INFO" "MERGING CSV FILES"
log "INFO" "=========================================="

TOPCV_DIR="$BASE_DIR/raw/source=topcv_jobs/date=$DATE"
JOBSGO_DIR="$BASE_DIR/raw/source=jobsgo_jobs/date=$DATE"
MERGED_DIR="$BASE_DIR/raw/merged/date=$DATE"

mkdir -p "$MERGED_DIR"

# Merge TopCV files
if [ -d "$TOPCV_DIR" ]; then
    log "INFO" "Merging TopCV CSV files..."
    
    # Get header from first file
    first_file=$(ls "$TOPCV_DIR"/*.csv 2>/dev/null | head -n 1)
    if [ -n "$first_file" ]; then
        head -n 1 "$first_file" > "$MERGED_DIR/topcv_all_jobs_$DATE.csv"
        
        # Append all data (skip headers)
        for csv in "$TOPCV_DIR"/*.csv; do
            tail -n +2 "$csv" >> "$MERGED_DIR/topcv_all_jobs_$DATE.csv"
        done
        
        topcv_count=$(tail -n +2 "$MERGED_DIR/topcv_all_jobs_$DATE.csv" | wc -l)
        log "INFO" "✓ TopCV merged: $topcv_count jobs"
    fi
fi

# Merge JobsGo files
if [ -d "$JOBSGO_DIR" ]; then
    log "INFO" "Merging JobsGo CSV files..."
    
    first_file=$(ls "$JOBSGO_DIR"/*.csv 2>/dev/null | head -n 1)
    if [ -n "$first_file" ]; then
        head -n 1 "$first_file" > "$MERGED_DIR/jobsgo_all_jobs_$DATE.csv"
        
        for csv in "$JOBSGO_DIR"/*.csv; do
            tail -n +2 "$csv" >> "$MERGED_DIR/jobsgo_all_jobs_$DATE.csv"
        done
        
        jobsgo_count=$(tail -n +2 "$MERGED_DIR/jobsgo_all_jobs_$DATE.csv" | wc -l)
        log "INFO" "✓ JobsGo merged: $jobsgo_count jobs"
    fi
fi

# Summary
log "INFO" "=========================================="
log "INFO" "EXECUTION SUMMARY"
log "INFO" "=========================================="
log "INFO" "Success:  $success"
log "INFO" "Failed:   $failed"
log "INFO" "Merged files:"
log "INFO" "  - $MERGED_DIR/topcv_all_jobs_$DATE.csv"
log "INFO" "  - $MERGED_DIR/jobsgo_all_jobs_$DATE.csv"
log "INFO" "=========================================="

exit $failed
