#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== OPTIMIZED HBASE PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

# Use environment variables with fallbacks
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Ensure results file and directory exist
mkdir -p "$(dirname "$RESULTS_FILE")"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

# Function to execute HBase commands efficiently
execute_hbase_single() {
    local temp_file=$(mktemp)
    echo "$1" > "$temp_file"
    echo "exit" >> "$temp_file"
    hbase shell "$temp_file" 2>/dev/null
    rm -f "$temp_file"
}

# Function to execute multiple HBase commands in one session
execute_hbase_batch() {
    local temp_file=$(mktemp)
    for cmd in "$@"; do
        echo "$cmd" >> "$temp_file"
    done
    echo "exit" >> "$temp_file"
    hbase shell "$temp_file" 2>/dev/null
    rm -f "$temp_file"
}

# Load test data first (not benchmarked, but may take time)
echo "📦 Loading test data into HBase..."
echo "⏳ This may take a while for scale $SCALE..."

# Load data with timeout protection
DATA_DIR="$BASE_DIR/test-data-scaled"
HBASE_FILE="$DATA_DIR/soal_hbase_${SCALE}.hbase"
if [ -f "$HBASE_FILE" ]; then
    timeout 1200 hbase shell "$HBASE_FILE" > /dev/null 2>&1 &
else
    echo "⚠️  HBase data file not found: $HBASE_FILE"
fi
LOAD_PID=$!

# Wait for load to complete or timeout
wait $LOAD_PID 2>/dev/null
LOAD_RESULT=$?

if [ $LOAD_RESULT -ne 0 ]; then
    echo "⚠️  HBase data load timed out or failed for scale $SCALE, skipping tests"
    exit 0
fi

echo "✅ HBase data loaded successfully"

# PHASE 2: Optimized Read Performance Tests
echo "🔍 PHASE 2: OPTIMIZED Read Performance Tests"

# Test 1: COUNT all records - OPTIMIZED
echo "🔍 Test 1: COUNT all records - OPTIMIZED"
START_TIME=$(date +%s%3N)

COUNT_OUTPUT=$(execute_hbase_single "count 'soalUjian'")

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
HBASE_COUNT=$(echo "$COUNT_OUTPUT" | grep -o '[0-9]* row(s)' | grep -o '[0-9]*' | head -1 || echo "0")

# Validasi count hasil
if [[ -z "$HBASE_COUNT" || ! "$HBASE_COUNT" =~ ^[0-9]+$ ]]; then
    HBASE_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$HBASE_COUNT" -gt 0 ]; then
    THROUGHPUT=$((HBASE_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,count_all_optimized,hbase,$DURATION,$HBASE_COUNT,$THROUGHPUT,optimized_table_count" >> "$RESULTS_FILE"
echo "   ✅ COUNT Duration: ${DURATION}ms, Records: $HBASE_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 2: SCAN all records with optimization
echo "🔍 Test 2: SCAN all records - OPTIMIZED"
START_TIME=$(date +%s%3N)

# Optimized scan dengan batch size yang lebih efisien
execute_hbase_single "scan 'soalUjian', {LIMIT => $SCALE, CACHE => 100}" > /tmp/hbase_scan_optimized.txt

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Hitung jumlah baris hasil scan
SCAN_COUNT=$(grep -E '^\s*ROW\s*' /tmp/hbase_scan_optimized.txt | wc -l | tr -d ' ')

if [[ -z "$SCAN_COUNT" || ! "$SCAN_COUNT" =~ ^[0-9]+$ ]]; then
    SCAN_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$SCAN_COUNT" -gt 0 ]; then
    THROUGHPUT=$((SCAN_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,scan_all_optimized,hbase,$DURATION,$SCAN_COUNT,$THROUGHPUT,optimized_scan_with_cache" >> "$RESULTS_FILE"
echo "   ✅ SCAN Duration: ${DURATION}ms, Scanned: $SCAN_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 3: SCAN with column family filter - OPTIMIZED
echo "🔍 Test 3: SCAN with column family filter - OPTIMIZED"
START_TIME=$(date +%s%3N)

execute_hbase_single "scan 'soalUjian', {COLUMNS => ['main'], LIMIT => 100, CACHE => 50}" > /tmp/hbase_filter_optimized.txt

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

FILTER_COUNT=$(grep -E '^\s*ROW\s*' /tmp/hbase_filter_optimized.txt | wc -l | tr -d ' ')

if [[ -z "$FILTER_COUNT" || ! "$FILTER_COUNT" =~ ^[0-9]+$ ]]; then
    FILTER_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$FILTER_COUNT" -gt 0 ]; then
    THROUGHPUT=$((FILTER_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,scan_filtered_optimized,hbase,$DURATION,$FILTER_COUNT,$THROUGHPUT,optimized_cf_filter_with_cache" >> "$RESULTS_FILE"
echo "   ✅ FILTERED SCAN Duration: ${DURATION}ms, Filtered: $FILTER_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 4: SCAN with specific columns only - NEW OPTIMIZATION
echo "🔍 Test 4: SCAN specific columns only - NEW"
START_TIME=$(date +%s%3N)

execute_hbase_single "scan 'soalUjian', {COLUMNS => ['main:pertanyaan', 'main:bobot'], LIMIT => 50, CACHE => 25}" > /tmp/hbase_selective_scan.txt

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

SELECTIVE_COUNT=$(grep -E '^\s*ROW\s*' /tmp/hbase_selective_scan.txt | wc -l | tr -d ' ')

if [[ -z "$SELECTIVE_COUNT" || ! "$SELECTIVE_COUNT" =~ ^[0-9]+$ ]]; then
    SELECTIVE_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$SELECTIVE_COUNT" -gt 0 ]; then
    THROUGHPUT=$((SELECTIVE_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,scan_selective_columns,hbase,$DURATION,$SELECTIVE_COUNT,$THROUGHPUT,selective_columns_scan" >> "$RESULTS_FILE"
echo "   ✅ SELECTIVE SCAN Duration: ${DURATION}ms, Records: $SELECTIVE_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 5: GET specific record - OPTIMIZED
echo "🔍 Test 5: GET specific record by ID - OPTIMIZED"
SAMPLE_ID=$(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_scan_optimized.txt | head -1)

if [ -n "$SAMPLE_ID" ]; then
    START_TIME=$(date +%s%3N)
    
    execute_hbase_single "get 'soalUjian', '$SAMPLE_ID'" > /tmp/hbase_get_optimized.txt
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    
    if [ "$DURATION" -gt 0 ]; then
        THROUGHPUT=$((1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,get_by_id_optimized,hbase,$DURATION,1,$THROUGHPUT,optimized_single_record_lookup" >> "$RESULTS_FILE"
    echo "   ✅ GET Duration: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
else
    echo "   ⚠️  No sample ID found for GET test"
fi

# Test 6: GET specific columns only - NEW OPTIMIZATION
echo "🔍 Test 6: GET specific columns only - NEW"
if [ -n "$SAMPLE_ID" ]; then
    START_TIME=$(date +%s%3N)
    
    execute_hbase_single "get 'soalUjian', '$SAMPLE_ID', 'main:pertanyaan', 'main:bobot'" > /tmp/hbase_get_selective.txt
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    
    if [ "$DURATION" -gt 0 ]; then
        THROUGHPUT=$((1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,get_selective_columns,hbase,$DURATION,1,$THROUGHPUT,selective_columns_get" >> "$RESULTS_FILE"
    echo "   ✅ GET SELECTIVE Duration: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
fi

# Test 7: Batch GET operations - OPTIMIZED
echo "🔍 Test 7: Batch GET operations (5 records) - OPTIMIZED"
SAMPLE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_scan_optimized.txt | head -5))

if [ ${#SAMPLE_IDS[@]} -gt 0 ]; then
    START_TIME=$(date +%s%3N)
    
    # Batch GET dalam satu session
    BATCH_GET_COMMANDS=()
    for id in "${SAMPLE_IDS[@]}"; do
        BATCH_GET_COMMANDS+=("get 'soalUjian', '$id'")
    done
    
    execute_hbase_batch "${BATCH_GET_COMMANDS[@]}" > /dev/null
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    RECORD_COUNT=${#SAMPLE_IDS[@]}
    
    if [ "$DURATION" -gt 0 ] && [ "$RECORD_COUNT" -gt 0 ]; then
        THROUGHPUT=$((RECORD_COUNT * 1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,batch_gets_optimized,hbase,$DURATION,$RECORD_COUNT,$THROUGHPUT,optimized_batch_gets" >> "$RESULTS_FILE"
    echo "   ✅ BATCH GET Duration: ${DURATION}ms for $RECORD_COUNT records, Throughput: ${THROUGHPUT} rps"
fi

# Test 8: Range SCAN - NEW OPTIMIZATION
echo "🔍 Test 8: Range SCAN with start/stop keys - NEW"
if [ ${#SAMPLE_IDS[@]} -ge 2 ]; then
    START_KEY=${SAMPLE_IDS[0]}
    STOP_KEY=${SAMPLE_IDS[1]}
    
    START_TIME=$(date +%s%3N)
    
    execute_hbase_single "scan 'soalUjian', {STARTROW => '$START_KEY', STOPROW => '$STOP_KEY', CACHE => 10}" > /tmp/hbase_range_scan.txt
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    
    RANGE_COUNT=$(grep -E '^\s*ROW\s*' /tmp/hbase_range_scan.txt | wc -l | tr -d ' ')
    
    if [[ -z "$RANGE_COUNT" || ! "$RANGE_COUNT" =~ ^[0-9]+$ ]]; then
        RANGE_COUNT=0
    fi
    
    if [ "$DURATION" -gt 0 ] && [ "$RANGE_COUNT" -gt 0 ]; then
        THROUGHPUT=$((RANGE_COUNT * 1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,range_scan,hbase,$DURATION,$RANGE_COUNT,$THROUGHPUT,range_scan_with_keys" >> "$RESULTS_FILE"
    echo "   ✅ RANGE SCAN Duration: ${DURATION}ms, Records: $RANGE_COUNT, Throughput: ${THROUGHPUT} rps"
fi

# Test 9: COUNT with filter - NEW OPTIMIZATION
echo "🔍 Test 9: COUNT with column family filter - NEW"
START_TIME=$(date +%s%3N)

COUNT_FILTER_OUTPUT=$(execute_hbase_single "count 'soalUjian', {COLUMNS => ['main']}")

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

FILTERED_COUNT=$(echo "$COUNT_FILTER_OUTPUT" | grep -o '[0-9]* row(s)' | grep -o '[0-9]*' | head -1 || echo "0")

if [[ -z "$FILTERED_COUNT" || ! "$FILTERED_COUNT" =~ ^[0-9]+$ ]]; then
    FILTERED_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$FILTERED_COUNT" -gt 0 ]; then
    THROUGHPUT=$((FILTERED_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,count_filtered,hbase,$DURATION,$FILTERED_COUNT,$THROUGHPUT,count_with_cf_filter" >> "$RESULTS_FILE"
echo "   ✅ COUNT FILTERED Duration: ${DURATION}ms, Records: $FILTERED_COUNT, Throughput: ${THROUGHPUT} rps"

# Cleanup temporary files
rm -f /tmp/hbase_*.txt

echo "✅ OPTIMIZED HBase benchmarks completed for scale $SCALE"

# Performance summary
echo ""
echo "📝 OPTIMIZED HBASE PERFORMANCE SUMMARY (Scale: $SCALE)"
echo "======================================================"
echo "📊 Test Results Summary:"
echo "  - 🔢 COUNT: Full table count (optimized)"
echo "  - 🔍 SCAN ALL: Complete table scan with cache"
echo "  - 🔍 SCAN FILTERED: Column family filter with cache"
echo "  - 🔍 SCAN SELECTIVE: Specific columns only"
echo "  - 📄 GET: Single record by ID (optimized)"
echo "  - 📄 GET SELECTIVE: Specific columns only"
echo "  - 📄 BATCH GET: Multiple records in one session"
echo "  - 📊 RANGE SCAN: Scan with start/stop keys"
echo "  - 🔢 COUNT FILTERED: Count with column family filter"
echo ""