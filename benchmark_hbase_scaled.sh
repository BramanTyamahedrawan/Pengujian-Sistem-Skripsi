  GNU nano 7.2                                                              benchmark_hbase_scaled.sh
#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== HBASE PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

# Use environment variables with fallbacks
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Ensure results file and directory exist
mkdir -p "$(dirname "$RESULTS_FILE")"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

# Load test data first (not benchmarked, but may take time)
echo "📦 Loading test data into HBase..."
echo "⏳ This may take a while for scale $SCALE..."

# Load data with timeout protection
timeout 300 hbase shell "soal_hbase_${SCALE}.hbase" > /dev/null 2>&1 &
LOAD_PID=$!

# Wait for load to complete or timeout
wait $LOAD_PID 2>/dev/null
LOAD_RESULT=$?

if [ $LOAD_RESULT -ne 0 ]; then
    echo "⚠️  HBase data load timed out or failed for scale $SCALE, skipping tests"
    return 1
fi

echo "✅ HBase data loaded successfully"

# PHASE 2: Read Performance Tests
echo "🔍 PHASE 2: Read Performance Tests"

# Test 1: COUNT all records
echo "🔍 Test 1: COUNT all records"
START_TIME=$(date +%s%3N)
COUNT_OUTPUT=$(echo "count 'soalUjian'" | hbase shell 2>/dev/null)
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
HBASE_COUNT=$(echo "$COUNT_OUTPUT" | grep -o '[0-9]* row(s)' | grep -o '[0-9]*' | head -1 || echo "0")
THROUGHPUT=$((HBASE_COUNT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,count_all,hbase,$DURATION,$HBASE_COUNT,$THROUGHPUT,full_table_count" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Records: $HBASE_COUNT"

# Test 2: SCAN all records (limited to avoid timeout)
echo "🔍 Test 2: SCAN all records"
START_TIME=$(date +%s%3N)
echo "scan 'soalUjian', {LIMIT => $SCALE}" | hbase shell > /tmp/hbase_scan_output.txt 2>/dev/null
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
SCAN_COUNT=$(grep -c "column=" /tmp/hbase_scan_output.txt || echo "0")
THROUGHPUT=$((SCAN_COUNT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,scan_all,hbase,$DURATION,$SCAN_COUNT,$THROUGHPUT,full_table_scan" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Scanned entries: $SCAN_COUNT"

# Test 3: SCAN with column family filter
echo "🔍 Test 3: SCAN with column family filter"
START_TIME=$(date +%s%3N)
echo "scan 'soalUjian', {COLUMNS => ['main:'], LIMIT => $SCALE}" | hbase shell > /tmp/hbase_filter_output.txt 2>/dev/null
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
FILTER_COUNT=$(grep -c "column=main:" /tmp/hbase_filter_output.txt || echo "0")
THROUGHPUT=$((FILTER_COUNT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,scan_filtered,hbase,$DURATION,$FILTER_COUNT,$THROUGHPUT,column_family_filter" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Filtered columns: $FILTER_COUNT"

# Test 4: GET specific record
echo "🔍 Test 4: GET specific record by ID"
SAMPLE_ID=$(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_scan_output.txt | head -1)
if [ -n "$SAMPLE_ID" ]; then
    START_TIME=$(date +%s%3N)
    echo "get 'soalUjian', '$SAMPLE_ID'" | hbase shell > /tmp/hbase_get_output.txt 2>/dev/null
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    THROUGHPUT=$((1000 / DURATION))

    echo "$TIMESTAMP,$SCALE,2,get_by_id,hbase,$DURATION,1,$THROUGHPUT,single_record_lookup" >> $RESULTS_FILE
    echo "   Duration: ${DURATION}ms"
else
    echo "   ⚠️  No sample ID found for GET test"
fi

# Test 5: Multiple GET operations (5 records)
echo "🔍 Test 5: Multiple GET operations (5 records)"
SAMPLE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_scan_output.txt | head -5))

START_TIME=$(date +%s%3N)
for id in "${SAMPLE_IDS[@]}"; do
    echo "get 'soalUjian', '$id'" | hbase shell > /dev/null 2>&1
done
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
RECORD_COUNT=${#SAMPLE_IDS[@]}
THROUGHPUT=$((RECORD_COUNT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,multiple_gets,hbase,$DURATION,$RECORD_COUNT,$THROUGHPUT,5_individual_gets" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms for $RECORD_COUNT records"

echo "✅ HBase benchmarks completed for scale $SCALE"

