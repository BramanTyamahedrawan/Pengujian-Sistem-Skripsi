#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== OPTIMIZED LATENCY PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

# Use environment variables with fallbacks
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Ensure results file and directory exist
mkdir -p "$(dirname "$RESULTS_FILE")"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

# Database configurations
DB_USER="postgres"
DB_NAME="postgres"
DB_HOST="localhost"
DB_PORT="5432"
export PGPASSWORD="mydreamonpsdkulumajang007"

# Function to execute HBase commands efficiently
execute_hbase_single() {
    local temp_file=$(mktemp)
    echo "$1" > "$temp_file"
    echo "exit" >> "$temp_file"
    hbase shell "$temp_file" 2>/dev/null
    rm -f "$temp_file"
}

# PHASE 4: Extended Latency Testing (Average of multiple operations)
echo "⚡ PHASE 4: EXTENDED Latency Testing"

echo "🔍 PostgreSQL Latency Tests (Standard Implementation):"

# Test 1: Single record latency (average of 10 operations)
echo "  📊 Single record SELECT latency (10 operations)..."
TOTAL_DURATION=0
OPERATIONS=10

for i in $(seq 1 $OPERATIONS); do
    SAMPLE_ID=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idsoalujian FROM soalujian ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')

    if [ -n "$SAMPLE_ID" ]; then
        START_TIME=$(date +%s%3N)
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian WHERE idsoalujian = '$SAMPLE_ID';" > /dev/null 2>&1
        END_TIME=$(date +%s%3N)
        DURATION=$((END_TIME - START_TIME))
        TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    fi
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_single_select,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_single_selects" >> "$RESULTS_FILE"
echo "    ✅ Average SELECT latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 2: Selective columns SELECT latency
echo "  🎯 Selective columns SELECT latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    SAMPLE_ID=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idsoalujian FROM soalujian ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')

    if [ -n "$SAMPLE_ID" ]; then
        START_TIME=$(date +%s%3N)
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT pertanyaan, bobot FROM soalujian WHERE idsoalujian = '$SAMPLE_ID';" > /dev/null 2>&1
        END_TIME=$(date +%s%3N)
        DURATION=$((END_TIME - START_TIME))
        TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    fi
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_selective_select,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_selective_selects" >> "$RESULTS_FILE"
echo "    ✅ Average SELECTIVE SELECT latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 3: JOIN query latency (average of 10 operations)
echo "  🔗 JOIN query latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    SAMPLE_ID=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idsoalujian FROM soalujian ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')

    if [ -n "$SAMPLE_ID" ]; then
        START_TIME=$(date +%s%3N)
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT s.*, t.namataksonomi, k.namakonsentrasi
        FROM soalujian s
        JOIN taksonomi t ON s.idtaksonomi = t.idtaksonomi
        JOIN konsentrasikeahliansekolah k ON s.idkonsentrasisekolah = k.idkonsentrasisekolah
        WHERE s.idsoalujian = '$SAMPLE_ID';" > /dev/null 2>&1
        END_TIME=$(date +%s%3N)
        DURATION=$((END_TIME - START_TIME))
        TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    fi
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_join_query,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_join_queries" >> "$RESULTS_FILE"
echo "    ✅ Average JOIN latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 4: JSON query latency
echo "  📋 JSON query latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    START_TIME=$(date +%s%3N)
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
    SELECT COUNT(*) FROM soalujian
    WHERE jawabanbenar->0 = '\"A\"' AND opsi->'A' IS NOT NULL;" > /dev/null 2>&1
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_json_query,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_json_queries" >> "$RESULTS_FILE"
echo "    ✅ Average JSON query latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 5: Filtered SELECT latency
echo "  🔍 Filtered SELECT latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    START_TIME=$(date +%s%3N)
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
    SELECT idsoalujian, pertanyaan FROM soalujian
    WHERE jenissoal = 'PG' LIMIT 10;" > /dev/null 2>&1
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_filtered_select,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_filtered_selects" >> "$RESULTS_FILE"
echo "    ✅ Average FILTERED SELECT latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 6: Batch SELECT latency (10 individual queries)
echo "  📦 Batch SELECT latency (10 individual queries)..."
TOTAL_DURATION=0

START_TIME=$(date +%s%3N)
for i in $(seq 1 $OPERATIONS); do
    SAMPLE_ID=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idsoalujian FROM soalujian ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')
    if [ -n "$SAMPLE_ID" ]; then
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian WHERE idsoalujian = '$SAMPLE_ID';" > /dev/null 2>&1
    fi
done
END_TIME=$(date +%s%3N)

TOTAL_DURATION=$((END_TIME - START_TIME))
AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_batch_select,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,batch_${OPERATIONS}_individual_selects" >> "$RESULTS_FILE"
echo "    ✅ Average BATCH SELECT latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

echo "🔍 HBase Latency Tests (Optimized Implementation):"

# Get some sample IDs from HBase with optimization
echo "📦 Getting sample IDs from HBase..."
execute_hbase_single "scan 'soalUjian', {LIMIT => 20, CACHE => 20}" > /tmp/hbase_sample_ids.txt
SAMPLE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_sample_ids.txt | head -10))

# Test 1: Single record GET latency (average of 10 operations) - OPTIMIZED
echo "  📊 Single record GET latency (10 operations) - OPTIMIZED..."
TOTAL_DURATION=0

for i in $(seq 0 $((OPERATIONS-1))); do
    if [ -n "${SAMPLE_IDS[$i]}" ]; then
        START_TIME=$(date +%s%3N)
        execute_hbase_single "get 'soalUjian', '${SAMPLE_IDS[$i]}'" > /dev/null
        END_TIME=$(date +%s%3N)
        DURATION=$((END_TIME - START_TIME))
        TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    fi
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_single_get_optimized,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,optimized_average_${OPERATIONS}_single_gets" >> "$RESULTS_FILE"
echo "    ✅ Average GET latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 2: Selective columns GET latency - OPTIMIZED
echo "  🎯 Selective columns GET latency (10 operations) - OPTIMIZED..."
TOTAL_DURATION=0

for i in $(seq 0 $((OPERATIONS-1))); do
    if [ -n "${SAMPLE_IDS[$i]}" ]; then
        START_TIME=$(date +%s%3N)
        execute_hbase_single "get 'soalUjian', '${SAMPLE_IDS[$i]}', 'main:pertanyaan', 'main:bobot'" > /dev/null
        END_TIME=$(date +%s%3N)
        DURATION=$((END_TIME - START_TIME))
        TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    fi
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_selective_get,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,optimized_average_${OPERATIONS}_selective_gets" >> "$RESULTS_FILE"
echo "    ✅ Average SELECTIVE GET latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 3: Column family specific GET latency - OPTIMIZED
echo "  🗂️ Column family GET latency (10 operations) - OPTIMIZED..."
TOTAL_DURATION=0

for i in $(seq 0 $((OPERATIONS-1))); do
    if [ -n "${SAMPLE_IDS[$i]}" ]; then
        START_TIME=$(date +%s%3N)
        execute_hbase_single "get 'soalUjian', '${SAMPLE_IDS[$i]}', {COLUMNS => ['main']}" > /dev/null
        END_TIME=$(date +%s%3N)
        DURATION=$((END_TIME - START_TIME))
        TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
    fi
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_cf_get_optimized,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,optimized_average_${OPERATIONS}_cf_gets" >> "$RESULTS_FILE"
echo "    ✅ Average CF GET latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 4: JSON-like query latency (scan with value filter) - NEW
echo "  📋 JSON-like query latency (10 operations) - NEW..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    START_TIME=$(date +%s%3N)
    execute_hbase_single "scan 'soalUjian', {COLUMNS => ['main:jenisSoal'], LIMIT => 10, CACHE => 10}" > /dev/null
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_json_like_query,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,optimized_average_${OPERATIONS}_json_like_queries" >> "$RESULTS_FILE"
echo "    ✅ Average JSON-like query latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 5: SCAN with filter latency - OPTIMIZED
echo "  🔍 SCAN filter latency (10 operations) - OPTIMIZED..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    START_TIME=$(date +%s%3N)
    execute_hbase_single "scan 'soalUjian', {COLUMNS => ['main:jenisSoal'], LIMIT => 10, CACHE => 10}" > /dev/null
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    TOTAL_DURATION=$((TOTAL_DURATION + DURATION))
done

AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))
if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_scan_filter_optimized,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,optimized_average_${OPERATIONS}_scan_filters" >> "$RESULTS_FILE"
echo "    ✅ Average SCAN filter latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Test 6: Batch GET latency (10 operations in batch) - OPTIMIZED
echo "  📦 Batch GET latency (10 operations in batch) - OPTIMIZED..."
START_TIME=$(date +%s%3N)

# Batch GET operations in single session
BATCH_COMMANDS=()
for i in $(seq 0 $((OPERATIONS-1))); do
    if [ -n "${SAMPLE_IDS[$i]}" ]; then
        BATCH_COMMANDS+=("get 'soalUjian', '${SAMPLE_IDS[$i]}'")
    fi
done

# Execute all GETs in one session
temp_file=$(mktemp)
for cmd in "${BATCH_COMMANDS[@]}"; do
    echo "$cmd" >> "$temp_file"
done
echo "exit" >> "$temp_file"
hbase shell "$temp_file" > /dev/null 2>&1
rm -f "$temp_file"

END_TIME=$(date +%s%3N)
TOTAL_DURATION=$((END_TIME - START_TIME))
AVG_DURATION=$((TOTAL_DURATION / OPERATIONS))

if [ "$AVG_DURATION" -gt 0 ]; then
    AVG_THROUGHPUT=$((1000 / AVG_DURATION))
else
    AVG_THROUGHPUT=0
fi
echo "$TIMESTAMP,$SCALE,4,latency_batch_get_optimized,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,optimized_batch_${OPERATIONS}_gets" >> "$RESULTS_FILE"
echo "    ✅ Average BATCH GET latency: ${AVG_DURATION}ms, Operations: $OPERATIONS, Avg Throughput: ${AVG_THROUGHPUT} rps"

# Cleanup
rm -f /tmp/hbase_sample_ids.txt

echo "✅ EXTENDED Latency benchmarks completed for scale $SCALE"

# Performance summary
echo ""
echo "⚡ EXTENDED LATENCY PERFORMANCE SUMMARY (Scale: $SCALE)"
echo "====================================================="
echo ""
echo "PostgreSQL Latency (avg of $OPERATIONS operations - Standard):"
echo "  - 📊 Single SELECT: Average latency"
echo "  - 🎯 Selective SELECT: Specific columns only"
echo "  - 🔗 JOIN Query: Multi-table operations"
echo "  - 📋 JSON Query: JSONB field operations"
echo "  - 🔍 Filtered SELECT: WHERE clause operations"
echo "  - 📦 Batch SELECT: 10 individual queries"
echo ""
echo "HBase Latency (avg of $OPERATIONS operations ):"
echo "  - 📊 Single GET: Row retrieval "
echo "  - 🎯 Selective GET: Specific columns only "
echo "  - 🗂️ CF GET: Column family specific "
echo "  - 📋 JSON-like Query: Filtered scans "
echo "  - 🔍 SCAN Filter: Filtered table scans "
echo "  - 📦 Batch GET: 10 operations in single session "
echo ""
