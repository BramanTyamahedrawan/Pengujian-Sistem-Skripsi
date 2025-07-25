#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== LATENCY PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

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

# PHASE 4: Latency Testing (Average of multiple operations)
echo "⚡ PHASE 4: Latency Testing"

echo "🔍 PostgreSQL Latency Tests:"

# Test 1: Single record latency (average of 10 operations)
echo "  Single record SELECT latency (10 operations)..."
TOTAL_DURATION=0
OPERATIONS=10

for i in $(seq 1 $OPERATIONS); do
    SAMPLE_ID=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idSoalUjian FROM soalUjian ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')

    if [ -n "$SAMPLE_ID" ]; then
        START_TIME=$(date +%s%3N)
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalUjian WHERE idSoalUjian = '$SAMPLE_ID';" > /dev/null 2>&1
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
echo "$TIMESTAMP,$SCALE,4,latency_single_select,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_single_selects" >> $RESULTS_FILE
echo "    Average SELECT latency: ${AVG_DURATION}ms"

# Test 2: JOIN query latency (average of 10 operations)
echo "  JOIN query latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    SAMPLE_ID=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idSoalUjian FROM soalUjian ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')

    if [ -n "$SAMPLE_ID" ]; then
        START_TIME=$(date +%s%3N)
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT s.*, t.namaTaksonomi, k.namaKonsentrasi, sch.namaSekolah
        FROM soalUjian s
        JOIN taksonomi t ON s.idTaksonomi = t.idTaksonomi
        JOIN konsentrasiKeahlianSekolah k ON s.idKonsentrasiSekolah = k.idKonsentrasiSekolah
        JOIN schools sch ON k.idSchool = sch.idSchool
        WHERE s.idSoalUjian = '$SAMPLE_ID';" > /dev/null 2>&1
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
echo "$TIMESTAMP,$SCALE,4,latency_join_query,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_join_queries" >> $RESULTS_FILE
echo "    Average JOIN latency: ${AVG_DURATION}ms"

# Test 3: JSON query latency
echo "  JSON query latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    START_TIME=$(date +%s%3N)
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
    SELECT COUNT(*) FROM soalUjian
    WHERE jawabanBenar->0 = '\"A\"' AND opsi->'A' IS NOT NULL;" > /dev/null 2>&1
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
echo "$TIMESTAMP,$SCALE,4,latency_json_query,postgresql,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_json_queries" >> $RESULTS_FILE
echo "    Average JSON query latency: ${AVG_DURATION}ms"

echo "🔍 HBase Latency Tests:"

# Get some sample IDs from HBase
echo "scan 'soalUjian', {LIMIT => 20}" | hbase shell 2>/dev/null > /tmp/hbase_sample_ids.txt
SAMPLE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_sample_ids.txt | head -10))

# Test 1: Single record GET latency (average of 10 operations)
echo "  Single record GET latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 0 $((OPERATIONS-1))); do
    if [ -n "${SAMPLE_IDS[$i]}" ]; then
        START_TIME=$(date +%s%3N)
        echo "get 'soalUjian', '${SAMPLE_IDS[$i]}'" | hbase shell > /dev/null 2>&1
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
echo "$TIMESTAMP,$SCALE,4,latency_single_get,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_single_gets" >> $RESULTS_FILE
echo "    Average GET latency: ${AVG_DURATION}ms"

# Test 2: Column family specific GET latency
echo "  Column family GET latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 0 $((OPERATIONS-1))); do
    if [ -n "${SAMPLE_IDS[$i]}" ]; then
        START_TIME=$(date +%s%3N)
        echo "get 'soalUjian', '${SAMPLE_IDS[$i]}', {COLUMNS => ['main:']}" | hbase shell > /dev/null 2>&1
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
echo "$TIMESTAMP,$SCALE,4,latency_cf_get,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_cf_gets" >> $RESULTS_FILE
echo "    Average CF GET latency: ${AVG_DURATION}ms"

# Test 3: SCAN with filter latency
echo "  SCAN filter latency (10 operations)..."
TOTAL_DURATION=0

for i in $(seq 1 $OPERATIONS); do
    START_TIME=$(date +%s%3N)
    echo "scan 'soalUjian', {COLUMNS => ['main:jenisSoal'], LIMIT => 10}" | hbase shell > /dev/null 2>&1
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
echo "$TIMESTAMP,$SCALE,4,latency_scan_filter,hbase,$AVG_DURATION,$OPERATIONS,$AVG_THROUGHPUT,average_${OPERATIONS}_scan_filters" >> $RESULTS_FILE
echo "    Average SCAN filter latency: ${AVG_DURATION}ms"

echo "✅ Latency benchmarks completed for scale $SCALE"

