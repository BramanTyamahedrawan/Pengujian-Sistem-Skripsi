#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== POSTGRESQL PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

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

# Load test data first (not benchmarked)
echo "📦 Loading test data into PostgreSQL..."
echo "⏳ This may take a while for scale $SCALE..."

# Navigate to data directory
DATA_DIR="$BASE_DIR/test-data-scaled"
if [ ! -d "$DATA_DIR" ]; then
    echo "❌ Data directory not found: $DATA_DIR"
    echo "💡 Please run generate_test_data.sh first"
    exit 1
fi

echo "📂 Changing to data directory: $DATA_DIR"
cd "$DATA_DIR" || exit 1

# Check if CSV files exist
echo "📋 Checking required CSV files..."
MISSING_FILES=0
for file in "schools_${SCALE}.csv" "taksonomi_${SCALE}.csv" "konsentrasi_${SCALE}.csv" "soal_psql_${SCALE}.csv"; do
    if [ ! -f "$file" ]; then
        echo "   ❌ Missing: $file"
        MISSING_FILES=1
    else
        LINES=$(wc -l < "$file")
        echo "   ✅ Found: $file ($LINES lines)"
    fi
done

if [ $MISSING_FILES -eq 1 ]; then
    echo "❌ Missing CSV files. Please run: ./generate_test_data.sh $SCALE"
    exit 1
fi

# Clear existing data
echo "🔧 Clearing existing data..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "TRUNCATE TABLE soalujian CASCADE; TRUNCATE TABLE konsentrasikeahliansekolah CASCADE; TRUNCATE TABLE taksonomi CASCADE; TRUNCATE TABLE schools CASCADE;" > /dev/null 2>&1

# Load data with timeout protection (equivalent to HBase loading with timeout)
echo "📥 Loading data with timeout protection..."
timeout 1200 bash -c "
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"\\COPY schools FROM 'schools_${SCALE}.csv' WITH (DELIMITER E'\\t', NULL '\\\\N')\" > /dev/null 2>&1
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"\\COPY taksonomi FROM 'taksonomi_${SCALE}.csv' WITH (DELIMITER E'\\t', NULL '\\\\N')\" > /dev/null 2>&1
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"\\COPY konsentrasikeahliansekolah FROM 'konsentrasi_${SCALE}.csv' WITH (DELIMITER E'\\t', NULL '\\\\N')\" > /dev/null 2>&1
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"\\COPY soalujian FROM 'soal_psql_${SCALE}.csv' WITH (DELIMITER E'\\t', NULL '\\\\N')\" > /dev/null 2>&1
" &

LOAD_PID=$!

# Wait for load to complete or timeout
wait $LOAD_PID 2>/dev/null
LOAD_RESULT=$?

if [ $LOAD_RESULT -ne 0 ]; then
    echo "⚠️  PostgreSQL data load timed out or failed for scale $SCALE, skipping tests"
    cd "$BASE_DIR"
    exit 0
fi

echo "✅ PostgreSQL data loaded successfully"

# Return to base directory for tests
cd "$BASE_DIR"

# PHASE 2: Read Performance Tests (Equivalent to HBase)
echo "🔍 PHASE 2: Read Performance Tests"

# Test 1: COUNT all records (equivalent to HBase COUNT)
echo "🔍 Test 1: COUNT all records"
START_TIME=$(date +%s%3N)

COUNT_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalujian;" 2>/dev/null | tr -d ' ')

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Validasi count hasil
if [[ -z "$COUNT_RESULT" || ! "$COUNT_RESULT" =~ ^[0-9]+$ ]]; then
    COUNT_RESULT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$COUNT_RESULT" -gt 0 ]; then
    THROUGHPUT=$((COUNT_RESULT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,count_all,postgresql,$DURATION,$COUNT_RESULT,$THROUGHPUT,full_table_count" >> "$RESULTS_FILE"
echo "   ✅ COUNT Duration: ${DURATION}ms, Records: $COUNT_RESULT, Throughput: ${THROUGHPUT} rps"

# Test 2: SELECT all records with LIMIT (equivalent to HBase SCAN ALL)
echo "🔍 Test 2: SELECT all records with LIMIT"
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian LIMIT $SCALE;" > /tmp/psql_scan_all.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Hitung jumlah baris hasil select
SCAN_COUNT=$(grep -v '^$' /tmp/psql_scan_all.txt | grep -v '^-' | grep -v '^(' | grep -c '^[^|]*|' || echo "0")

if [[ -z "$SCAN_COUNT" || ! "$SCAN_COUNT" =~ ^[0-9]+$ ]]; then
    SCAN_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$SCAN_COUNT" -gt 0 ]; then
    THROUGHPUT=$((SCAN_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,select_all,postgresql,$DURATION,$SCAN_COUNT,$THROUGHPUT,full_table_select_with_limit" >> "$RESULTS_FILE"
echo "   ✅ SELECT ALL Duration: ${DURATION}ms, Selected: $SCAN_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 3: SELECT with specific columns (equivalent to HBase SCAN FILTERED)
echo "🔍 Test 3: SELECT with specific columns"
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT idsoalujian, namaujian, pertanyaan, jenissoal, bobot FROM soalujian LIMIT 100;" > /tmp/psql_filtered.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

FILTER_COUNT=$(grep -v '^$' /tmp/psql_filtered.txt | grep -v '^-' | grep -v '^(' | grep -c '^[^|]*|' || echo "0")

if [[ -z "$FILTER_COUNT" || ! "$FILTER_COUNT" =~ ^[0-9]+$ ]]; then
    FILTER_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$FILTER_COUNT" -gt 0 ]; then
    THROUGHPUT=$((FILTER_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,select_filtered,postgresql,$DURATION,$FILTER_COUNT,$THROUGHPUT,main_columns_select" >> "$RESULTS_FILE"
echo "   ✅ SELECT FILTERED Duration: ${DURATION}ms, Filtered: $FILTER_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 4: SELECT specific columns only (equivalent to HBase SCAN SELECTIVE)
echo "🔍 Test 4: SELECT specific columns only"
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT pertanyaan, bobot FROM soalujian LIMIT 50;" > /tmp/psql_selective.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

SELECTIVE_COUNT=$(grep -v '^$' /tmp/psql_selective.txt | grep -v '^-' | grep -v '^(' | grep -c '^[^|]*|' || echo "0")

if [[ -z "$SELECTIVE_COUNT" || ! "$SELECTIVE_COUNT" =~ ^[0-9]+$ ]]; then
    SELECTIVE_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$SELECTIVE_COUNT" -gt 0 ]; then
    THROUGHPUT=$((SELECTIVE_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,select_selective_columns,postgresql,$DURATION,$SELECTIVE_COUNT,$THROUGHPUT,selective_columns_select" >> "$RESULTS_FILE"
echo "   ✅ SELECT SELECTIVE Duration: ${DURATION}ms, Records: $SELECTIVE_COUNT, Throughput: ${THROUGHPUT} rps"

# Test 5: SELECT by specific ID (equivalent to HBase GET)
echo "🔍 Test 5: SELECT by specific ID"
SAMPLE_ID=$(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/psql_scan_all.txt | head -1)

if [ -n "$SAMPLE_ID" ]; then
    START_TIME=$(date +%s%3N)
    
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian WHERE idsoalujian = '$SAMPLE_ID';" > /tmp/psql_get_by_id.txt 2>/dev/null
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    
    if [ "$DURATION" -gt 0 ]; then
        THROUGHPUT=$((1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,select_by_id,postgresql,$DURATION,1,$THROUGHPUT,single_record_lookup" >> "$RESULTS_FILE"
    echo "   ✅ SELECT BY ID Duration: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
else
    echo "   ⚠️  No sample ID found for SELECT BY ID test"
fi

# Test 6: SELECT specific columns by ID (equivalent to HBase GET SELECTIVE)
echo "🔍 Test 6: SELECT specific columns by ID"
if [ -n "$SAMPLE_ID" ]; then
    START_TIME=$(date +%s%3N)
    
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT pertanyaan, bobot FROM soalujian WHERE idsoalujian = '$SAMPLE_ID';" > /tmp/psql_get_selective.txt 2>/dev/null
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    
    if [ "$DURATION" -gt 0 ]; then
        THROUGHPUT=$((1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,select_selective_by_id,postgresql,$DURATION,1,$THROUGHPUT,selective_columns_by_id" >> "$RESULTS_FILE"
    echo "   ✅ SELECT SELECTIVE BY ID Duration: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
fi

# Test 7: Multiple SELECT operations (equivalent to HBase BATCH GET)
echo "🔍 Test 7: Multiple SELECT operations (5 records)"
SAMPLE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/psql_scan_all.txt | head -5))

if [ ${#SAMPLE_IDS[@]} -gt 0 ]; then
    START_TIME=$(date +%s%3N)
    
    # Multiple SELECT operations (individual queries like standard PostgreSQL)
    for id in "${SAMPLE_IDS[@]}"; do
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian WHERE idsoalujian = '$id';" > /dev/null 2>&1
    done
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    RECORD_COUNT=${#SAMPLE_IDS[@]}
    
    if [ "$DURATION" -gt 0 ] && [ "$RECORD_COUNT" -gt 0 ]; then
        THROUGHPUT=$((RECORD_COUNT * 1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,multiple_selects,postgresql,$DURATION,$RECORD_COUNT,$THROUGHPUT,5_individual_selects" >> "$RESULTS_FILE"
    echo "   ✅ MULTIPLE SELECT Duration: ${DURATION}ms for $RECORD_COUNT records, Throughput: ${THROUGHPUT} rps"
fi

# Test 8: Range SELECT (equivalent to HBase RANGE SCAN)
echo "🔍 Test 8: Range SELECT with ORDER BY"
if [ ${#SAMPLE_IDS[@]} -ge 2 ]; then
    START_KEY=${SAMPLE_IDS[0]}
    STOP_KEY=${SAMPLE_IDS[1]}
    
    START_TIME=$(date +%s%3N)
    
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian WHERE idsoalujian >= '$START_KEY' AND idsoalujian <= '$STOP_KEY' ORDER BY idsoalujian LIMIT 10;" > /tmp/psql_range_select.txt 2>/dev/null
    
    END_TIME=$(date +%s%3N)
    DURATION=$((END_TIME - START_TIME))
    
    RANGE_COUNT=$(grep -v '^$' /tmp/psql_range_select.txt | grep -v '^-' | grep -v '^(' | grep -c '^[^|]*|' || echo "0")
    
    if [[ -z "$RANGE_COUNT" || ! "$RANGE_COUNT" =~ ^[0-9]+$ ]]; then
        RANGE_COUNT=0
    fi
    
    if [ "$DURATION" -gt 0 ] && [ "$RANGE_COUNT" -gt 0 ]; then
        THROUGHPUT=$((RANGE_COUNT * 1000 / DURATION))
    else
        THROUGHPUT=0
    fi

    echo "$TIMESTAMP,$SCALE,2,range_select,postgresql,$DURATION,$RANGE_COUNT,$THROUGHPUT,range_select_with_keys" >> "$RESULTS_FILE"
    echo "   ✅ RANGE SELECT Duration: ${DURATION}ms, Records: $RANGE_COUNT, Throughput: ${THROUGHPUT} rps"
fi

# Test 9: COUNT with WHERE filter (equivalent to HBase COUNT FILTERED)
echo "🔍 Test 9: COUNT with WHERE filter"
START_TIME=$(date +%s%3N)

FILTERED_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalujian WHERE jenissoal IS NOT NULL;" 2>/dev/null | tr -d ' ')

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

if [[ -z "$FILTERED_COUNT" || ! "$FILTERED_COUNT" =~ ^[0-9]+$ ]]; then
    FILTERED_COUNT=0
fi

if [ "$DURATION" -gt 0 ] && [ "$FILTERED_COUNT" -gt 0 ]; then
    THROUGHPUT=$((FILTERED_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,count_filtered,postgresql,$DURATION,$FILTERED_COUNT,$THROUGHPUT,count_with_where_filter" >> "$RESULTS_FILE"
echo "   ✅ COUNT FILTERED Duration: ${DURATION}ms, Records: $FILTERED_COUNT, Throughput: ${THROUGHPUT} rps"

# Cleanup temporary files
rm -f /tmp/psql_*.txt

echo "✅ PostgreSQL benchmarks completed for scale $SCALE"

# Performance summary
echo ""
echo "📝 POSTGRESQL PERFORMANCE SUMMARY (Scale: $SCALE)"
echo "================================================="
echo "📊 Test Results Summary:"
echo "  - 🔢 COUNT: Full table count"
echo "  - 🔍 SELECT ALL: Complete table select with limit"
echo "  - 🔍 SELECT FILTERED: Main columns selection"
echo "  - 🔍 SELECT SELECTIVE: Specific columns only"
echo "  - 📄 SELECT BY ID: Single record by ID"
echo "  - 📄 SELECT SELECTIVE BY ID: Specific columns by ID"
echo "  - 📄 MULTIPLE SELECT: 5 individual select operations"
echo "  - 📊 RANGE SELECT: Select with range conditions"
echo "  - 🔢 COUNT FILTERED: Count with WHERE filter"
echo ""
