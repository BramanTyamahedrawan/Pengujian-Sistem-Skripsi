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

# Clear existing data
echo "🔧 Clearing existing data..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "TRUNCATE TABLE soalujian CASCADE; TRUNCATE TABLE konsentrasikeahliansekolah CASCADE; TRUNCATE TABLE taksonomi CASCADE; TRUNCATE TABLE schools CASCADE;" > /dev/null 2>&1

# Load data with detailed logging
echo "📥 Loading schools data..."
LOAD_START=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY schools FROM 'schools_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1
LOAD_RESULT=$?
LOAD_END=$(date +%s%3N)
LOAD_DURATION=$((LOAD_END - LOAD_START))

if [ $LOAD_RESULT -eq 0 ]; then
    SCHOOLS_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM schools;" 2>/dev/null | tr -d ' ')
    echo "   ✅ Schools loaded: $SCHOOLS_COUNT records (${LOAD_DURATION}ms)"
else
    echo "   ❌ Schools loading failed"
    SCHOOLS_COUNT=0
fi

echo "📥 Loading taksonomi data..."
LOAD_START=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY taksonomi FROM 'taksonomi_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1
LOAD_RESULT=$?
LOAD_END=$(date +%s%3N)
LOAD_DURATION=$((LOAD_END - LOAD_START))

if [ $LOAD_RESULT -eq 0 ]; then
    TAKSONOMI_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM taksonomi;" 2>/dev/null | tr -d ' ')
    echo "   ✅ Taksonomi loaded: $TAKSONOMI_COUNT records (${LOAD_DURATION}ms)"
else
    echo "   ❌ Taksonomi loading failed"
    TAKSONOMI_COUNT=0
fi

echo "📥 Loading konsentrasi data..."
LOAD_START=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY konsentrasikeahliansekolah FROM 'konsentrasi_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1
LOAD_RESULT=$?
LOAD_END=$(date +%s%3N)
LOAD_DURATION=$((LOAD_END - LOAD_START))

if [ $LOAD_RESULT -eq 0 ]; then
    KONSENTRASI_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM konsentrasikeahliansekolah;" 2>/dev/null | tr -d ' ')
    echo "   ✅ Konsentrasi loaded: $KONSENTRASI_COUNT records (${LOAD_DURATION}ms)"
else
    echo "   ❌ Konsentrasi loading failed"
    KONSENTRASI_COUNT=0
fi

echo "📥 Loading soal ujian data..."
echo "⏳ This may take a while for scale $SCALE..."
LOAD_START=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY soalujian FROM 'soal_psql_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1
LOAD_RESULT=$?
LOAD_END=$(date +%s%3N)
LOAD_DURATION=$((LOAD_END - LOAD_START))

if [ $LOAD_RESULT -eq 0 ]; then
    SOAL_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalujian;" 2>/dev/null | tr -d ' ')
    echo "   ✅ Soal Ujian loaded: $SOAL_COUNT records (${LOAD_DURATION}ms)"
else
    echo "   ❌ Soal Ujian loading failed"
    SOAL_COUNT=0
fi

# Data loading summary
TOTAL_RECORDS=$((SCHOOLS_COUNT + TAKSONOMI_COUNT + KONSENTRASI_COUNT + SOAL_COUNT))
echo "✅ PostgreSQL data loaded successfully"
echo "📊 Summary: schools=$SCHOOLS_COUNT, taksonomi=$TAKSONOMI_COUNT, konsentrasi=$KONSENTRASI_COUNT, soal=$SOAL_COUNT (Total: $TOTAL_RECORDS)"

# Check if we have enough data to proceed
if [ "$SOAL_COUNT" -eq 0 ]; then
    echo "⚠️  No soal ujian data loaded, skipping performance tests"
    exit 0
fi

# PHASE 2: Read Performance Tests
echo "🔍 PHASE 2: Read Performance Tests"

# Test 1: COUNT all records
echo "🔍 Test 1: COUNT all records"
START_TIME=$(date +%s%3N)
COUNT_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalujian;" 2>/dev/null | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Validate COUNT_RESULT is a number
if [[ "$COUNT_RESULT" =~ ^[0-9]+$ ]] && [ "$COUNT_RESULT" -gt 0 ] && [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((COUNT_RESULT * 1000 / DURATION))
else
    COUNT_RESULT=0
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,count_all,postgresql,$DURATION,$COUNT_RESULT,$THROUGHPUT,full_table_count" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Records: $COUNT_RESULT"

# Test 2: SELECT all records
echo "🔍 Test 2: SELECT all records"
START_TIME=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalujian;" > /tmp/psql_select_output.txt 2>/dev/null
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

if [ "$COUNT_RESULT" -gt 0 ] && [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((COUNT_RESULT * 1000 / DURATION))
else
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,select_all,postgresql,$DURATION,$COUNT_RESULT,$THROUGHPUT,full_table_select" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Records: $COUNT_RESULT"

# Test 3: SELECT with WHERE clause (menggunakan nama kolom yang benar: jenissoal)
echo "🔍 Test 3: SELECT with WHERE clause filter"
START_TIME=$(date +%s%3N)
FILTER_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalujian WHERE jenissoal = 'PG';" 2>/dev/null | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Validate FILTER_RESULT
if [[ "$FILTER_RESULT" =~ ^[0-9]+$ ]] && [ "$FILTER_RESULT" -gt 0 ] && [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((FILTER_RESULT * 1000 / DURATION))
else
    FILTER_RESULT=0
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,select_filter,postgresql,$DURATION,$FILTER_RESULT,$THROUGHPUT,where_clause_filter" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Filtered records: $FILTER_RESULT"

# Test 4: Complex JOIN query (menggunakan nama kolom yang benar)
echo "🔍 Test 4: Complex JOIN query"
START_TIME=$(date +%s%3N)
JOIN_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
    SELECT COUNT(*) 
    FROM soalujian s
    JOIN taksonomi t ON s.idtaksonomi = t.idtaksonomi
    JOIN konsentrasikeahliansekolah k ON s.idkonsentrasisekolah = k.idkonsentrasisekolah
    JOIN schools sch ON k.idschool = sch.idschool;" 2>/dev/null | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Validate JOIN_RESULT
if [[ "$JOIN_RESULT" =~ ^[0-9]+$ ]] && [ "$JOIN_RESULT" -gt 0 ] && [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((JOIN_RESULT * 1000 / DURATION))
else
    JOIN_RESULT=0
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,complex_join,postgresql,$DURATION,$JOIN_RESULT,$THROUGHPUT,four_table_join" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Joined records: $JOIN_RESULT"

# Test 5: JSON field operations (menggunakan nama kolom yang benar: jawabanbenar, opsi)
echo "🔍 Test 5: JSON field operations"
START_TIME=$(date +%s%3N)
JSON_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
    SELECT COUNT(*) FROM soalujian
    WHERE jawabanbenar->0 = '\"A\"' AND opsi->'A' IS NOT NULL;" 2>/dev/null | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))

# Validate JSON_RESULT
if [[ "$JSON_RESULT" =~ ^[0-9]+$ ]] && [ "$JSON_RESULT" -gt 0 ] && [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((JSON_RESULT * 1000 / DURATION))
else
    JSON_RESULT=0
    THROUGHPUT=0
fi

echo "$TIMESTAMP,$SCALE,2,json_operations,postgresql,$DURATION,$JSON_RESULT,$THROUGHPUT,jsonb_field_queries" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, JSON matches: $JSON_RESULT"

echo "✅ PostgreSQL benchmarks completed for scale $SCALE"

# Performance summary
echo ""
echo "📈 POSTGRESQL PERFORMANCE SUMMARY (Scale: $SCALE)"
echo "=================================================="
echo "Data Loading: $TOTAL_RECORDS total records"
echo "COUNT Query: ${COUNT_RESULT} records"
echo "Filter Query: ${FILTER_RESULT} records"
echo "JOIN Query: ${JOIN_RESULT} records"
echo "JSON Query: ${JSON_RESULT} records"
echo ""