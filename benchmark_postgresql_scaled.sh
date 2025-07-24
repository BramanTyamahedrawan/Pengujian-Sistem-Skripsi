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
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "TRUNCATE TABLE soalUjian CASCADE; TRUNCATE TABLE konsentrasiKeahlianSekolah CASCADE; TRUNCATE TABLE taksonomi CASC>
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY schools FROM 'schools_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY konsentrasiKeahlianSekolah FROM 'konsentrasi_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY taksonomi FROM 'taksonomi_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY soalUjian FROM 'soal_psql_${SCALE}.csv' WITH (DELIMITER E'\t', NULL '\\N')" > /dev/null 2>&1

# PHASE 2: Read Performance Tests
echo "🔍 PHASE 2: Read Performance Tests"

# Test 1: COUNT all records
echo "🔍 Test 1: COUNT all records"
START_TIME=$(date +%s%3N)
COUNT_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalUjian;" | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((COUNT_RESULT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,count_all,postgresql,$DURATION,$COUNT_RESULT,$THROUGHPUT,full_table_count" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Records: $COUNT_RESULT"
# Test 2: SELECT all records
echo "🔍 Test 2: SELECT all records"
START_TIME=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM soalUjian;" > /tmp/psql_select_output.txt 2>/dev/null
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((COUNT_RESULT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,select_all,postgresql,$DURATION,$COUNT_RESULT,$THROUGHPUT,full_table_select" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Records: $COUNT_RESULT"

# Test 3: SELECT with WHERE clause
echo "🔍 Test 3: SELECT with WHERE clause filter"
START_TIME=$(date +%s%3N)
FILTER_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM soalUjian WHERE jenisSoal = 'PG';" | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((FILTER_RESULT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,select_filtered,postgresql,$DURATION,$FILTER_RESULT,$THROUGHPUT,where_clause_filter" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Filtered records: $FILTER_RESULT"

# Test 4: Complex JOIN query (using proper foreign keys)
echo "🔍 Test 4: Complex JOIN query"
START_TIME=$(date +%s%3N)
JOIN_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
SELECT COUNT(*)
FROM soalUjian s
JOIN taksonomi t ON s.idTaksonomi = t.idTaksonomi
JOIN konsentrasiKeahlianSekolah k ON s.idKonsentrasiSekolah = k.idKonsentrasiSekolah
JOIN schools sch ON k.idSchool = sch.idSchool;" | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((JOIN_RESULT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,complex_join,postgresql,$DURATION,$JOIN_RESULT,$THROUGHPUT,4_table_join" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, Joined records: $JOIN_RESULT"

# Test 5: JSON operations
echo "🔍 Test 5: JSON field operations"
START_TIME=$(date +%s%3N)
JSON_RESULT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
SELECT COUNT(*)
FROM soalUjian
WHERE jawabanBenar->0 = '\"A\"';" | tr -d ' ')
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((JSON_RESULT * 1000 / DURATION))

echo "$TIMESTAMP,$SCALE,2,json_query,postgresql,$DURATION,$JSON_RESULT,$THROUGHPUT,json_field_search" >> $RESULTS_FILE
echo "   Duration: ${DURATION}ms, JSON matches: $JSON_RESULT"

echo "✅ PostgreSQL benchmarks completed for scale $SCALE"

