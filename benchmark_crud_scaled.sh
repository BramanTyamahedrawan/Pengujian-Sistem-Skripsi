#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== OPTIMIZED CRUD PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

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

# Function to generate random UUID
generate_uuid() {
    cat /proc/sys/kernel/random/uuid
}

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

# PHASE 3: CRUD Operations
echo "📝 PHASE 3: EXTENDED CRUD Operations"

# Get existing foreign key values for PostgreSQL
EXISTING_TAKSONOMI=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idtaksonomi FROM taksonomi ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')
EXISTING_KONSENTRASI=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idkonsentrasisekolah FROM konsentrasikeahliansekolah ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')

echo "🔍 PostgreSQL CRUD Tests:"

# CREATE (INSERT) - PostgreSQL
echo "  ➕ CREATE (INSERT) test..."
TEST_ID=$(generate_uuid)
TEST_QUESTION="Test CRUD question for scale $SCALE"
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
INSERT INTO soalujian (
    idsoalujian, namaujian, pertanyaan, jenissoal, bobot,
    jawabanbenar, opsi, idtaksonomi, idkonsentrasisekolah, createdby
) VALUES (
    '$TEST_ID', 'CRUD Test $SCALE', '$TEST_QUESTION', 'PG', 5,
    '[\"A\"]'::jsonb, '{\"A\":\"Option A\"}'::jsonb,
    '$EXISTING_TAKSONOMI', '$EXISTING_KONSENTRASI', 'CRUDTester'
);" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_insert,postgresql,$DURATION,1,$THROUGHPUT,single_record_insert_with_fk" >> "$RESULTS_FILE"
echo "    ✅ INSERT Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# READ (SELECT) - PostgreSQL
echo "  🔍 READ (SELECT) test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT s.*, t.namataksonomi, k.namakonsentrasi
FROM soalujian s
JOIN taksonomi t ON s.idtaksonomi = t.idtaksonomi
JOIN konsentrasikeahliansekolah k ON s.idkonsentrasisekolah = k.idkonsentrasisekolah
WHERE s.idsoalujian = '$TEST_ID';" > /tmp/psql_crud_read.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_select_join,postgresql,$DURATION,1,$THROUGHPUT,single_record_select_with_join" >> "$RESULTS_FILE"
echo "    ✅ SELECT with JOIN Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# READ (SELECT) specific columns - PostgreSQL
echo "  🔍 READ (SELECT selective) test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT s.pertanyaan, s.bobot
FROM soalujian s
WHERE s.idsoalujian = '$TEST_ID';" > /tmp/psql_crud_selective.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_select_selective,postgresql,$DURATION,1,$THROUGHPUT,selective_columns_select" >> "$RESULTS_FILE"
echo "    ✅ SELECT SELECTIVE Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# UPDATE - PostgreSQL
echo "  ✏️ UPDATE test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
UPDATE soalujian
SET pertanyaan = 'Updated: $TEST_QUESTION', bobot = 10
WHERE idsoalujian = '$TEST_ID';" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_update,postgresql,$DURATION,1,$THROUGHPUT,single_record_update" >> "$RESULTS_FILE"
echo "    ✅ UPDATE Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# DELETE - PostgreSQL
echo "  🗑️ DELETE test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DELETE FROM soalujian WHERE idsoalujian = '$TEST_ID';" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_delete,postgresql,$DURATION,1,$THROUGHPUT,single_record_delete" >> "$RESULTS_FILE"
echo "    ✅ DELETE Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# BULK TEST - PostgreSQL (tanpa optimasi, menggunakan cara standard)
echo "  🚀 BULK Operations test (10 individual inserts)..."
START_TIME=$(date +%s%3N)

BULK_IDS=()
for i in {1..10}; do
    BULK_ID=$(generate_uuid)
    BULK_IDS+=("$BULK_ID")
    
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
    INSERT INTO soalujian (
        idsoalujian, namaujian, pertanyaan, jenissoal, bobot,
        jawabanbenar, opsi, idtaksonomi, idkonsentrasisekolah, createdby
    ) VALUES (
        '$BULK_ID', 'Bulk Test $i', 'Bulk question $i for scale $SCALE', 'PG', $i,
        '[\"A\"]'::jsonb, '{\"A\":\"Option A\"}'::jsonb,
        '$EXISTING_TAKSONOMI', '$EXISTING_KONSENTRASI', 'BulkTester'
    );" > /dev/null 2>&1
done

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((10000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_bulk_insert,postgresql,$DURATION,10,$THROUGHPUT,bulk_10_records_individual" >> "$RESULTS_FILE"
echo "    ✅ BULK INSERT (10 records) Duration: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Cleanup bulk test records - PostgreSQL
for bulk_id in "${BULK_IDS[@]}"; do
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
    DELETE FROM soalujian WHERE idsoalujian = '$bulk_id';" > /dev/null 2>&1
done

echo "🔍 OPTIMIZED HBase CRUD Tests:"

# CREATE (PUT) - HBase OPTIMIZED dengan batch
echo "  ➕ CREATE (PUT) test - OPTIMIZED..."
TEST_ID=$(generate_uuid)
TEST_QUESTION="Test CRUD question for scale $SCALE"
START_TIME=$(date +%s%3N)

# Batch PUT operations untuk mengurangi overhead
execute_hbase_batch \
    "put 'soalUjian', '$TEST_ID', 'main:idSoalUjian', '$TEST_ID'" \
    "put 'soalUjian', '$TEST_ID', 'main:namaUjian', 'CRUD Test $SCALE'" \
    "put 'soalUjian', '$TEST_ID', 'main:pertanyaan', '$TEST_QUESTION'" \
    "put 'soalUjian', '$TEST_ID', 'main:jenisSoal', 'PG'" \
    "put 'soalUjian', '$TEST_ID', 'main:bobot', '5'" \
    "put 'soalUjian', '$TEST_ID', 'main:jawabanBenar', '[\"A\"]'" \
    "put 'soalUjian', '$TEST_ID', 'main:opsi', '{\"A\":\"Option A\"}'" \
    "put 'soalUjian', '$TEST_ID', 'refs:idTaksonomi', '$EXISTING_TAKSONOMI'" \
    "put 'soalUjian', '$TEST_ID', 'refs:idKonsentrasiSekolah', '$EXISTING_KONSENTRASI'" \
    "put 'soalUjian', '$TEST_ID', 'detail:createdBy', 'CRUDTester'"

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_put_batch,hbase,$DURATION,1,$THROUGHPUT,batch_put_multi_cf" >> "$RESULTS_FILE"
echo "    ✅ BATCH PUT Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# READ (GET) - HBase OPTIMIZED
echo "  🔍 READ (GET) test - OPTIMIZED..."
START_TIME=$(date +%s%3N)

execute_hbase_single "get 'soalUjian', '$TEST_ID'" > /tmp/hbase_crud_read.txt

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_get_optimized,hbase,$DURATION,1,$THROUGHPUT,single_get_all_cf" >> "$RESULTS_FILE"
echo "    ✅ GET Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# READ (GET) specific columns - untuk test performa selective read
echo "  🔍 READ (GET selective) test..."
START_TIME=$(date +%s%3N)

execute_hbase_single "get 'soalUjian', '$TEST_ID', 'main:pertanyaan', 'main:bobot'" > /tmp/hbase_crud_selective.txt

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_get_selective,hbase,$DURATION,1,$THROUGHPUT,selective_columns_get" >> "$RESULTS_FILE"
echo "    ✅ GET SELECTIVE Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# UPDATE (PUT with new values) - HBase OPTIMIZED dengan batch
echo "  ✏️ UPDATE test - OPTIMIZED..."
START_TIME=$(date +%s%3N)

execute_hbase_batch \
    "put 'soalUjian', '$TEST_ID', 'main:pertanyaan', 'Updated: $TEST_QUESTION'" \
    "put 'soalUjian', '$TEST_ID', 'main:bobot', '10'"

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_update_batch,hbase,$DURATION,1,$THROUGHPUT,batch_update_cf" >> "$RESULTS_FILE"
echo "    ✅ BATCH UPDATE Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# DELETE - HBase OPTIMIZED
echo "  🗑️ DELETE test - OPTIMIZED..."
START_TIME=$(date +%s%3N)

execute_hbase_single "deleteall 'soalUjian', '$TEST_ID'"

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_delete_optimized,hbase,$DURATION,1,$THROUGHPUT,deleteall_row" >> "$RESULTS_FILE"
echo "    ✅ DELETE Duration: ${DURATION}ms, Records: 1, Throughput: ${THROUGHPUT} rps"

# BULK TEST - untuk simulasi beban tinggi seperti 10M data
echo "  🚀 BULK Operations test (mini batch)..."
START_TIME=$(date +%s%3N)

# Create multiple records in one batch untuk test throughput
BULK_IDS=()
BULK_COMMANDS=()

for i in {1..10}; do
    BULK_ID=$(generate_uuid)
    BULK_IDS+=("$BULK_ID")
    BULK_COMMANDS+=(
        "put 'soalUjian', '$BULK_ID', 'main:idSoalUjian', '$BULK_ID'"
        "put 'soalUjian', '$BULK_ID', 'main:namaUjian', 'Bulk Test $i'"
        "put 'soalUjian', '$BULK_ID', 'main:pertanyaan', 'Bulk question $i for scale $SCALE'"
        "put 'soalUjian', '$BULK_ID', 'main:jenisSoal', 'PG'"
        "put 'soalUjian', '$BULK_ID', 'main:bobot', '$i'"
    )
done

execute_hbase_batch "${BULK_COMMANDS[@]}"

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((10000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_bulk_insert,hbase,$DURATION,10,$THROUGHPUT,bulk_10_records_batch" >> "$RESULTS_FILE"
echo "    ✅ BULK INSERT (10 records) Duration: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Cleanup bulk test records
CLEANUP_COMMANDS=()
for bulk_id in "${BULK_IDS[@]}"; do
    CLEANUP_COMMANDS+=("deleteall 'soalUjian', '$bulk_id'")
done
execute_hbase_batch "${CLEANUP_COMMANDS[@]}"

echo "✅ EXTENDED CRUD benchmarks completed for scale $SCALE"

# Performance summary
echo ""
echo "📝 EXTENDED CRUD PERFORMANCE SUMMARY (Scale: $SCALE)"
echo "===================================================="
echo ""
echo "PostgreSQL CRUD :"
echo "  - ➕ INSERT: Single record with FK"
echo "  - 🔍 SELECT: With JOIN operations"
echo "  - 🔍 SELECT SELECTIVE: Specific columns only"
echo "  - ✏️ UPDATE: Single record update"
echo "  - 🗑️ DELETE: Single record delete"
echo "  - 🚀 BULK: 10 records individual inserts"
echo ""
echo "HBase CRUD:"
echo "  - ➕ BATCH PUT: Multi-column family (10 operations in 1 session)"
echo "  - 🔍 GET: All column families "
echo "  - 🔍 GET SELECTIVE: Specific columns only"
echo "  - ✏️ BATCH UPDATE: Multiple fields (2 operations in 1 session)"
echo "  - 🗑️ DELETE: Complete row delete "
echo "  - 🚀 BULK: 10 records batch insert test"
echo ""