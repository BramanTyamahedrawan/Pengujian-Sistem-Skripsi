#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== THROUGHPUT PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

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

# Get existing foreign key values
EXISTING_TAKSONOMI=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idTaksonomi FROM taksonomi ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')
EXISTING_KONSENTRASI=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idKonsentrasiSekolah FROM konsentrasiKeahlianSekolah ORDER BY RANDOM() LIMIT 1;" >

# PHASE 5: Throughput Testing
echo "📈 PHASE 5: Throughput Testing"

# Determine batch size based on scale
if [ $SCALE -le 100 ]; then
    BATCH_SIZE=10
elif [ $SCALE -le 1000 ]; then
    BATCH_SIZE=50
elif [ $SCALE -le 10000 ]; then
    BATCH_SIZE=100
else
    BATCH_SIZE=200
fi

echo "Using batch size: $BATCH_SIZE for scale $SCALE"

echo "🔍 PostgreSQL Throughput Tests:"

# Test 1: Bulk INSERT throughput
echo "  Bulk INSERT throughput ($BATCH_SIZE records)..."
START_TIME=$(date +%s%3N)

BULK_SQL="BEGIN;"
for i in $(seq 1 $BATCH_SIZE); do
    UUID=$(generate_uuid)
    BULK_SQL="$BULK_SQL INSERT INTO soalUjian (idSoalUjian, namaUjian, pertanyaan, jenisSoal, bobot, jawabanBenar, opsi, idTaksonomi, idKonsentrasiSekolah, createdBy) VALU>
done
BULK_SQL="$BULK_SQL COMMIT;"

echo "$BULK_SQL" | psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_insert,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_batch_with_fk" >> $RESULTS_FILE
echo "    Bulk INSERT: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Test 2: Bulk UPDATE throughput
echo "  Bulk UPDATE throughput..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
UPDATE soalUjian
SET bobot = bobot + 1,
    pertanyaan = CONCAT(pertanyaan, ' - Updated for throughput test')
WHERE createdBy = 'ThroughputTester';" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_update,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_bulk_update" >> $RESULTS_FILE
echo "    Bulk UPDATE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Test 3: Bulk SELECT with JOIN throughput
echo "  Bulk SELECT with JOIN throughput..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT s.idSoalUjian, s.namaUjian, s.pertanyaan, t.namaTaksonomi, k.namaKonsentrasi, sch.namaSekolah
FROM soalUjian s
JOIN taksonomi t ON s.idTaksonomi = t.idTaksonomi
JOIN konsentrasiKeahlianSekolah k ON s.idKonsentrasiSekolah = k.idKonsentrasiSekolah
JOIN schools sch ON k.idSchool = sch.idSchool
WHERE s.createdBy = 'ThroughputTester';" > /tmp/bulk_select_result.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_select_join,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_join_query" >> $RESULTS_FILE
echo "    Bulk SELECT with JOIN: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Test 4: Bulk DELETE throughput
echo "  Bulk DELETE throughput..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DELETE FROM soalUjian WHERE createdBy = 'ThroughputTester';" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_delete,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_bulk_delete" >> $RESULTS_FILE
echo "    Bulk DELETE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

echo "🔍 HBase Throughput Tests:"

# Test 1: Bulk PUT throughput
echo "  Bulk PUT throughput ($BATCH_SIZE records)..."
START_TIME=$(date +%s%3N)

BULK_HBASE=""
for i in $(seq 1 $BATCH_SIZE); do
    UUID=$(generate_uuid)
    BULK_HBASE="$BULK_HBASE
put 'soalUjian', '$UUID', 'main:idSoalUjian', '$UUID'
put 'soalUjian', '$UUID', 'main:namaUjian', 'Throughput Test $i'
put 'soalUjian', '$UUID', 'main:pertanyaan', 'Throughput test question $i for scale $SCALE'
put 'soalUjian', '$UUID', 'main:jenisSoal', 'PG'
put 'soalUjian', '$UUID', 'main:bobot', '$((i % 10 + 1))'
put 'soalUjian', '$UUID', 'main:jawabanBenar', '[\"A\"]'
put 'soalUjian', '$UUID', 'main:opsi', '{\"A\":\"Option A\"}'
put 'soalUjian', '$UUID', 'refs:idTaksonomi', '$EXISTING_TAKSONOMI'
put 'soalUjian', '$UUID', 'refs:idKonsentrasiSekolah', '$EXISTING_KONSENTRASI'
put 'soalUjian', '$UUID', 'detail:createdBy', 'ThroughputTester'"
done

echo "$BULK_HBASE" | hbase shell > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_put,hbase,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_multi_cf_put" >> $RESULTS_FILE
echo "    Bulk PUT: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Test 2: Bulk UPDATE (PUT with new values) throughput
echo "  Bulk UPDATE throughput..."
START_TIME=$(date +%s%3N)

# Get IDs of records we just inserted
echo "scan 'soalUjian', {COLUMNS => ['detail:createdBy'], FILTER => \"SingleColumnValueFilter('detail', 'createdBy', =, 'binary:ThroughputTester')\"}" | hbase shell 2>/dev>
UPDATE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_throughput_ids.txt | head -$BATCH_SIZE))

BULK_UPDATE=""
for id in "${UPDATE_IDS[@]}"; do
    BULK_UPDATE="$BULK_UPDATE
put 'soalUjian', '$id', 'main:bobot', '10'
put 'soalUjian', '$id', 'main:pertanyaan', 'Updated throughput question'"
done

echo "$BULK_UPDATE" | hbase shell > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
UPDATE_COUNT=${#UPDATE_IDS[@]}
THROUGHPUT=$((UPDATE_COUNT * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_update,hbase,$DURATION,$UPDATE_COUNT,$THROUGHPUT,${UPDATE_COUNT}_records_bulk_update" >> $RESULTS_FILE
echo "    Bulk UPDATE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Test 3: Bulk SCAN throughput
echo "  Bulk SCAN throughput..."
START_TIME=$(date +%s%3N)

echo "scan 'soalUjian', {COLUMNS => ['main:', 'refs:'], FILTER => \"SingleColumnValueFilter('detail', 'createdBy', =, 'binary:ThroughputTester')\"}" | hbase shell > /tmp/b>

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
SCAN_COUNT=$(grep -c "column=" /tmp/bulk_scan_result.txt || echo "0")
THROUGHPUT=$((SCAN_COUNT * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_scan,hbase,$DURATION,$SCAN_COUNT,$THROUGHPUT,${SCAN_COUNT}_entries_filtered_scan" >> $RESULTS_FILE
echo "    Bulk SCAN: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

# Test 4: Bulk DELETE throughput
echo "  Bulk DELETE throughput..."
START_TIME=$(date +%s%3N)

DELETE_COMMANDS=""
for id in "${UPDATE_IDS[@]}"; do
    DELETE_COMMANDS="$DELETE_COMMANDS
deleteall 'soalUjian', '$id'"
done

echo "$DELETE_COMMANDS" | hbase shell > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
DELETE_COUNT=${#UPDATE_IDS[@]}
THROUGHPUT=$((DELETE_COUNT * 1000 / DURATION))
echo "$TIMESTAMP,$SCALE,5,bulk_delete,hbase,$DURATION,$DELETE_COUNT,$THROUGHPUT,${DELETE_COUNT}_records_bulk_delete" >> $RESULTS_FILE
echo "    Bulk DELETE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"

echo "✅ Throughput benchmarks completed for scale $SCALE"

