  GNU nano 7.2                                                              benchmark_crud_scaled.sh
#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== CRUD PERFORMANCE BENCHMARKS (SCALE: $SCALE) ==="

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

# PHASE 3: CRUD Operations
echo "📝 PHASE 3: CRUD Operations"

# Get existing foreign key values for PostgreSQL
EXISTING_TAKSONOMI=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idTaksonomi FROM taksonomi ORDER BY RANDOM() LIMIT 1;" 2>/dev/null | tr -d ' ')
EXISTING_KONSENTRASI=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT idKonsentrasiSekolah FROM konsentrasiKeahlianSekolah ORDER BY RANDOM() LIMIT 1;" >

echo "🔍 PostgreSQL CRUD Tests:"

# CREATE (INSERT) - PostgreSQL
echo "  CREATE (INSERT) test..."
TEST_ID=$(generate_uuid)
TEST_QUESTION="Test CRUD question for scale $SCALE"
START_TIME=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
INSERT INTO soalUjian (
    idSoalUjian, namaUjian, pertanyaan, jenisSoal, bobot,
    jawabanBenar, opsi, idTaksonomi, idKonsentrasiSekolah, createdBy
) VALUES (
    '$TEST_ID', 'CRUD Test $SCALE', '$TEST_QUESTION', 'PG', 5,
    '[\"A\"]'::jsonb, '{\"A\":\"Option A\"}'::jsonb,
    '$EXISTING_TAKSONOMI', '$EXISTING_KONSENTRASI', 'CRUDTester'
);" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_insert,postgresql,$DURATION,1,$THROUGHPUT,single_record_insert_with_fk" >> $RESULTS_FILE
echo "    INSERT Duration: ${DURATION}ms"

# READ (SELECT) - PostgreSQL
echo "  READ (SELECT) test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT s.*, t.namaTaksonomi, k.namaKonsentrasi
FROM soalUjian s
JOIN taksonomi t ON s.idTaksonomi = t.idTaksonomi
JOIN konsentrasiKeahlianSekolah k ON s.idKonsentrasiSekolah = k.idKonsentrasiSekolah
WHERE s.idSoalUjian = '$TEST_ID';" > /tmp/psql_crud_read.txt 2>/dev/null

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_select_join,postgresql,$DURATION,1,$THROUGHPUT,single_record_select_with_join" >> $RESULTS_FILE
echo "    SELECT with JOIN Duration: ${DURATION}ms"

# UPDATE - PostgreSQL
echo "  UPDATE test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
UPDATE soalUjian
SET pertanyaan = 'Updated: $TEST_QUESTION', bobot = 10
WHERE idSoalUjian = '$TEST_ID';" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_update,postgresql,$DURATION,1,$THROUGHPUT,single_record_update" >> $RESULTS_FILE
echo "    UPDATE Duration: ${DURATION}ms"

# DELETE - PostgreSQL
echo "  DELETE test..."
START_TIME=$(date +%s%3N)

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DELETE FROM soalUjian WHERE idSoalUjian = '$TEST_ID';" > /dev/null 2>&1

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_delete,postgresql,$DURATION,1,$THROUGHPUT,single_record_delete" >> $RESULTS_FILE
echo "    DELETE Duration: ${DURATION}ms"

echo "🔍 HBase CRUD Tests:"

# CREATE (PUT) - HBase
echo "  CREATE (PUT) test..."
TEST_ID=$(generate_uuid)
TEST_QUESTION="Test CRUD question for scale $SCALE"
START_TIME=$(date +%s%3N)

hbase shell << EOF > /dev/null 2>&1
put 'soalUjian', '$TEST_ID', 'main:idSoalUjian', '$TEST_ID'
put 'soalUjian', '$TEST_ID', 'main:namaUjian', 'CRUD Test $SCALE'
put 'soalUjian', '$TEST_ID', 'main:pertanyaan', '$TEST_QUESTION'
put 'soalUjian', '$TEST_ID', 'main:jenisSoal', 'PG'
put 'soalUjian', '$TEST_ID', 'main:bobot', '5'
put 'soalUjian', '$TEST_ID', 'main:jawabanBenar', '[\"A\"]'
put 'soalUjian', '$TEST_ID', 'main:opsi', '{\"A\":\"Option A\"}'
put 'soalUjian', '$TEST_ID', 'refs:idTaksonomi', '$EXISTING_TAKSONOMI'
put 'soalUjian', '$TEST_ID', 'refs:idKonsentrasiSekolah', '$EXISTING_KONSENTRASI'
put 'soalUjian', '$TEST_ID', 'detail:createdBy', 'CRUDTester'
exit
EOF

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_put,hbase,$DURATION,1,$THROUGHPUT,single_record_put_multi_cf" >> $RESULTS_FILE
echo "    PUT Duration: ${DURATION}ms"

# READ (GET) - HBase
echo "  READ (GET) test..."
START_TIME=$(date +%s%3N)

hbase shell << EOF > /tmp/hbase_crud_read.txt 2>/dev/null
get 'soalUjian', '$TEST_ID'
exit
EOF

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_get,hbase,$DURATION,1,$THROUGHPUT,single_record_get_all_cf" >> $RESULTS_FILE
echo "    GET Duration: ${DURATION}ms"

# UPDATE (PUT with new values) - HBase
echo "  UPDATE test..."
START_TIME=$(date +%s%3N)

hbase shell << EOF > /dev/null 2>&1
put 'soalUjian', '$TEST_ID', 'main:pertanyaan', 'Updated: $TEST_QUESTION'
put 'soalUjian', '$TEST_ID', 'main:bobot', '10'
exit
EOF

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_put_update,hbase,$DURATION,1,$THROUGHPUT,single_record_update_cf" >> $RESULTS_FILE
echo "    UPDATE Duration: ${DURATION}ms"

# DELETE - HBase
echo "  DELETE test..."
START_TIME=$(date +%s%3N)

hbase shell << EOF > /dev/null 2>&1
deleteall 'soalUjian', '$TEST_ID'
exit
EOF

END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
THROUGHPUT=$((1000 / DURATION))
echo "$TIMESTAMP,$SCALE,3,crud_delete,hbase,$DURATION,1,$THROUGHPUT,single_record_delete_all_cf" >> $RESULTS_FILE
echo "    DELETE Duration: ${DURATION}ms"

echo "✅ CRUD benchmarks completed for scale $SCALE"

