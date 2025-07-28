#!/bin/bash

# === OPTIMIZED THROUGHPUT PERFORMANCE BENCHMARKS ===

# Enhanced logging with colors and icons
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }
log_header() { echo -e "${PURPLE}📊 $1${NC}"; }
log_subheader() { echo -e "${CYAN}📋 $1${NC}"; }

log_info "Benchmark Started: $(date '+%Y-%m-%d %H:%M:%S UTC')"
log_info "Benchmark User: $USER"
log_info "System: $(uname -s) $(uname -r)"
log_info "Current Directory: $(pwd)"

SCALE=$1
if [ -z "$SCALE" ]; then
    log_warning "Usage: $0 <scale>"
    exit 1
fi

log_header "THROUGHPUT PERFORMANCE BENCHMARKS (SCALE: $SCALE)"

# Environment and config
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
mkdir -p "$(dirname "$RESULTS_FILE")"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

DB_USER="postgres"
DB_NAME="postgres"
DB_HOST="localhost"
DB_PORT="5432"
export PGPASSWORD="mydreamonpsdkulumajang007"

generate_uuid() {
    if command -v uuidgen >/dev/null 2>&1; then
        uuidgen | tr '[:upper:]' '[:lower:]'
    else
        cat /proc/sys/kernel/random/uuid
    fi
}

log_subheader "Retrieving Foreign Key References"
EXISTING_TAKSONOMI=$(psql -qtAX -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT idtaksonomi FROM taksonomi ORDER BY RANDOM() LIMIT 1;" 2>/dev/null)
EXISTING_KONSENTRASI=$(psql -qtAX -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT idkonsentrasisekolah FROM konsentrasikeahliansekolah ORDER BY RANDOM() LIMIT 1;" 2>/dev/null)

# Adaptive batch size
if   [ $SCALE -le 100 ];       then BATCH_SIZE=10
elif [ $SCALE -le 1000 ];      then BATCH_SIZE=50
elif [ $SCALE -le 10000 ];     then BATCH_SIZE=100
elif [ $SCALE -le 100000 ];    then BATCH_SIZE=200
elif [ $SCALE -le 1000000 ];   then BATCH_SIZE=500
else                                BATCH_SIZE=1000
fi
log_info "Using batch size: $BATCH_SIZE for scale $SCALE"

log_subheader "PostgreSQL Throughput Tests"

# Bulk INSERT
echo "  ➕ Bulk INSERT throughput ($BATCH_SIZE records)..."
START_TIME=$(date +%s%3N)
BULK_SQL="BEGIN;"
for i in $(seq 1 $BATCH_SIZE); do
    UUID=$(generate_uuid)
    BULK_SQL="$BULK_SQL INSERT INTO soalujian (idsoalujian, namaujian, pertanyaan, jenissoal, bobot, jawabanbenar, opsi, idtaksonomi, idkonsentrasisekolah, createdby) VALUES ('$UUID', 'Bulk Test $i', 'Bulk question $i for scale $SCALE', 'PG', 5, '[\"A\"]'::jsonb, '{\"A\":\"Option A\"}'::jsonb, '$EXISTING_TAKSONOMI', '$EXISTING_KONSENTRASI', 'ThroughputTester');"
done
BULK_SQL="$BULK_SQL COMMIT;"
echo "$BULK_SQL" | psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME > /dev/null 2>&1
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
if [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk INSERT: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_insert,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_batch_with_fk" >> $RESULTS_FILE

# Bulk UPDATE
echo "  ✏️ Bulk UPDATE throughput..."
START_TIME=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
UPDATE soalujian
SET bobot = bobot + 1,
    pertanyaan = CONCAT(pertanyaan, ' - Updated for throughput test')
WHERE createdby = 'ThroughputTester';" > /dev/null 2>&1
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
if [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk UPDATE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_update,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_bulk_update" >> $RESULTS_FILE

# Bulk SELECT JOIN
echo "  🔗 Bulk SELECT with JOIN throughput..."
START_TIME=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT s.idsoalujian, s.namaujian, s.pertanyaan, t.namataksonomi, k.namakonsentrasi, sch.namasekolah
FROM soalujian s
JOIN taksonomi t ON s.idtaksonomi = t.idtaksonomi
JOIN konsentrasikeahliansekolah k ON s.idkonsentrasisekolah = k.idkonsentrasisekolah
JOIN schools sch ON k.idschool = sch.idschool
WHERE s.createdby = 'ThroughputTester';" > /tmp/bulk_select_result.txt 2>/dev/null
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
if [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk SELECT with JOIN: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_select_join,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_join_query" >> $RESULTS_FILE

# Bulk DELETE
echo "  🗑️ Bulk DELETE throughput..."
START_TIME=$(date +%s%3N)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DELETE FROM soalujian WHERE createdby = 'ThroughputTester';" > /dev/null 2>&1
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
if [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk DELETE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_delete,postgresql,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_bulk_delete" >> $RESULTS_FILE

log_subheader "HBase Throughput Tests"

# Efficient HBase batch execution
execute_hbase_batch() {
    local temp_file=$(mktemp)
    echo "$1" > "$temp_file"
    echo "exit" >> "$temp_file"
    hbase shell "$temp_file" > /dev/null 2>&1
    rm -f "$temp_file"
}

# Bulk PUT
echo "  ➕ Bulk PUT throughput ($BATCH_SIZE records)..."
START_TIME=$(date +%s%3N)
BULK_HBASE=""
for i in $(seq 1 $BATCH_SIZE); do
    UUID=$(generate_uuid)
    BULK_HBASE+="
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
execute_hbase_batch "$BULK_HBASE"
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
if [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((BATCH_SIZE * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk PUT: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_put,hbase,$DURATION,$BATCH_SIZE,$THROUGHPUT,${BATCH_SIZE}_records_multi_cf_put" >> $RESULTS_FILE

# Bulk UPDATE
echo "  ✏️ Bulk UPDATE throughput..."
START_TIME=$(date +%s%3N)
echo "scan 'soalUjian', {COLUMNS => ['detail:createdBy'], FILTER => \"SingleColumnValueFilter('detail', 'createdBy', =, 'binary:ThroughputTester')\"}" | hbase shell > /tmp/hbase_throughput_ids.txt 2>/dev/null
UPDATE_IDS=($(grep -o '[a-f0-9]\{8\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{4\}-[a-f0-9]\{12\}' /tmp/hbase_throughput_ids.txt | head -$BATCH_SIZE))
BULK_UPDATE=""
for id in "${UPDATE_IDS[@]}"; do
    BULK_UPDATE+="
put 'soalUjian', '$id', 'main:bobot', '10'
put 'soalUjian', '$id', 'main:pertanyaan', 'Updated throughput question'"
done
execute_hbase_batch "$BULK_UPDATE"
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
UPDATE_COUNT=${#UPDATE_IDS[@]}
if [ "$DURATION" -gt 0 ] && [ "$UPDATE_COUNT" -gt 0 ]; then
    THROUGHPUT=$((UPDATE_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk UPDATE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_update,hbase,$DURATION,$UPDATE_COUNT,$THROUGHPUT,${UPDATE_COUNT}_records_bulk_update" >> $RESULTS_FILE

# Bulk SCAN
echo "  🔍 Bulk SCAN throughput..."
START_TIME=$(date +%s%3N)
echo "scan 'soalUjian', {COLUMNS => ['main:', 'refs:'], FILTER => \"SingleColumnValueFilter('detail', 'createdBy', =, 'binary:ThroughputTester')\"}" | hbase shell > /tmp/bulk_scan_result.txt 2>/dev/null
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
# Fixed the scan count calculation to handle empty results
SCAN_COUNT=$(grep -c "column=" /tmp/bulk_scan_result.txt 2>/dev/null || echo "0")
# Ensure SCAN_COUNT is a valid integer
if ! [[ "$SCAN_COUNT" =~ ^[0-9]+$ ]]; then
    SCAN_COUNT=0
fi
# Calculate throughput with proper validation
if [ "$SCAN_COUNT" -gt 0 ] && [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((SCAN_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk SCAN: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_scan,hbase,$DURATION,$SCAN_COUNT,$THROUGHPUT,${SCAN_COUNT}_entries_filtered_scan" >> $RESULTS_FILE

# Bulk DELETE
echo "  🗑️ Bulk DELETE throughput..."
START_TIME=$(date +%s%3N)
DELETE_COMMANDS=""
for id in "${UPDATE_IDS[@]}"; do
    DELETE_COMMANDS+="
deleteall 'soalUjian', '$id'"
done
execute_hbase_batch "$DELETE_COMMANDS"
END_TIME=$(date +%s%3N)
DURATION=$((END_TIME - START_TIME))
DELETE_COUNT=${#UPDATE_IDS[@]}
if [ "$DURATION" -gt 0 ] && [ "$DELETE_COUNT" -gt 0 ]; then
    THROUGHPUT=$((DELETE_COUNT * 1000 / DURATION))
else
    THROUGHPUT=0
fi
log_success "Bulk DELETE: ${DURATION}ms, Throughput: ${THROUGHPUT} rps"
echo "$TIMESTAMP,$SCALE,5,bulk_delete,hbase,$DURATION,$DELETE_COUNT,$THROUGHPUT,${DELETE_COUNT}_records_bulk_delete" >> $RESULTS_FILE
