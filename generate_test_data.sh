#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== OPTIMIZED GENERATING TEST DATA FOR SCALE: $SCALE ==="

# Database configurations
DB_USER="postgres"
DB_NAME="postgres"
DB_HOST="localhost"
DB_PORT="5432"
export PGPASSWORD="mydreamonpsdkulumajang007"

# Use environment variables with fallbacks
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
DATA_DIR="${BASE_DIR}/test-data-scaled"

# Ensure results file and directory exist
mkdir -p "$(dirname "$RESULTS_FILE")"
mkdir -p "$DATA_DIR"

if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

# Change to data directory
cd "$DATA_DIR" || exit 1

# Function to generate random UUID (optimized)
generate_uuid() {
    if command -v uuidgen >/dev/null 2>&1; then
        uuidgen | tr '[:upper:]' '[:lower:]'
    else
        cat /proc/sys/kernel/random/uuid
    fi
}

# Function to generate random question (optimized with array)
QUESTIONS=(
    "Apa yang dimaksud dengan algoritma sorting dalam pemrograman?"
    "Bagaimana cara mengoptimalkan query database dengan indexing?"
    "Jelaskan perbedaan antara NoSQL dan SQL database?"
    "Apa keuntungan menggunakan cloud computing untuk scalability?"
    "Bagaimana implementasi RESTful API yang baik dan secure?"
    "Jelaskan konsep microservices architecture dan benefitnya?"
    "Apa perbedaan antara Docker container dan Virtual Machine?"
    "Bagaimana cara menerapkan security best practices dalam development?"
    "Jelaskan konsep Big Data dan teknik processing yang digunakan?"
    "Apa yang dimaksud dengan machine learning dan deep learning?"
    "Bagaimana cara implementasi continuous integration dan deployment?"
    "Jelaskan konsep design patterns dalam software engineering?"
    "Apa pentingnya version control system dalam development?"
    "Bagaimana cara optimasi performance aplikasi web?"
    "Jelaskan konsep distributed systems dan scalability?"
    "Apa yang dimaksud dengan agile methodology dalam project management?"
    "Bagaimana implementasi security authentication dan authorization?"
    "Jelaskan konsep data structures dan algoritma kompleksitas?"
    "Apa keuntungan menggunakan microservices versus monolithic?"
    "Bagaimana cara testing automation dalam software development?"
)

generate_question() {
    echo "${QUESTIONS[$((RANDOM % ${#QUESTIONS[@]}))]}"
}

# Function to generate timestamp (optimized)
generate_timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"
}

# Pre-generate reference data (optimized)
echo "📝 Generating reference data with optimizations..."

# Generate schools data efficiently
SCHOOL_IDS=()
{
    for i in {1..10}; do
        SCHOOL_ID=$(generate_uuid)
        SCHOOL_IDS+=("$SCHOOL_ID")
        printf "%s\t%s\t%s\t%s\n" \
            "$SCHOOL_ID" \
            "Sekolah Test $i" \
            "Alamat Sekolah $i" \
            "$(generate_timestamp)"
    done
} > "schools_${SCALE}.csv"

# Generate konsentrasi data efficiently
KONSENTRASI_IDS=()
{
    for i in {1..20}; do
        KONSENTRASI_ID=$(generate_uuid)
        KONSENTRASI_IDS+=("$KONSENTRASI_ID")
        RANDOM_SCHOOL="${SCHOOL_IDS[$((RANDOM % ${#SCHOOL_IDS[@]}))]}"
        printf "%s\t%s\t%s\t%s\n" \
            "$KONSENTRASI_ID" \
            "Konsentrasi Keahlian $i" \
            "$RANDOM_SCHOOL" \
            "$(generate_timestamp)"
    done
} > "konsentrasi_${SCALE}.csv"

# Generate taksonomi data efficiently
TAKSONOMI_IDS=()
{
    for i in {1..15}; do
        TAKSONOMI_ID=$(generate_uuid)
        TAKSONOMI_IDS+=("$TAKSONOMI_ID")
        printf "%s\t%s\t%d\t%s\t%s\n" \
            "$TAKSONOMI_ID" \
            "Taksonomi Level $i" \
            "$((i % 6 + 1))" \
            "" \
            "$(generate_timestamp)"
    done
} > "taksonomi_${SCALE}.csv"

echo "   ✅ Reference data generated: 10 schools, 20 konsentrasi, 15 taksonomi"

# Pre-generate common options and answers (optimization)
OPTIONS_TEMPLATE='{"A":"Option A - Pilihan pertama","B":"Option B - Pilihan kedua","C":"Option C - Pilihan ketiga","D":"Option D - Pilihan keempat","E":"Option E - Pilihan kelima"}'
ANSWER_TEMPLATE='["A"]'

echo "📝 Generating $SCALE soalujian records with optimizations..."

# PHASE 1: Optimized PostgreSQL Data Generation
echo "🚀 Generating PostgreSQL data with batch processing..."
START_TIME=$(date +%s%3N)

# Calculate batch size for optimal performance
BATCH_SIZE=1000
if [ "$SCALE" -gt 100000 ]; then
    BATCH_SIZE=5000
elif [ "$SCALE" -gt 10000 ]; then
    BATCH_SIZE=2000
fi

# Generate PostgreSQL data in batches
{
    BATCH_COUNT=0
    for ((i=1; i<=SCALE; i++)); do
        SOAL_ID=$(generate_uuid)
        QUESTION=$(generate_question)
        RANDOM_TAKSONOMI="${TAKSONOMI_IDS[$((RANDOM % ${#TAKSONOMI_IDS[@]}))]}"
        RANDOM_KONSENTRASI="${KONSENTRASI_IDS[$((RANDOM % ${#KONSENTRASI_IDS[@]}))]}"
        BOBOT=$((RANDOM % 10 + 1))
        CREATED_AT=$(generate_timestamp)

        # Optimize string escaping
        QUESTION_ESCAPED="${QUESTION//	/    }"
        QUESTION_ESCAPED="${QUESTION_ESCAPED//\'/\\\'}"

        printf "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n" \
            "$SOAL_ID" \
            "Test Soal $i" \
            "$QUESTION_ESCAPED" \
            "PG" \
            "$BOBOT" \
            "$ANSWER_TEMPLATE" \
            "$OPTIONS_TEMPLATE" \
            "$RANDOM_TAKSONOMI" \
            "$RANDOM_KONSENTRASI" \
            "$CREATED_AT" \
            "BramMahendrawan"

        # Optimized progress reporting
        if [ $((i % BATCH_SIZE)) -eq 0 ]; then
            BATCH_COUNT=$((BATCH_COUNT + 1))
            PROGRESS=$((i * 100 / SCALE))
            echo "  📊 PostgreSQL: Batch $BATCH_COUNT completed - $i/$SCALE records (${PROGRESS}%)" >&2
        fi
    done
} > "soal_psql_${SCALE}.csv"

END_TIME=$(date +%s%3N)
PSQL_GEN_DURATION=$((END_TIME - START_TIME))
PSQL_THROUGHPUT=$((SCALE * 1000 / PSQL_GEN_DURATION))

echo "$TIMESTAMP,$SCALE,1,data_generation_optimized,postgresql,$PSQL_GEN_DURATION,$SCALE,$PSQL_THROUGHPUT,optimized_csv_with_batches" >> "$RESULTS_FILE"
echo "   ✅ PostgreSQL data generation: ${PSQL_GEN_DURATION}ms ($PSQL_THROUGHPUT rps)"

# PHASE 1: Optimized HBase Data Generation
echo "🚀 Generating HBase data with optimized commands..."
START_TIME=$(date +%s%3N)

# Generate HBase data with optimized batch writing
{
    BATCH_COUNT=0
    for ((i=1; i<=SCALE; i++)); do
        SOAL_ID=$(generate_uuid)
        QUESTION=$(generate_question)
        RANDOM_TAKSONOMI="${TAKSONOMI_IDS[$((RANDOM % ${#TAKSONOMI_IDS[@]}))]}"
        RANDOM_KONSENTRASI="${KONSENTRASI_IDS[$((RANDOM % ${#KONSENTRASI_IDS[@]}))]}"
        BOBOT=$((RANDOM % 10 + 1))
        CREATED_AT=$(generate_timestamp)

        # Optimize string escaping for HBase
        QUESTION_ESCAPED="${QUESTION//\'/\\\'}"
        QUESTION_ESCAPED="${QUESTION_ESCAPED//\"/\\\"}"
        OPTIONS_ESCAPED="${OPTIONS_TEMPLATE//\"/\\\"}"
        ANSWER_ESCAPED="${ANSWER_TEMPLATE//\"/\\\"}"

        # Generate optimized HBase commands (more compact)
        cat << EOF
put 'soalUjian', '$SOAL_ID', 'main:idSoalUjian', '$SOAL_ID'
put 'soalUjian', '$SOAL_ID', 'main:namaUjian', 'Test Soal $i'
put 'soalUjian', '$SOAL_ID', 'main:pertanyaan', '$QUESTION_ESCAPED'
put 'soalUjian', '$SOAL_ID', 'main:jenisSoal', 'PG'
put 'soalUjian', '$SOAL_ID', 'main:bobot', '$BOBOT'
put 'soalUjian', '$SOAL_ID', 'main:jawabanBenar', '$ANSWER_ESCAPED'
put 'soalUjian', '$SOAL_ID', 'main:opsi', '$OPTIONS_ESCAPED'
put 'soalUjian', '$SOAL_ID', 'refs:idTaksonomi', '$RANDOM_TAKSONOMI'
put 'soalUjian', '$SOAL_ID', 'refs:idKonsentrasiSekolah', '$RANDOM_KONSENTRASI'
put 'soalUjian', '$SOAL_ID', 'detail:createdAt', '$CREATED_AT'
put 'soalUjian', '$SOAL_ID', 'detail:createdBy', 'BramMahendrawan'

EOF

        # Optimized progress reporting
        if [ $((i % BATCH_SIZE)) -eq 0 ]; then
            BATCH_COUNT=$((BATCH_COUNT + 1))
            PROGRESS=$((i * 100 / SCALE))
            echo "  📊 HBase: Batch $BATCH_COUNT completed - $i/$SCALE records (${PROGRESS}%)" >&2
        fi
    done
} > "soal_hbase_${SCALE}.hbase"

END_TIME=$(date +%s%3N)
HBASE_GEN_DURATION=$((END_TIME - START_TIME))
HBASE_THROUGHPUT=$((SCALE * 1000 / HBASE_GEN_DURATION))

echo "$TIMESTAMP,$SCALE,1,data_generation_optimized,hbase,$HBASE_GEN_DURATION,$SCALE,$HBASE_THROUGHPUT,optimized_hbase_script_with_batches" >> "$RESULTS_FILE"
echo "   ✅ HBase data generation: ${HBASE_GEN_DURATION}ms ($HBASE_THROUGHPUT rps)"

# Optimization summary and file size reporting
echo ""
echo "📊 OPTIMIZED DATA GENERATION SUMMARY"
echo "===================================="
echo "🎯 Scale: $SCALE records"
echo "📈 Performance Comparison:"
echo "   PostgreSQL: ${PSQL_GEN_DURATION}ms (${PSQL_THROUGHPUT} rps)"
echo "   HBase:      ${HBASE_GEN_DURATION}ms (${HBASE_THROUGHPUT} rps)"

# Calculate and display file sizes
PSQL_SIZE=$(du -h "soal_psql_${SCALE}.csv" | cut -f1)
HBASE_SIZE=$(du -h "soal_hbase_${SCALE}.hbase" | cut -f1)
SCHOOLS_SIZE=$(du -h "schools_${SCALE}.csv" | cut -f1)
KONSENTRASI_SIZE=$(du -h "konsentrasi_${SCALE}.csv" | cut -f1)
TAKSONOMI_SIZE=$(du -h "taksonomi_${SCALE}.csv" | cut -f1)

echo ""
echo "💾 Generated File Sizes:"
echo "   📄 PostgreSQL data: $PSQL_SIZE (soal_psql_${SCALE}.csv)"
echo "   📄 HBase data: $HBASE_SIZE (soal_hbase_${SCALE}.hbase)"
echo "   📄 Schools: $SCHOOLS_SIZE (schools_${SCALE}.csv)"
echo "   📄 Konsentrasi: $KONSENTRASI_SIZE (konsentrasi_${SCALE}.csv)"
echo "   📄 Taksonomi: $TAKSONOMI_SIZE (taksonomi_${SCALE}.csv)"



# Performance improvement calculation
if [ "$HBASE_GEN_DURATION" -gt "$PSQL_GEN_DURATION" ]; then
    DIFF_PERCENT=$(( (HBASE_GEN_DURATION - PSQL_GEN_DURATION) * 100 / HBASE_GEN_DURATION ))
    echo "   📊 PostgreSQL generation is ${DIFF_PERCENT}% faster than HBase"
else
    DIFF_PERCENT=$(( (PSQL_GEN_DURATION - HBASE_GEN_DURATION) * 100 / PSQL_GEN_DURATION ))
    echo "   📊 HBase generation is ${DIFF_PERCENT}% faster than PostgreSQL"
fi

echo ""
echo "✅ OPTIMIZED test data for scale $SCALE generated successfully!"

# Return to original directory
cd "$BASE_DIR"