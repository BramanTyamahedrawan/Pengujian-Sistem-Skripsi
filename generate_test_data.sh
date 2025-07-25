#!/bin/bash

SCALE=$1
if [ -z "$SCALE" ]; then
    echo "Usage: $0 <scale>"
    exit 1
fi

echo "=== GENERATING TEST DATA FOR SCALE: $SCALE ==="

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

# Ensure results file and directory exist
mkdir -p "$(dirname "$RESULTS_FILE")"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

# Function to generate random UUID
generate_uuid() {
    cat /proc/sys/kernel/random/uuid
}

# Function to generate random question
generate_question() {
    local questions=(
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
    )
    echo "${questions[$((RANDOM % ${#questions[@]}))]}"
}

# Generate reference data first (smaller scale)
echo "📝 Generating reference data..."

# Generate schools (10 records)
SCHOOL_IDS=()
> "schools_${SCALE}.csv"
for i in {1..10}; do
    SCHOOL_ID=$(generate_uuid)
    SCHOOL_IDS+=("$SCHOOL_ID")
    printf "%s\t%s\t%s\t%s\n" \
        "$SCHOOL_ID" \
        "Sekolah Test $i" \
        "Alamat Sekolah $i" \
        "$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")" >> "schools_${SCALE}.csv"
done

# Generate konsentrasi (20 records)
KONSENTRASI_IDS=()
> "konsentrasi_${SCALE}.csv"
for i in {1..20}; do
    KONSENTRASI_ID=$(generate_uuid)
    KONSENTRASI_IDS+=("$KONSENTRASI_ID")
    RANDOM_SCHOOL="${SCHOOL_IDS[$((RANDOM % ${#SCHOOL_IDS[@]}))]}"
    printf "%s\t%s\t%s\t%s\n" \
        "$KONSENTRASI_ID" \
        "Konsentrasi Keahlian $i" \
        "$RANDOM_SCHOOL" \
        "$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")" >> "konsentrasi_${SCALE}.csv"
done

# Generate taksonomi (15 records)
TAKSONOMI_IDS=()
> "taksonomi_${SCALE}.csv"
for i in {1..15}; do
    TAKSONOMI_ID=$(generate_uuid)
    TAKSONOMI_IDS+=("$TAKSONOMI_ID")
    printf "%s\t%s\t%d\t%s\t%s\n" \
        "$TAKSONOMI_ID" \
        "Taksonomi Level $i" \
        "$((i % 6 + 1))" \
        "" \
        "$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")" >> "taksonomi_${SCALE}.csv"
done

echo "📝 Generating $SCALE soalujian records..."

# PHASE 1: Data Generation Performance Test
START_TIME=$(date +%s%3N)

# Generate PostgreSQL soalujian data
> "soal_psql_${SCALE}.csv"
for ((i=1; i<=SCALE; i++)); do
    SOAL_ID=$(generate_uuid)
    QUESTION=$(generate_question)
    OPTIONS='{"A":"Option A - Pilihan pertama","B":"Option B - Pilihan kedua","C":"Option C - Pilihan ketiga","D":"Option D - Pilihan keempat","E":"Option E - Pilihan kelima"}'
    ANSWER='["A"]'
    RANDOM_TAKSONOMI="${TAKSONOMI_IDS[$((RANDOM % ${#TAKSONOMI_IDS[@]}))]}"
    RANDOM_KONSENTRASI="${KONSENTRASI_IDS[$((RANDOM % ${#KONSENTRASI_IDS[@]}))]}"
    BOBOT=$((RANDOM % 10 + 1))
    CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")

    # Escape quotes and tabs
    QUESTION_ESCAPED=$(echo "$QUESTION" | sed 's/\t/    /g' | sed "s/'/\\'/g")

    printf "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$SOAL_ID" \
        "Test Soal $i" \
        "$QUESTION_ESCAPED" \
        "PG" \
        "$BOBOT" \
        "$ANSWER" \
        "$OPTIONS" \
        "$RANDOM_TAKSONOMI" \
        "$RANDOM_KONSENTRASI" \
        "$CREATED_AT" \
        "BramMahendrawan" >> "soal_psql_${SCALE}.csv"

    # Progress indicator
    if [ $((i % 1000)) -eq 0 ]; then
        echo "  Generated $i/$SCALE records..."
    fi
done

END_TIME=$(date +%s%3N)
PSQL_GEN_DURATION=$((END_TIME - START_TIME))
PSQL_THROUGHPUT=$((SCALE * 1000 / PSQL_GEN_DURATION))

echo "$TIMESTAMP,$SCALE,1,data_generation,postgresql,$PSQL_GEN_DURATION,$SCALE,$PSQL_THROUGHPUT,csv_with_foreign_keys" >> $RESULTS_FILE
echo "✅ PostgreSQL data generation: ${PSQL_GEN_DURATION}ms ($PSQL_THROUGHPUT rps)"

# Generate HBase soalUjian data
START_TIME=$(date +%s%3N)

> "soal_hbase_${SCALE}.hbase"
for ((i=1; i<=SCALE; i++)); do
    SOAL_ID=$(generate_uuid)
    QUESTION=$(generate_question)
    OPTIONS='{"A":"Option A - Pilihan pertama","B":"Option B - Pilihan kedua","C":"Option C - Pilihan ketiga","D":"Option D - Pilihan keempat","E":"Option E - Pilihan kelima"}'
    ANSWER='["A"]'
    RANDOM_TAKSONOMI="${TAKSONOMI_IDS[$((RANDOM % ${#TAKSONOMI_IDS[@]}))]}"
    RANDOM_KONSENTRASI="${KONSENTRASI_IDS[$((RANDOM % ${#KONSENTRASI_IDS[@]}))]}"
    BOBOT=$((RANDOM % 10 + 1))
    CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")

    # Escape quotes for HBase
    QUESTION_ESCAPED=$(echo "$QUESTION" | sed "s/'/\\'/g" | sed 's/"/\\"/g')
    OPTIONS_ESCAPED=$(echo "$OPTIONS" | sed 's/"/\\"/g')
    ANSWER_ESCAPED=$(echo "$ANSWER" | sed 's/"/\\"/g')

    # HBase structure like your original script
    cat >> "soal_hbase_${SCALE}.hbase" << EOF
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
    # Progress indicator
    if [ $((i % 1000)) -eq 0 ]; then
        echo "  Generated $i/$SCALE HBase commands..."
    fi
done

END_TIME=$(date +%s%3N)
HBASE_GEN_DURATION=$((END_TIME - START_TIME))
HBASE_THROUGHPUT=$((SCALE * 1000 / HBASE_GEN_DURATION))

echo "$TIMESTAMP,$SCALE,1,data_generation,hbase,$HBASE_GEN_DURATION,$SCALE,$HBASE_THROUGHPUT,hbase_script_with_refs" >> $RESULTS_FILE
echo "✅ HBase data generation: ${HBASE_GEN_DURATION}ms ($HBASE_THROUGHPUT rps)"

echo "✅ Test data for scale $SCALE generated successfully!"
echo "📊 PostgreSQL: ${PSQL_GEN_DURATION}ms, HBase: ${HBASE_GEN_DURATION}ms"