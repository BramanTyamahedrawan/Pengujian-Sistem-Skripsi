#!/bin/bash

echo "=== SETUP SCALED BENCHMARK ENVIRONMENT ==="

# Create benchmark directory
mkdir -p benchmark-results test-data-scaled logs
cd test-data-scaled

# Database configurations
DB_USER="postgres"
DB_NAME="postgres"
DB_HOST="localhost"
DB_PORT="5432"
export PGPASSWORD="mydreamonpsdkulumajang007"

# HBase configuration
HBASE_TABLE="soalUjian"

# Create benchmark results files
# Set RESULTS_FILE variable properly
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"

# Create benchmark results files
mkdir -p "$(dirname "$RESULTS_FILE")"
echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"

# Setup PostgreSQL schema (dengan struktur yang benar seperti script asli)
echo "🔧 Setting up PostgreSQL schema..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOSQL'
-- Drop existing tables
DROP TABLE IF EXISTS soalujian CASCADE;
DROP TABLE IF EXISTS taksonomi CASCADE;
DROP TABLE IF EXISTS konsentrasikeahliansekolah CASCADE;
DROP TABLE IF EXISTS schools CASCADE;

CREATE TABLE schools (
    idschool VARCHAR(255) PRIMARY KEY,
    namasekolah TEXT NOT NULL,
    alamat TEXT,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE konsentrasikeahliansekolah (
    idkonsentrasisekolah VARCHAR(255) PRIMARY KEY,
    namakonsentrasi TEXT NOT NULL,
    idschool VARCHAR(255) REFERENCES schools(idschool),
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE taksonomi (
    idtaksonomi VARCHAR(255) PRIMARY KEY,
    namataksonomi TEXT NOT NULL,
    leveltaksonomi INTEGER DEFAULT 1,
    parentid VARCHAR(255),
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE soalujian (
    idsoalujian VARCHAR(255) PRIMARY KEY,
    namaujian TEXT,
    pertanyaan TEXT,
    jenissoal VARCHAR(50) DEFAULT 'PG',
    bobot INTEGER DEFAULT 1,
    jawabanbenar JSONB,
    opsi JSONB,
    idtaksonomi VARCHAR(255) REFERENCES taksonomi(idtaksonomi),
    idkonsentrasisekolah VARCHAR(255) REFERENCES konsentrasikeahliansekolah(idkonsentrasisekolah),
    createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    createdby TEXT DEFAULT 'BramMahendrawan'
);

-- Create proper indexes
CREATE INDEX idx_soal_taksonomi ON soalujian(idtaksonomi);
CREATE INDEX idx_soal_konsentrasi ON soalujian(idkonsentrasisekolah);
CREATE INDEX idx_soal_jenis ON soalujian(jenissoal);
CREATE INDEX idx_soal_bobot ON soalujian(bobot);
CREATE INDEX idx_konsentrasi_school ON konsentrasikeahliansekolah(idschool);
EOSQL

# Setup HBase schema (dengan struktur yang benar)
echo "🔧 Setting up HBase schema..."
hbase shell << 'EOHBASE'
disable 'soalUjian'
drop 'soalUjian'
create 'soalUjian',
  {NAME => 'main', VERSIONS => 1, COMPRESSION => 'NONE'},
  {NAME => 'detail', VERSIONS => 1, COMPRESSION => 'NONE'},
  {NAME => 'refs', VERSIONS => 1, COMPRESSION => 'NONE'}
exit
EOHBASE

echo "✅ Scaled benchmark environment ready"
echo "📊 Schema: PostgreSQL with proper foreign keys, HBase with column families"

