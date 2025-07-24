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
echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"

# Setup PostgreSQL schema (dengan struktur yang benar seperti script asli)
echo "🔧 Setting up PostgreSQL schema..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOSQL'
-- Drop existing tables
DROP TABLE IF EXISTS soalUjian CASCADE;
DROP TABLE IF EXISTS taksonomi CASCADE;
DROP TABLE IF EXISTS konsentrasiKeahlianSekolah CASCADE;
DROP TABLE IF EXISTS schools CASCADE;

-- Create proper schema like your original script
CREATE TABLE schools (
    idSchool VARCHAR(255) PRIMARY KEY,
    namaSekolah TEXT NOT NULL,
    alamat TEXT,
    createdAt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE konsentrasiKeahlianSekolah (
    idKonsentrasiSekolah VARCHAR(255) PRIMARY KEY,
    namaKonsentrasi TEXT NOT NULL,
    idSchool VARCHAR(255) REFERENCES schools(idSchool),
    createdAt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE taksonomi (
    idTaksonomi VARCHAR(255) PRIMARY KEY,
    namaTaksonomi TEXT NOT NULL,
    levelTaksonomi INTEGER DEFAULT 1,
    parentId VARCHAR(255),
    createdAt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE soalUjian (
    idSoalUjian VARCHAR(255) PRIMARY KEY,
    namaUjian TEXT,
    pertanyaan TEXT,
    jenisSoal VARCHAR(50) DEFAULT 'PG',
    bobot INTEGER DEFAULT 1,
    jawabanBenar JSONB,
    opsi JSONB,
    idTaksonomi VARCHAR(255) REFERENCES taksonomi(idTaksonomi),
    idKonsentrasiSekolah VARCHAR(255) REFERENCES konsentrasiKeahlianSekolah(idKonsentrasiSekolah),
    createdAt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    createdBy TEXT DEFAULT 'BramMahendrawan'
);

-- Create proper indexes
CREATE INDEX idx_soal_taksonomi ON soalUjian(idTaksonomi);
CREATE INDEX idx_soal_konsentrasi ON soalUjian(idKonsentrasiSekolah);
CREATE INDEX idx_soal_jenis ON soalUjian(jenisSoal);
CREATE INDEX idx_soal_bobot ON soalUjian(bobot);
CREATE INDEX idx_konsentrasi_school ON konsentrasiKeahlianSekolah(idSchool);
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

