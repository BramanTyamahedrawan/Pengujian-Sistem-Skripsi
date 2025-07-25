#!/bin/bash

echo "🚀 STARTING COMPREHENSIVE DATABASE BENCHMARK WITH SCALES"
echo "========================================================"
echo "Current Date and Time (UTC): $(date '+%Y-%m-%d %H:%M:%S')"
echo "Current User's Login: BramMahendrawan"
echo ""
echo "📋 5-Phase Testing with Multiple Scales:"
echo "  1. 📥 Data Generation Performance"
echo "  2. 🔍 Read Performance (COUNT, SELECT, SCAN)"
echo "  3. 📝 CRUD Operations (INSERT/PUT, UPDATE, DELETE)"
echo "  4. ⚡ Latency Testing (single record operations)"
echo "  5. 📈 Throughput Testing (bulk operations)"
echo ""
echo "🎯 Scales: 100 → 1000 → 10000 → 100000  → 1000000  → 10000000 records"

# Make all scripts executable
chmod +x setup_benchmark_scaled.sh
chmod +x generate_test_data.sh
chmod +x benchmark_hbase_scaled.sh
chmod +x benchmark_postgresql_scaled.sh
chmod +x benchmark_crud_scaled.sh
chmod +x benchmark_latency_scaled.sh
chmod +x benchmark_throughput_scaled.sh
chmod +x analyze_results_scaled.sh

# Setup
echo ""
echo "🔧 Setting up benchmark environment..."
./setup_benchmark_scaled.sh

# Create logs directory
mkdir -p logs

# Run all benchmarks for each scale
for scale in 100 1000 10000 100000 1000000 10000000; do
    echo ""
    echo "🎯 TESTING SCALE: $scale RECORDS"
    echo "================================"
    echo "⏰ Started at: $(date '+%Y-%m-%d %H:%M:%S UTC')"

    # Generate test data for this scale
    echo ""
    echo "📝 Phase 1: Generating test data..."
    cd test-data-scaled
    ../generate_test_data.sh $scale 2>&1 | tee "../logs/generate_${scale}_$(date +%Y%m%d_%H%M%S).log"
    cd ..

    # Run 5-phase benchmarks
    echo ""
    echo "🔍 Phase 2: Read Performance..."
    ./benchmark_postgresql_scaled.sh $scale 2>&1 | tee "logs/postgresql_${scale}_$(date +%Y%m%d_%H%M%S).log"
    ./benchmark_hbase_scaled.sh $scale 2>&1 | tee "logs/hbase_${scale}_$(date +%Y%m%d_%H%M%S).log"

    echo ""
    echo "📝 Phase 3: CRUD Operations..."
    ./benchmark_crud_scaled.sh $scale 2>&1 | tee "logs/crud_${scale}_$(date +%Y%m%d_%H%M%S).log"

    echo ""
    echo "⚡ Phase 4: Latency Testing..."
    ./benchmark_latency_scaled.sh $scale 2>&1 | tee "logs/latency_${scale}_$(date +%Y%m%d_%H%M%S).log"

    echo ""
    echo "📈 Phase 5: Throughput Testing..."
    ./benchmark_throughput_scaled.sh $scale 2>&1 | tee "logs/throughput_${scale}_$(date +%Y%m%d_%H%M%S).log"

    echo ""
    echo "✅ Scale $scale completed at: $(date '+%Y-%m-%d %H:%M:%S UTC')"
    echo ""
done

# Analyze results
echo ""
echo "📊 Analyzing comprehensive results..."
./analyze_results_scaled.sh 2>&1 | tee "logs/analysis_$(date +%Y%m%d_%H%M%S).log"

echo ""
echo "🎉 ALL SCALED BENCHMARKS COMPLETED!"
echo "=================================="
echo "📁 Results available in: benchmark-results/"
echo "📊 Main CSV: benchmark-results/benchmark_results_scaled.csv"
echo "📈 Summary: benchmark-results/benchmark_summary_*.csv"
echo "📋 Logs: logs/"
echo ""
echo "⏰ Total benchmark completed at: $(date '+%Y-%m-%d %H:%M:%S UTC')"
echo "👤 Benchmark executed by: BramMahendrawan"
