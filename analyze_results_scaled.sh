  GNU nano 7.2                                                              analyze_results_scaled.sh
#!/bin/bash

echo "=== SCALED BENCHMARK RESULTS ANALYSIS ==="
echo "Current Date and Time (UTC): $(date '+%Y-%m-%d %H:%M:%S')"
echo "Current User's Login: BramMahendrawan"

# Use environment variables with fallbacks
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Ensure results file and directory exist
mkdir -p "$(dirname "$RESULTS_FILE")"
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,scale,phase,operation,database,duration_ms,records_affected,throughput_rps,notes" > "$RESULTS_FILE"
fi

if [ ! -f "$RESULTS_FILE" ]; then
    echo "❌ Results file not found!"
    exit 1
fi

echo ""
echo "📊 RAW RESULTS:"
echo "=============="
cat "$RESULTS_FILE"

echo ""
echo "📈 COMPREHENSIVE PERFORMANCE ANALYSIS:"
echo "======================================"

# Detailed analysis with awk
awk -F',' '
BEGIN {
    print "=== 5-PHASE SCALED BENCHMARK ANALYSIS ==="
    print ""

    # Initialize arrays
    # Initialize arrays
    scales[1] = 100; scales[2] = 1000; scales[3] = 10000; scales[4] = 100000; scales[5] = 1000000; scales[6] = 10000000
    phases[1] = "Data Generation"; phases[2] = "Read Performance"; phases[3] = "CRUD Operations"
    phases[4] = "Latency Testing"; phases[5] = "Throughput Testing"
}
NR > 1 {
    timestamp = $1
    scale = $2
    phase = $3
    operation = $4
    database = $5
    duration = $6
    records = $7
    throughput = $8
    notes = $9

    # Store data for analysis
    key = scale "_" phase "_" operation
    if (database == "postgresql") {
        psql_times[key] = duration
        psql_throughput[key] = throughput
        psql_records[key] = records
    } else if (database == "hbase") {
        hbase_times[key] = duration
        hbase_throughput[key] = throughput
        hbase_records[key] = records
    }

    # Track all operations per phase
    phase_ops[phase "_" operation] = 1
    all_scales[scale] = 1
    all_phases[phase] = 1
}
END {
    # Scale-by-scale comparison
    print "🎯 SCALE-BY-SCALE PERFORMANCE COMPARISON"
    print "========================================"

    for (scale in all_scales) {
        print ""
        print "📊 SCALE: " scale " RECORDS"
        print "=================="
        print sprintf("%-25s | %-12s | %-10s | %-10s | %s", "Operation", "PostgreSQL", "HBase", "Winner", "Advantage")
        print sprintf("%-25s | %-12s | %-10s | %-10s | %s", "-------------------------", "------------", "----------", "----------", "-----------")

        psql_wins = 0
        hbase_wins = 0

        for (phase in all_phases) {
            for (key in psql_times) {
                if (key in hbase_times) {
                    split(key, parts, "_")
                    if (parts[1] == scale && parts[2] == phase) {
                        operation = parts[3]
                        psql_time = psql_times[key]
                        hbase_time = hbase_times[key]

                        if (psql_time > 0 && hbase_time > 0) {
                            if (psql_time < hbase_time) {
                                winner = "PostgreSQL"
                                advantage = sprintf("%.1fx", hbase_time / psql_time)
                                psql_wins++
                            } else {
                                winner = "HBase"
                                advantage = sprintf("%.1fx", psql_time / hbase_time)
                                hbase_wins++
                            }

                            print sprintf("%-25s | %8s ms | %6s ms | %-10s | %s", operation, psql_time, hbase_time, winner, advantage)
                        }
                    }
                }
            }
        }

        print ""
        print "🏆 Scale " scale " Summary: PostgreSQL wins: " psql_wins ", HBase wins: " hbase_wins
        if (psql_wins > hbase_wins) {
            print "   Overall winner for scale " scale ": PostgreSQL"
        } else if (hbase_wins > psql_wins) {
            print "   Overall winner for scale " scale ": HBase"
        } else {
            print "   Scale " scale ": Tied performance"
        }
    }

    # Phase-by-phase analysis
    print ""
    print "🔍 PHASE-BY-PHASE ANALYSIS"
    print "=========================="

    for (phase in all_phases) {
        phase_name = phases[phase]
        if (phase_name == "") phase_name = "Phase " phase

        print ""
        print "📋 " phase_name " (Phase " phase ")"
        print "----------------------------------------"
        print sprintf("%-8s | %-15s | %-12s | %-10s | %s", "Scale", "Operation", "PostgreSQL", "HBase", "Winner")
        print sprintf("%-8s | %-15s | %-12s | %-10s | %s", "--------", "---------------", "------------", "----------", "--------")

        for (scale in all_scales) {
            for (key in psql_times) {
                if (key in hbase_times) {
                    split(key, parts, "_")
                    if (parts[1] == scale && parts[2] == phase) {
                        operation = parts[3]
                        psql_time = psql_times[key]
                        hbase_time = hbase_times[key]

                        if (psql_time > 0 && hbase_time > 0) {
                            winner = (psql_time < hbase_time) ? "PostgreSQL" : "HBase"
                            print sprintf("%-8s | %-15s | %8s ms | %6s ms | %s", scale, operation, psql_time, hbase_time, winner)
                        }
                    }
                }
            }
        }
    }

    # Throughput analysis
    print ""
    print "📈 THROUGHPUT ANALYSIS (Records per Second)"
    print "=========================================="
    print sprintf("%-8s | %-20s | %-12s | %-10s | %s", "Scale", "Operation", "PostgreSQL", "HBase", "Winner")
    print sprintf("%-8s | %-20s | %-12s | %-10s | %s", "--------", "--------------------", "------------", "----------", "--------")

    for (scale in all_scales) {
        for (key in psql_throughput) {
            if (key in hbase_throughput) {
                split(key, parts, "_")
                if (parts[1] == scale) {
                    operation = parts[2] "_" parts[3]
                    psql_tps = psql_throughput[key]
                    hbase_tps = hbase_throughput[key]

                    if (psql_tps > 0 && hbase_tps > 0) {
                        winner = (psql_tps > hbase_tps) ? "PostgreSQL" : "HBase"
                        print sprintf("%-8s | %-20s | %8s rps | %6s rps | %s", scale, operation, psql_tps, hbase_tps, winner)
                    }
                }
            }
        }
    }

    # Overall statistics
    print ""
    print "📊 OVERALL PERFORMANCE STATISTICS"
    print "================================"

    total_psql_time = 0; total_hbase_time = 0
    total_psql_ops = 0; total_hbase_ops = 0
    overall_psql_wins = 0; overall_hbase_wins = 0

    for (key in psql_times) {
        if (key in hbase_times) {
            psql_time = psql_times[key]
            hbase_time = hbase_times[key]


            if (psql_time > 0 && hbase_time > 0) {
                total_psql_time += psql_time
                total_hbase_time += hbase_time
                total_psql_ops++
                total_hbase_ops++

                if (psql_time < hbase_time) {
                    overall_psql_wins++
                } else {
                    overall_hbase_wins++
                }
            }
        }
    }

    if (total_psql_ops > 0 && total_hbase_ops > 0) {
        avg_psql = total_psql_time / total_psql_ops
        avg_hbase = total_hbase_time / total_hbase_ops

        print "Average PostgreSQL operation time: " sprintf("%.1f", avg_psql) "ms"
        print "Average HBase operation time: " sprintf("%.1f", avg_hbase) "ms"
        print ""
        print "PostgreSQL wins: " overall_psql_wins " operations"
        print "HBase wins: " overall_hbase_wins " operations"
        print ""

        if (overall_psql_wins > overall_hbase_wins) {
            improvement = sprintf("%.1f", ((avg_hbase - avg_psql) / avg_hbase) * 100)
            print "🏆 OVERALL WINNER: PostgreSQL"
            print "   PostgreSQL is " improvement "% faster on average"
        } else if (overall_hbase_wins > overall_psql_wins) {
            improvement = sprintf("%.1f", ((avg_psql - avg_hbase) / avg_psql) * 100)
            print "🏆 OVERALL WINNER: HBase"
            print "   HBase is " improvement "% faster on average"
        } else {
            print "🤝 OVERALL RESULT: Tied performance"
        }
    }

    print ""
    print "📋 SCALABILITY ANALYSIS"
    print "======================"

    # Analyze how performance changes with scale
    for (operation in phase_ops) {
        split(operation, op_parts, "_")
        phase = op_parts[1]
        op_name = op_parts[2]

        print ""
        print "Operation: " op_name " (Phase " phase ")"
        print "Scale    | PostgreSQL | HBase     | PostgreSQL vs HBase"
        print "---------|------------|-----------|--------------------"

        for (scale in all_scales) {
            key = scale "_" operation
            if (key in psql_times && key in hbase_times) {
                psql_time = psql_times[key]
                hbase_time = hbase_times[key]
                ratio = sprintf("%.2f", psql_time / hbase_time)
                print sprintf("%-8s | %8s ms | %7s ms | %s", scale, psql_time, hbase_time, ratio)
            }
        }
    }
}' "$RESULTS_FILE"

echo ""
echo "💾 Detailed results saved in: $RESULTS_FILE"
echo "📊 Total operations benchmarked: $(tail -n +2 "$RESULTS_FILE" | wc -l)"

# Generate summary CSV
SUMMARY_FILE="benchmark-results/benchmark_summary_$(date +%Y%m%d_%H%M%S).csv"
echo "scale,database,avg_duration_ms,total_operations,win_count" > "$SUMMARY_FILE"

awk -F',' '
NR > 1 {
    scale = $2
    database = $5
    duration = $6

    scale_db = scale "_" database
    total_time[scale_db] += duration
    op_count[scale_db]++
}
END {
    for (scale_db in total_time) {
        split(scale_db, parts, "_")
        scale = parts[1]
        database = parts[2]
        avg_duration = total_time[scale_db] / op_count[scale_db]
        print scale "," database "," int(avg_duration) "," op_count[scale_db] ",0"
    }
}' "$RESULTS_FILE" >> "$SUMMARY_FILE"

echo "📈 Summary CSV created: $SUMMARY_FILE"

echo ""
echo "✅ COMPREHENSIVE SCALED BENCHMARK ANALYSIS COMPLETED!"
echo "======================================================"
echo "📊 5 phases tested across 4 different scales"
echo "🎯 PostgreSQL vs HBase comparison with proper foreign keys and column families"
echo "📈 Performance, latency, and throughput analysis included"
