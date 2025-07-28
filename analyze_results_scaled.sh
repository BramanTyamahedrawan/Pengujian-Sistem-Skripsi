#!/bin/bash

# Enhanced logging with colors and icons
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Function for colored output with icons
log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }
log_header() { echo -e "${PURPLE}📊 $1${NC}"; }
log_subheader() { echo -e "${CYAN}📋 $1${NC}"; }

clear
echo -e "${WHITE}"
cat << "EOF"
╔══════════════════════════════════════════════════════════════════════════════╗
║                  🚀 ENHANCED SCALED BENCHMARK ANALYSIS 🚀                   ║
║                        PostgreSQL vs HBase Performance                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

log_info "Analysis Started: $(date '+%Y-%m-%d %H:%M:%S UTC')"
log_info "Analyzing User: BramMahendrawan"
log_info "System: $(uname -s) $(uname -r)"

# Use environment variables with fallbacks
RESULTS_FILE="${BENCHMARK_RESULTS_FILE:-$(pwd)/benchmark-results/benchmark_results_scaled.csv}"
BASE_DIR="${BENCHMARK_BASE_DIR:-$(pwd)}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
ANALYSIS_DIR="$(dirname "$RESULTS_FILE")/analysis"

# Create analysis directory
mkdir -p "$ANALYSIS_DIR"

# Ensure results file exists
if [ ! -f "$RESULTS_FILE" ]; then
    log_error "Results file not found: $RESULTS_FILE"
    log_warning "Please run benchmark tests first!"
    exit 1
fi

# Check file size and record count
FILE_SIZE=$(du -h "$RESULTS_FILE" | cut -f1)
RECORD_COUNT=$(tail -n +2 "$RESULTS_FILE" | wc -l)
log_success "Results file loaded: $FILE_SIZE ($RECORD_COUNT records)"

echo ""
log_header "RAW BENCHMARK RESULTS"
echo "══════════════════════════════════════════════════════════════════════════════"
cat "$RESULTS_FILE" | column -t -s ','
echo ""

log_header "COMPREHENSIVE PERFORMANCE ANALYSIS"
echo "══════════════════════════════════════════════════════════════════════════════"

# Enhanced analysis with awk
awk -F',' -v GREEN="$GREEN" -v RED="$RED" -v YELLOW="$YELLOW" -v BLUE="$BLUE" -v PURPLE="$PURPLE" -v CYAN="$CYAN" -v WHITE="$WHITE" -v NC="$NC" '
BEGIN {
    printf "%s🎯 MULTI-SCALE BENCHMARK ANALYSIS REPORT%s\n", WHITE, NC
    printf "%s================================================================%s\n", WHITE, NC
    print ""

    # Initialize data structures
    scales[1] = 100; scales[2] = 1000; scales[3] = 10000; scales[4] = 100000; scales[5] = 1000000; scales[6] = 10000000
    phases[1] = "📝 Data Generation"; phases[2] = "🔍 Read Performance"; phases[3] = "✏️ CRUD Operations"
    phases[4] = "⚡ Latency Testing"; phases[5] = "🚀 Throughput Testing"
    
    # Performance metrics
    total_tests = 0
    fastest_operation = ""
    fastest_time = 999999
    slowest_operation = ""
    slowest_time = 0
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
    
    total_tests++
    
    # Track fastest and slowest operations
    if (duration > 0) {
        if (duration < fastest_time) {
            fastest_time = duration
            fastest_operation = database "_" operation "_" scale
        }
        if (duration > slowest_time) {
            slowest_time = duration
            slowest_operation = database "_" operation "_" scale
        }
    }

    # Store data for analysis
    key = scale "_" phase "_" operation
    if (database == "postgresql") {
        psql_times[key] = duration
        psql_throughput[key] = throughput
        psql_records[key] = records
        psql_notes[key] = notes
    } else if (database == "hbase") {
        hbase_times[key] = duration
        hbase_throughput[key] = throughput
        hbase_records[key] = records
        hbase_notes[key] = notes
    }

    # Track statistics
    all_scales[scale] = 1
    all_phases[phase] = 1
    all_operations[operation] = 1
    
    # Database operation counts
    db_ops[database]++
    scale_ops[scale]++
    phase_ops[phase]++
}
END {
    # Quick Statistics Overview
    printf "%s📊 BENCHMARK OVERVIEW%s\n", CYAN, NC
    printf "%s══════════════════════════════════════════════════════════════════════════════%s\n", CYAN, NC
    printf "🔢 Total Tests Executed: %d\n", total_tests
    printf "🏢 Databases Tested: PostgreSQL, HBase\n"
    printf "📈 Scales Tested: %d different scales\n", length(all_scales)
    printf "🎯 Phases Analyzed: %d testing phases\n", length(all_phases)
    printf "⚡ Fastest Operation: %s (%.2f ms)\n", fastest_operation, fastest_time
    printf "🐌 Slowest Operation: %s (%.2f ms)\n", slowest_operation, slowest_time
    print ""
    
    # Scale-by-scale detailed comparison
    printf "%s🎯 SCALE-BY-SCALE PERFORMANCE COMPARISON%s\n", PURPLE, NC
    printf "%s══════════════════════════════════════════════════════════════════════════════%s\n", PURPLE, NC

    overall_psql_wins = 0
    overall_hbase_wins = 0
    overall_ties = 0

    for (scale in all_scales) {
        printf "\n%s📊 SCALE: %s RECORDS%s\n", BLUE, scale, NC
        printf "%s────────────────────────────────────────────────────────────────────────────────%s\n", BLUE, NC
        printf "%-30s | %-12s | %-10s | %-12s | %-10s | %s\n", "Operation", "PostgreSQL", "HBase", "Winner", "Advantage", "Performance Gap"
        printf "%-30s | %-12s | %-10s | %-12s | %-10s | %s\n", "──────────────────────────────", "────────────", "──────────", "────────────", "──────────", "────────────────"

        scale_psql_wins = 0
        scale_hbase_wins = 0
        scale_ties = 0

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
                                winner = "🏆 PostgreSQL"
                                advantage = sprintf("%.1fx faster", hbase_time / psql_time)
                                gap = sprintf("%.1f%% better", ((hbase_time - psql_time) / hbase_time) * 100)
                                scale_psql_wins++
                                overall_psql_wins++
                            } else if (hbase_time < psql_time) {
                                winner = "🏆 HBase"
                                advantage = sprintf("%.1fx faster", psql_time / hbase_time)
                                gap = sprintf("%.1f%% better", ((psql_time - hbase_time) / psql_time) * 100)
                                scale_hbase_wins++
                                overall_hbase_wins++
                            } else {
                                winner = "🤝 Tie"
                                advantage = "Equal"
                                gap = "0% diff"
                                scale_ties++
                                overall_ties++
                            }

                            printf "%-30s | %8s ms | %6s ms | %-12s | %-10s | %s\n", operation, psql_time, hbase_time, winner, advantage, gap
                        }
                    }
                }
            }
        }

        printf "\n%s🏆 Scale %s Summary:%s\n", GREEN, scale, NC
        printf "   PostgreSQL wins: %d 🏆 | HBase wins: %d 🏆 | Ties: %d 🤝\n", scale_psql_wins, scale_hbase_wins, scale_ties
        
        if (scale_psql_wins > scale_hbase_wins) {
            printf "   %s✨ Overall scale winner: PostgreSQL%s\n", GREEN, NC
        } else if (scale_hbase_wins > scale_psql_wins) {
            printf "   %s✨ Overall scale winner: HBase%s\n", GREEN, NC
        } else {
            printf "   %s🤝 Scale result: Balanced performance%s\n", YELLOW, NC
        }
    }

    # Advanced Performance Analytics
    printf "\n%s📈 ADVANCED PERFORMANCE ANALYTICS%s\n", PURPLE, NC
    printf "%s══════════════════════════════════════════════════════════════════════════════%s\n", PURPLE, NC

    # Throughput Analysis
    printf "\n%s🚀 THROUGHPUT ANALYSIS (Operations per Second)%s\n", CYAN, NC
    printf "%s────────────────────────────────────────────────────────────────────────────────%s\n", CYAN, NC
    printf "%-12s | %-25s | %-15s | %-12s | %-12s | %s\n", "Scale", "Operation", "PostgreSQL", "HBase", "Winner", "Performance Ratio"
    printf "%-12s | %-25s | %-15s | %-12s | %-12s | %s\n", "────────────", "─────────────────────────", "───────────────", "────────────", "────────────", "────────────────"

    for (scale in all_scales) {
        for (key in psql_throughput) {
            if (key in hbase_throughput) {
                split(key, parts, "_")
                if (parts[1] == scale) {
                    operation = parts[2] "_" parts[3]
                    psql_tps = psql_throughput[key]
                    hbase_tps = hbase_throughput[key]

                    if (psql_tps > 0 && hbase_tps > 0) {
                        if (psql_tps > hbase_tps) {
                            winner = "🏆 PostgreSQL"
                            ratio = sprintf("%.2fx", psql_tps / hbase_tps)
                        } else if (hbase_tps > psql_tps) {
                            winner = "🏆 HBase"
                            ratio = sprintf("%.2fx", hbase_tps / psql_tps)
                        } else {
                            winner = "🤝 Equal"
                            ratio = "1.00x"
                        }
                        printf "%-12s | %-25s | %11s ops/s | %8s ops/s | %-12s | %s\n", scale, operation, psql_tps, hbase_tps, winner, ratio
                    }
                }
            }
        }
    }

    # Scalability Analysis
    printf "\n%s📊 SCALABILITY ANALYSIS%s\n", PURPLE, NC
    printf "%s────────────────────────────────────────────────────────────────────────────────%s\n", PURPLE, NC
    
    for (operation in all_operations) {
        printf "\n%s🎯 Operation: %s%s\n", BLUE, operation, NC
        printf "%-12s | %-15s | %-12s | %-15s | %s\n", "Scale", "PostgreSQL", "HBase", "Perf. Ratio", "Scalability Trend"
        printf "%-12s | %-15s | %-12s | %-15s | %s\n", "────────────", "───────────────", "────────────", "───────────────", "─────────────────"

        prev_psql = 0
        prev_hbase = 0
        
        for (scale in all_scales) {
            for (phase in all_phases) {
                key = scale "_" phase "_" operation
                if (key in psql_times && key in hbase_times) {
                    psql_time = psql_times[key]
                    hbase_time = hbase_times[key]
                    
                    if (psql_time > 0 && hbase_time > 0) {
                        ratio = sprintf("%.2f", psql_time / hbase_time)
                        
                        # Determine trend
                        trend = ""
                        if (prev_psql > 0 && prev_hbase > 0) {
                            psql_change = (psql_time - prev_psql) / prev_psql * 100
                            hbase_change = (hbase_time - prev_hbase) / prev_hbase * 100
                            
                            if (psql_change < hbase_change) {
                                trend = "📈 PostgreSQL scales better"
                            } else if (hbase_change < psql_change) {
                                trend = "📈 HBase scales better"
                            } else {
                                trend = "📊 Similar scaling"
                            }
                        } else {
                            trend = "📋 Baseline"
                        }
                        
                        printf "%-12s | %11s ms | %8s ms | %13s | %s\n", scale, psql_time, hbase_time, ratio, trend
                        
                        prev_psql = psql_time
                        prev_hbase = hbase_time
                    }
                }
            }
        }
    }

    # Overall Performance Summary
    printf "\n%s🏆 OVERALL PERFORMANCE SUMMARY%s\n", WHITE, NC
    printf "%s══════════════════════════════════════════════════════════════════════════════%s\n", WHITE, NC

    total_comparisons = overall_psql_wins + overall_hbase_wins + overall_ties
    
    if (total_comparisons > 0) {
        psql_win_rate = (overall_psql_wins / total_comparisons) * 100
        hbase_win_rate = (overall_hbase_wins / total_comparisons) * 100
        tie_rate = (overall_ties / total_comparisons) * 100
        
        printf "📊 Total Performance Comparisons: %d\n", total_comparisons
        printf "🏆 PostgreSQL Wins: %d (%.1f%%)\n", overall_psql_wins, psql_win_rate
        printf "🏆 HBase Wins: %d (%.1f%%)\n", overall_hbase_wins, hbase_win_rate
        printf "🤝 Tied Results: %d (%.1f%%)\n", overall_ties, tie_rate
        printf "\n"
        
        if (overall_psql_wins > overall_hbase_wins) {
            printf "%s🎉 ULTIMATE WINNER: PostgreSQL%s\n", GREEN, NC
            printf "%s   Victory margin: %d operations%s\n", GREEN, overall_psql_wins - overall_hbase_wins, NC
            printf "%s   Win rate advantage: %.1f%%%s\n", GREEN, psql_win_rate - hbase_win_rate, NC
        } else if (overall_hbase_wins > overall_psql_wins) {
            printf "%s🎉 ULTIMATE WINNER: HBase%s\n", GREEN, NC
            printf "%s   Victory margin: %d operations%s\n", GREEN, overall_hbase_wins - overall_psql_wins, NC
            printf "%s   Win rate advantage: %.1f%%%s\n", GREEN, hbase_win_rate - psql_win_rate, NC
        } else {
            printf "%s🤝 ULTIMATE RESULT: Perfectly Balanced Performance%s\n", YELLOW, NC
            printf "%s   Both databases show equivalent capabilities%s\n", YELLOW, NC
        }
    }

    # Performance Insights and Recommendations
    printf "\n%s💡 PERFORMANCE INSIGHTS & RECOMMENDATIONS%s\n", CYAN, NC
    printf "%s══════════════════════════════════════════════════════════════════════════════%s\n", CYAN, NC
    
    # Calculate average times for insights
    total_psql_time = 0; total_hbase_time = 0
    total_psql_ops = 0; total_hbase_ops = 0
    
    for (key in psql_times) {
        if (key in hbase_times && psql_times[key] > 0 && hbase_times[key] > 0) {
            total_psql_time += psql_times[key]
            total_hbase_time += hbase_times[key]
            total_psql_ops++
            total_hbase_ops++
        }
    }
    
    if (total_psql_ops > 0 && total_hbase_ops > 0) {
        avg_psql = total_psql_time / total_psql_ops
        avg_hbase = total_hbase_time / total_hbase_ops
        
        printf "📊 Average PostgreSQL Response Time: %.1f ms\n", avg_psql
        printf "📊 Average HBase Response Time: %.1f ms\n", avg_hbase
        printf "📊 Performance Difference: %.1f ms (%.1f%%)\n", abs(avg_psql - avg_hbase), abs(avg_psql - avg_hbase) / ((avg_psql + avg_hbase) / 2) * 100
        
        printf "\n%s🎯 STRATEGIC RECOMMENDATIONS:%s\n", GREEN, NC
        if (avg_psql < avg_hbase) {
            printf "✅ PostgreSQL shows superior average performance\n"
            printf "💡 Consider PostgreSQL for: Complex queries, ACID transactions, structured data\n"
            printf "📈 PostgreSQL advantage: %.1f%% faster average response time\n", ((avg_hbase - avg_psql) / avg_hbase) * 100
        } else {
            printf "✅ HBase shows superior average performance\n"
            printf "💡 Consider HBase for: Large-scale data, horizontal scaling, column-oriented storage\n"
            printf "📈 HBase advantage: %.1f%% faster average response time\n", ((avg_psql - avg_hbase) / avg_psql) * 100
        }
    }
}

function abs(x) { return x < 0 ? -x : x }
' "$RESULTS_FILE"

# Generate enhanced reports
echo ""
log_header "GENERATING ENHANCED ANALYSIS REPORTS"
echo "══════════════════════════════════════════════════════════════════════════════"

# Create detailed summary CSV
SUMMARY_FILE="$ANALYSIS_DIR/detailed_summary_$(date +%Y%m%d_%H%M%S).csv"
log_info "Creating detailed summary CSV..."

echo "timestamp,scale,database,operation,duration_ms,throughput_rps,records_affected,performance_category,notes" > "$SUMMARY_FILE"

awk -F',' '
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
    
    # Categorize performance
    if (duration <= 100) category = "Excellent"
    else if (duration <= 500) category = "Good"
    else if (duration <= 1000) category = "Average"
    else if (duration <= 5000) category = "Poor"
    else category = "Critical"
    
    print timestamp "," scale "," database "," operation "," duration "," throughput "," records "," category "," notes
}' "$RESULTS_FILE" >> "$SUMMARY_FILE"

log_success "Detailed summary created: $SUMMARY_FILE"

# Create performance comparison matrix
MATRIX_FILE="$ANALYSIS_DIR/performance_matrix_$(date +%Y%m%d_%H%M%S).csv"
log_info "Creating performance comparison matrix..."

echo "scale,operation,postgresql_ms,hbase_ms,winner,advantage_ratio,performance_gap_percent" > "$MATRIX_FILE"

awk -F',' '
NR > 1 {
    scale = $2
    phase = $3
    operation = $4
    database = $5
    duration = $6
    
    key = scale "_" phase "_" operation
    if (database == "postgresql") {
        psql_times[key] = duration
    } else if (database == "hbase") {
        hbase_times[key] = duration
    }
}
END {
    for (key in psql_times) {
        if (key in hbase_times) {
            split(key, parts, "_")
            scale = parts[1]
            operation = parts[2] "_" parts[3]
            psql_time = psql_times[key]
            hbase_time = hbase_times[key]
            
            if (psql_time > 0 && hbase_time > 0) {
                if (psql_time < hbase_time) {
                    winner = "PostgreSQL"
                    ratio = hbase_time / psql_time
                    gap = ((hbase_time - psql_time) / hbase_time) * 100
                } else {
                    winner = "HBase"
                    ratio = psql_time / hbase_time
                    gap = ((psql_time - hbase_time) / psql_time) * 100
                }
                
                print scale "," operation "," psql_time "," hbase_time "," winner "," sprintf("%.2f", ratio) "," sprintf("%.1f", gap)
            }
        }
    }
}' "$RESULTS_FILE" >> "$MATRIX_FILE"

log_success "Performance matrix created: $MATRIX_FILE"

# Create scalability report
SCALABILITY_FILE="$ANALYSIS_DIR/scalability_report_$(date +%Y%m%d_%H%M%S).csv"
log_info "Creating scalability analysis report..."

echo "operation,scale_100,scale_1000,scale_10000,scale_100000,scale_1000000,database,scalability_trend" > "$SCALABILITY_FILE"

awk -F',' '
NR > 1 {
    operation = $4
    database = $5
    scale = $2
    duration = $6
    
    key = operation "_" database
    times[key "_" scale] = duration
    ops[key] = 1
    databases[database] = 1
}
END {
    for (key in ops) {
        split(key, parts, "_")
        operation = parts[1]
        database = parts[2]
        
        line = operation
        trend = "stable"
        prev_time = 0
        
        # Check each scale
        scales[1] = 100; scales[2] = 1000; scales[3] = 10000; scales[4] = 100000; scales[5] = 1000000
        
        for (i = 1; i <= 5; i++) {
            scale = scales[i]
            time_key = key "_" scale
            if (time_key in times) {
                line = line "," times[time_key]
                
                # Determine trend
                if (prev_time > 0) {
                    growth = (times[time_key] - prev_time) / prev_time
                    if (growth > 2) trend = "exponential"
                    else if (growth > 1) trend = "high_growth"
                    else if (growth > 0.5) trend = "moderate_growth"
                    else if (growth > 0.1) trend = "linear"
                    else trend = "excellent"
                }
                prev_time = times[time_key]
            } else {
                line = line ",N/A"
            }
        }
        
        line = line "," database "," trend
        print line
    }
}' "$RESULTS_FILE" >> "$SCALABILITY_FILE"

log_success "Scalability report created: $SCALABILITY_FILE"

# Generate final statistics
echo ""
log_header "FINAL ANALYSIS STATISTICS"
echo "══════════════════════════════════════════════════════════════════════════════"

FINAL_STATS=$(awk -F',' '
NR > 1 {
    total_operations++
    if ($5 == "postgresql") postgresql_ops++
    else if ($5 == "hbase") hbase_ops++
    
    if ($6 > 0) {
        total_duration += $6
        if ($6 < min_time || min_time == 0) min_time = $6
        if ($6 > max_time) max_time = $6
    }
    
    if ($8 > 0) {
        total_throughput += $8
        if ($8 > max_throughput) max_throughput = $8
    }
}
END {
    avg_duration = total_duration / total_operations
    avg_throughput = total_throughput / total_operations
    
    printf "📊 Total Operations: %d\n", total_operations
    printf "🐘 PostgreSQL Operations: %d\n", postgresql_ops
    printf "🏗️  HBase Operations: %d\n", hbase_ops
    printf "⚡ Fastest Operation: %.1f ms\n", min_time
    printf "🐌 Slowest Operation: %.1f ms\n", max_time
    printf "📈 Average Duration: %.1f ms\n", avg_duration
    printf "🚀 Average Throughput: %.1f ops/s\n", avg_throughput
    printf "🏆 Peak Throughput: %.1f ops/s\n", max_throughput
    printf "📁 Results File Size: %s\n", "'$(du -h "$RESULTS_FILE" | cut -f1)'"
}' "$RESULTS_FILE")

echo "$FINAL_STATS"

echo ""
log_success "All analysis reports generated in: $ANALYSIS_DIR"
log_info "Files created:"
echo "   📄 Detailed Summary: $(basename "$SUMMARY_FILE")"
echo "   📊 Performance Matrix: $(basename "$MATRIX_FILE")"
echo "   📈 Scalability Report: $(basename "$SCALABILITY_FILE")"

echo ""
echo -e "${WHITE}"
cat << "EOF"
╔══════════════════════════════════════════════════════════════════════════════╗
║                    ✨ ANALYSIS COMPLETED SUCCESSFULLY ✨                    ║
║                                                                              ║
║  🎯 Comprehensive performance comparison completed                           ║
║  📊 Multi-scale analysis with detailed insights                             ║
║  📈 Scalability trends and recommendations provided                         ║
║  💾 Enhanced reports saved for future reference                             ║
╚══════════════════════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

log_success "Enhanced Scaled Benchmark Analysis Completed!"
log_info "Analysis Duration: $(($(date +%s) - $(date -d "$TIMESTAMP" +%s))) seconds"