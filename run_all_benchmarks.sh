#!/bin/bash
# Run all xraygraph-bench benchmarks against xrayGraphDB
# Usage: ./run_all_benchmarks.sh [host] [port]

set -euo pipefail

HOST="${1:-localhost}"
PORT="${2:-7689}"
ENGINE="${3:-xraygraphdb-native}"
RESULTS_DIR="/opt/xraybench-results/$(date +%Y%m%d_%H%M%S)"
BENCH_DIR="/opt/xraygraph-bench/benchmarks/suites"
LOG_FILE="${RESULTS_DIR}/run.log"

mkdir -p "$RESULTS_DIR"

echo "=== xraygraph-bench Full Suite ===" | tee "$LOG_FILE"
echo "Engine: $ENGINE @ $HOST:$PORT" | tee -a "$LOG_FILE"
echo "Results: $RESULTS_DIR" | tee -a "$LOG_FILE"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

TOTAL=0
PASS=0
FAIL=0
SKIP=0

# Benchmarks that require unavailable procedures/features — skip these
SKIP_LIST=(
    # xray.* native procedures (may not be available in this build)
    "budgeted-path-search"      # requires xray.find_path_budgeted()
    "query-budget-enforcement"   # requires xray.query_budget()
    "semantic-search"            # requires xray.semantic_search()
    "impact-analysis-procedure"  # requires xray.impact_analysis()
    "edge-aggregate"             # requires xray.edge_aggregate()
    "live-aggregate"             # requires xray.live_aggregate()
    "frontier-profile"           # requires xray.frontier_profile()
    "similarity-clustering"      # requires xray.similarity()
    "neighborhood-stats"         # requires xray.neighborhood_stats()
    "topk-reachable"             # requires xray.topk_reachable()
    "full-health-report"         # requires xray.health_report()
    # Vector search (requires embedding infrastructure)
    "graph-constrained-ann"      # requires vector index
    "multihop-from-vector-seeds" # requires vector index
    "vector-to-graph-expansion"  # requires vector index
    # Generators not implemented
    "code-dependency-analysis"   # requires code_graph generator
    "impact-analysis"            # requires code_graph generator
    "lineage-basic"              # requires provenance_dag generator
    "complexity-analysis"        # requires code_graph generator
    "coupling-analysis"          # requires code_graph generator
    "dead-code-detection"        # requires code_graph generator
    # GFQL requires xrayProtocol (not Bolt)
    "aggregate-gfql"             # requires GFQL via xrayProtocol
    "dependency-gfql"            # requires GFQL via xrayProtocol
    "multi-hop-gfql"             # requires GFQL via xrayProtocol
    "scan-filter-gfql"           # requires GFQL via xrayProtocol
    "transpilation-overhead"     # requires GFQL via xrayProtocol
    "traversal-gfql"             # requires GFQL via xrayProtocol
    # Ingestion benchmarks (need special measurement approach)
    "bulk-node-create"           # ingestion benchmark, not query benchmark
    "bulk-edge-create"           # ingestion benchmark
    "batch-size-scaling"         # ingestion benchmark
    "index-build-time"           # ingestion benchmark
    "mixed-ingest"               # ingestion benchmark
    "property-width-scaling"     # ingestion benchmark
)

should_skip() {
    local name="$1"
    for skip in "${SKIP_LIST[@]}"; do
        if [ "$name" = "$skip" ]; then
            return 0
        fi
    done
    return 1
}

# Find all benchmark specs
for spec in $(find "$BENCH_DIR" -name "benchmark.yaml" -type f | sort); do
    TOTAL=$((TOTAL + 1))
    bench_name=$(basename "$(dirname "$spec")")
    family=$(basename "$(dirname "$(dirname "$spec")")")

    if should_skip "$bench_name"; then
        SKIP=$((SKIP + 1))
        echo "[$TOTAL] SKIP  $family/$bench_name (requires unavailable feature)" | tee -a "$LOG_FILE"
        continue
    fi

    outfile="${RESULTS_DIR}/${family}__${bench_name}.json"
    echo -n "[$TOTAL] RUN   $family/$bench_name ... " | tee -a "$LOG_FILE"

    # Clean database between benchmarks to prevent OOM
    # Always use Bolt (7687) for cleanup regardless of benchmark protocol
    python3.12 -c "
from neo4j import GraphDatabase
d = GraphDatabase.driver('bolt://$HOST:7687')
with d.session() as s:
    s.run('MATCH (n) DETACH DELETE n').consume()
d.close()
" >> "$LOG_FILE" 2>&1 || true

    if timeout 300 xraybench run "$spec" \
        --engine "$ENGINE" \
        --host "$HOST" \
        --port "$PORT" \
        --output "$outfile" \
        --param row_count=100000 \
        >> "$LOG_FILE" 2>&1; then

        # Check if correctness passed
        if [ -f "$outfile" ]; then
            passed=$(python3.12 -c "import json; d=json.load(open('$outfile')); print(d.get('correctness',{}).get('passed','unknown'))" 2>/dev/null || echo "unknown")
            outcome=$(python3.12 -c "import json; d=json.load(open('$outfile')); print(d.get('outcome','unknown'))" 2>/dev/null || echo "unknown")
            warm_ms=$(python3.12 -c "import json; d=json.load(open('$outfile')); print(round(d.get('warm_ms',0),2))" 2>/dev/null || echo "?")

            if [ "$outcome" = "success" ]; then
                PASS=$((PASS + 1))
                echo "PASS  warm=${warm_ms}ms" | tee -a "$LOG_FILE"
            else
                FAIL=$((FAIL + 1))
                echo "FAIL  outcome=$outcome" | tee -a "$LOG_FILE"
            fi
        else
            FAIL=$((FAIL + 1))
            echo "FAIL  (no output file)" | tee -a "$LOG_FILE"
        fi
    else
        FAIL=$((FAIL + 1))
        echo "FAIL  (timeout or error)" | tee -a "$LOG_FILE"
    fi
done

echo "" | tee -a "$LOG_FILE"
echo "=== SUMMARY ===" | tee -a "$LOG_FILE"
echo "Total: $TOTAL  Pass: $PASS  Fail: $FAIL  Skip: $SKIP" | tee -a "$LOG_FILE"
echo "Finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG_FILE"
echo "Results directory: $RESULTS_DIR" | tee -a "$LOG_FILE"
