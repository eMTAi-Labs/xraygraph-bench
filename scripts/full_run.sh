#!/bin/bash
# Full benchmark run: load LDBC + run all benchmarks
set -e

echo "=== STEP 1: Load LDBC SF1 nodes ==="
PYTHONUNBUFFERED=1 python3 /tmp/ldbc_load_xgdb.py 2>&1 | tee /tmp/full_run_load_nodes.log

echo ""
echo "=== STEP 2: Load LDBC SF1 edges (GID bulk) ==="
PYTHONUNBUFFERED=1 python3 /tmp/ldbc_load_edges_fast.py 2>&1 | tee /tmp/full_run_load_edges.log

echo ""
echo "=== STEP 3: Run official benchmarks ==="
PYTHONUNBUFFERED=1 python3 /tmp/official_bench.py 2>&1

echo ""
echo "=== ALL DONE ==="
