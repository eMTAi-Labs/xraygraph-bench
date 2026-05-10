# Benchmark Server Setup Guide

**Last updated:** 2026-04-16
**Target OS:** Ubuntu 24.04 LTS
**Target DB:** xrayGraphDB v4.9.2+

---

## 1. Safety Rails (DO FIRST — non-negotiable)

xrayGraphDB is an in-memory database. Without limits, it will eat all RAM,
trigger the OOM killer, and can destroy the filesystem via core dumps.

```bash
# Create systemd override
mkdir -p /etc/systemd/system/xraygraphdb.service.d
cat > /etc/systemd/system/xraygraphdb.service.d/safety.conf << 'EOF'
[Service]
MemoryMax=20G
LimitCORE=0
EOF
systemctl daemon-reload
```

```bash
# Add swap (8GB minimum for a 32GB server)
fallocate -l 8G /swapfile8g
chmod 600 /swapfile8g
mkswap /swapfile8g
swapon /swapfile8g
echo '/swapfile8g none swap sw 0 0' >> /etc/fstab
```

**Why:** On 2026-04-16, running benchmarks without these limits caused:
- xrayGraphDB ate 29GB+ RAM → OOM killer fired
- Core dumps (20GB+ each) filled the 878GB disk
- On reboot, fsck found unrecoverable orphaned inodes
- Server had to be rebuilt from scratch on a new OS

---

## 2. Install Prerequisites

```bash
# Python 3.12 (comes with Ubuntu 24.04)
apt-get update
apt-get install -y python3-venv python3-dev

# Rust (for xraybench_core PyO3 module)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

# zstd (for LDBC data decompression)
apt-get install -y zstd
```

---

## 3. Install xraybench

```bash
# Clone
cd /opt
git clone https://github.com/eMTAi-Labs/xraygraph-bench.git

# Virtualenv
python3 -m venv /opt/xraybench-env
source /opt/xraybench-env/bin/activate
pip install --upgrade pip

# Build Rust core (timing, stats, generators)
cd /opt/xraygraph-bench/rust/xraybench-py
source ~/.cargo/env
maturin develop --release

# Install Python package with neo4j driver
cd /opt/xraygraph-bench
pip install -e '.[neo4j]'

# Verify
xraybench --version
```

---

## 4. xrayGraphDB GLIBC Notes

- **Ubuntu 24.04** has GLIBC 2.39 — no compatibility issues.
- **Rocky Linux 9 / RHEL 9** has GLIBC 2.34 — xrayGraphDB's bundled
  libstdc++.so.6 requires GLIBC 2.38 and **poisons the system**.
  Fix: remove `/etc/ld.so.conf.d/xraygraphdb.conf`, run `ldconfig`,
  move bundled libstdc++ to `.bundled`. System libstdc++ is sufficient.

---

## 5. Known Benchmark Harness Issues (Fixed)

### 5.1 Edge data UNWIND syntax

**File:** `tools/xraybench/adapters/dataset_loader.py`

Python's `str([{"s": 0, "t": 1}])` produces `[{'s': 0, 't': 1}]` with
single-quoted keys. xrayGraphDB's Cypher parser requires unquoted keys
in map literals: `[{s: 0, t: 1}]`.

**Fix:** Use f-string Cypher map syntax instead of Python str():
```python
pairs_cypher = "[" + ", ".join(f"{{s: {s}, t: {t}}}" for s, t in batch) + "]"
```

### 5.2 Dataset generation not connected to runner

**File:** `tools/xraybench/runner.py`

The runner's `load_dataset()` call cleared the graph but never generated
synthetic data. The `_generate_synthetic_data()` method was added to:
- Create flat nodes inline via UNWIND for `uniform_nodes`, `flat_nodes`, etc.
- Generate edge lists via Rust generators for `power_law`, `hub`, `deep_traversal`

### 5.3 Query parameter derivation

**File:** `tools/xraybench/runner.py`

Query templates reference `$threshold`, `$category` etc. but spec parameters
are `selectivity`, `row_count` etc. The `_derive_query_params()` method:
- Computes `threshold = 1.0 - selectivity`
- Provides defaults for common unreferenced params
- Filters params to only those referenced in the query (prevents "parameter not provided" errors)

### 5.4 Cache clear commands

**File:** `tools/xraybench/adapters/xraygraphdb.py`

The adapter tried `FREE MEMORY` and `mg.clear_cache()` — Memgraph commands.
xrayGraphDB uses `xray.clear_cache()` (if available) with graceful fallback.

### 5.5 COUNT query must preserve LIMIT

The correctness checker wraps the benchmark query in a COUNT to get the
true row count (since Bolt PULL batching caps fetched rows). The regex
must only replace the RETURN projection, NOT strip LIMIT or ORDER BY.

**Wrong:** `RETURN n.id LIMIT 1000` → `RETURN count(*) AS __cnt` (LIMIT gone, counts all rows)
**Right:** `RETURN n.id LIMIT 1000` → `RETURN count(*) AS __cnt LIMIT 1000` (LIMIT preserved)

Without this, every query with LIMIT gets `correctness_mismatch` because
COUNT returns the full unLIMITed count vs the oracle's LIMITed expectation.

### 5.6 Row count accuracy

**File:** `tools/xraybench/runner.py`, `tools/xraybench/models.py`

The neo4j Bolt driver pulls results in batches of 1000, so `len(rows)` caps
at the PULL batch size. For correctness validation, a separate COUNT query
runs after the timed benchmark. The `ExecuteResult.row_count` property was
made settable to support this.

### 5.6 Dataset size capping

**File:** `tools/xraybench/runner.py`

On a 32GB server, loading 1M nodes per benchmark causes OOM after 2-3 runs.
The `--param row_count=100000` CLI override now applies to dataset generation
(even for specs that don't define `row_count` in their parameters section).

---

## 6. Running the Benchmark Suite

```bash
source /opt/xraybench-env/bin/activate

# Clear DB data for a fresh start
rm -rf /var/lib/xraygraphdb/*
systemctl restart xraygraphdb
sleep 5

# Run the suite (capped at 100K nodes per benchmark)
./run_all_benchmarks.sh localhost 7687
```

The batch script:
- Cleans the database between benchmarks (prevents memory accumulation)
- Uses `--param row_count=100000` to cap dataset size
- Skips benchmarks needing unavailable features (GFQL, vector search, etc.)
- Times out at 300 seconds per benchmark

---

## 7. xrayGraphDB Self-Documentation

xrayGraphDB v4.9.2 has 385+ built-in functions across 30 categories.
Query them:

```cypher
CALL xg.builtin_functions() YIELD name, category
RETURN category, name ORDER BY category, name;
```

Categories include: Aggregation, Aviation, Bitwise, DateTime, GIS,
GraphAnalytics, Hash, Math, ParticlePhysics, Physics, RAG_LLM,
ReactiveEngine, String, UnitConversion, XRayVision, and more.

---

## 8. LDBC SNB Data

Download from LDBC Council CDN or SURF repository:
- Format: `CsvCompositeMergeForeign-LongDateFormatter` (pipe-delimited)
- SF1: ~888MB uncompressed (9.8K persons, 1M posts, 2M comments)
- SF10: ~8.9GB uncompressed (65K persons, 7.4M posts, 21.8M comments)

Load via `LOAD CSV` (server-side, much faster than Bolt UNWIND):
```cypher
LOAD CSV WITH HEADERS FROM 'file:///var/lib/xraygraphdb/import/person.csv' AS row
FIELDTERMINATOR '|'
CREATE (:Person {id: toInteger(row.id), firstName: row.firstName, ...});
```

For large imports, use periodic transactions:
```cypher
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///import/large.csv' AS row
  CREATE (:Node {id: row.id, val: row.val})
} IN TRANSACTIONS OF 10000 ROWS;
```
