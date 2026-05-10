# mmap Engine SIGSEGV Crash Report — 2026-04-19

## Summary

xrayGraphDB mmap engine crashes with SIGSEGV during Bolt UNWIND data loading at approximately **2.58M total vertices**. The crash occurs during Post node insertion, after successfully loading Person (10K), Forum (90K), and Comment (2.05M) nodes.

## Server Environment

- **Server:** S2 — 187GB RAM, 44-core Xeon Gold 6152, Ubuntu 24.04, kernel 6.8.0-51-generic
- **Disk:** 3.5TB NVMe (3.2TB free)
- **Binary:** xrayGraphDB v4.9.3 (latest with BFS planner fix)
- **Config:** `--storage-engine=mmap --storage-properties-on-edges=true --bolt-port=7687`
- **vm.max_map_count:** 1,048,576 (default, should be sufficient)

## Reproduction Steps

```bash
# 1. Clean data directory
rm -rf /var/lib/xraygraphdb/*

# 2. Start in mmap mode
systemctl start xraygraphdb  # starts fine, port 7687 opens

# 3. Load LDBC SF1 nodes via Bolt UNWIND (batch size 1000)
# Person: 9,892 rows → OK (3,281 rows/s)
# Forum: 90,492 rows → OK (9,819 rows/s)
# Comment: 2,052,169 rows → OK (5,187 rows/s) ← NEW: this used to crash at ~900K
# Post: loading starts, reaches some rows, then → SIGSEGV
```

## Crash Details

```
status=11/SEGV (signal=SEGV)
```

- **Signal:** SIGSEGV (segmentation fault — null pointer dereference or invalid memory access)
- **Crash point:** During Post node insertion via Bolt UNWIND, after ~2.58M vertices committed
- **Crash loop:** After SIGSEGV, systemd auto-restarts → immediate SIGSEGV on startup (corrupted mmap state)
- **Recovery:** Only possible by wiping `/var/lib/xraygraphdb/*` and reloading

## State at Crash Time

From `meta.json` written at crash:

```json
{
  "vertex_count": 2577432,         ← 2.58M vertices committed
  "vertices_committed": 164955648, ← 157MB vertex data (in 256MB vertices.mmap)
  "props_committed": 1017965592,   ← 971MB property data (971MB props.mmap)
  "string_pool_committed": 264112929, ← 252MB strings (in 256MB string_pool.mmap)
  "edge_count": 0,                 ← no edges loaded yet (crash during node phase)
  "adj_in_committed": 0,
  "adj_out_committed": 0,
  "edges_committed": 0,
  "wal_offset": 221666438,         ← 211MB WAL
  "wal_segment_id": 1
}
```

## mmap File Sizes at Crash

| File | Size | Committed | Utilization |
|------|------|-----------|-------------|
| `vertices.mmap` | **256MB** | 157MB (61%) | ← approaching capacity |
| `props.mmap` | **971MB** | 971MB (100%) | ← **FULL** |
| `string_pool.mmap` | **256MB** | 252MB (98%) | ← **nearly FULL** |
| `adj_in.mmap` | 256MB | 0 | unused (no edges yet) |
| `adj_out.mmap` | 256MB | 0 | unused |
| `edges.mmap` | 256MB | 0 | unused |
| `wal/segment_000001.wal` | 219MB | 211MB | |

## Root Cause Analysis

**The crash correlates with `string_pool.mmap` and `props.mmap` reaching capacity:**

1. `string_pool.mmap` is 252MB of 256MB (98% full) — one more batch of string properties would exceed it
2. `props.mmap` is 971MB — already grew past the initial 256MB allocation (was remapped)
3. `vertices.mmap` is 157MB of 256MB (61% full) — still has headroom

**Likely scenario:** The mmap engine pre-allocates files at 256MB and grows them via `mremap()` or `ftruncate()` + re-mmap when they fill up. The SIGSEGV happens when:
- `string_pool.mmap` fills to 256MB and needs to grow
- The `mremap()` call returns a different virtual address
- Existing pointers into the old mapping become dangling → SIGSEGV

**Evidence:** `props.mmap` already grew from 256MB to 971MB (successfully remapped 3x), but `string_pool.mmap` stayed at 256MB. This suggests the string pool remap codepath has a bug where it doesn't update all references to the new mapping address.

## Previous Crash History

| Date | Version | Crash Point | Signal | Notes |
|------|---------|-------------|--------|-------|
| 2026-04-16 | v4.9.2 | ~900K vertices | SIGABRT | Original mmap crash, different signal |
| 2026-04-19 (attempt 1) | v4.9.3 | ~2.15M vertices | SIGABRT | First fix moved threshold up |
| 2026-04-19 (attempt 2) | v4.9.3 | 0 vertices | SIGSEGV | Clean start on corrupted data dir |
| 2026-04-19 (attempt 3) | v4.9.3+ | **2.58M vertices** | **SIGSEGV** | Latest fix — Comment survived, Post crashed |

**Progress:** The crash threshold has moved from 900K → 2.15M → 2.58M vertices across fixes. Each fix extends the range but doesn't fully solve the growth issue.

## What Needs to Be Fixed

1. **String pool mmap growth:** When `string_pool.mmap` exceeds its initial 256MB allocation, the remap must update ALL pointers/offsets that reference the string pool memory region. Check for any raw pointers into the string pool that aren't updated after `mremap()`.

2. **Crash-loop on restart with corrupted state:** After a SIGSEGV during write, the data files are left in an inconsistent state. On restart, the engine tries to open these files and immediately segfaults again. The engine should either:
   - Validate mmap file integrity on startup and repair/skip corrupted segments
   - Or use WAL replay to reconstruct a consistent state (the 211MB WAL should have enough data)

3. **Consider using relative offsets instead of raw pointers:** If the mmap files are accessed via absolute pointers (`char*` into the mapped region), any `mremap()` that moves the base address invalidates all existing pointers. Using offsets from the base (`uint64_t offset` + `base_ptr + offset` at access time) would eliminate this class of bugs entirely.

## Impact on Benchmarks

- **LDBC SF1 (3.18M nodes):** Cannot load in mmap — crashes at 2.58M
- **LDBC SF10 (~30M nodes):** Cannot load in mmap — would crash much earlier proportionally
- **Friendster (65.6M nodes):** Cannot load in mmap via Bolt — needs BULK_IMPORT_FILE (CSR path)
- **In-memory engine:** Works fine for SF1 (91GB) but SF10 and Friendster exceed 187GB server RAM

**This is the single blocker preventing the full benchmark matrix from being completed.**

## Test When Fixed

```bash
# Wipe and reload
rm -rf /var/lib/xraygraphdb/*
systemctl restart xraygraphdb

# Load LDBC SF1 (3.18M nodes, 17.2M edges) via Bolt
python3 scripts/competitor_benchmark.py \
  --engine memgraph --host localhost --port 7687 \
  --ldbc-dir /opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter \
  --output /opt/xraybench-results/xraygraphdb-matrix/mmap_sf1/

# If SF1 passes, load SF10 (10x larger)
python3 scripts/competitor_benchmark.py \
  --engine memgraph --host localhost --port 7687 \
  --ldbc-dir /opt/ldbc-snb/sf10/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter \
  --output /opt/xraybench-results/xraygraphdb-matrix/mmap_sf10/
```
