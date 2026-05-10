# Friendster Edge Insertion Degradation Report — 2026-04-20

## Summary

Edge insertion rate via xrayProtocol BULK_INSERT_EDGES (GID fast path) degrades **continuously** as the graph grows, dropping from 10,662 edges/sec at 10M edges to 6,998 edges/sec at 170M edges — a **34% throughput loss** with no plateau in sight. At the current trajectory, loading 1.8B edges would take **65+ hours** instead of the expected ~75 minutes.

## Rate Degradation Curve

| Edges Loaded | Rate (edges/sec) | Drop from Start | ETA (min) |
|-------------|------------------|-----------------|-----------|
| 10M | 10,662 | baseline | 2,808 |
| 20M | 10,233 | -4% | 2,909 |
| 30M | 9,997 | -6% | 2,961 |
| 40M | 9,888 | -7% | 2,977 |
| 50M | 9,724 | -9% | 3,010 |
| 60M | 9,475 | -11% | 3,071 |
| 70M | 9,188 | -14% | 3,149 |
| 80M | 8,885 | -17% | 3,238 |
| 90M | 8,575 | -20% | 3,335 |
| 100M | 8,270 | -22% | 3,438 |
| 110M | 7,992 | -25% | 3,537 |
| 120M | 7,773 | -27% | 3,615 |
| 130M | 7,577 | -29% | 3,687 |
| 140M | 7,398 | -31% | 3,753 |
| 150M | 7,244 | -32% | 3,810 |
| 160M | 7,118 | -33% | 3,854 |
| 170M | 6,998 | -34% | 3,896 |

The degradation is linear — roughly **220 edges/sec slower per 10M edges added**. If extrapolated:
- At 500M edges: ~3,700/s
- At 1B edges: ~1,200/s
- At 1.8B edges: effectively stalled

## Comparison to Previous Runs

| Dataset | Edge Rate | Notes |
|---------|-----------|-------|
| **LDBC SF1 (4.5M edges)** | **407,733/s** | Same GID fast path, same mmap engine |
| **LDBC SF10 (88M edges)** | **177,115/s** | Slower but still fast |
| **Friendster at 10M** | **10,662/s** | Already 40x slower than SF1 |
| **Friendster at 170M** | **6,998/s** | Degrading further |

The Friendster rate starts 40x slower than SF1 even at the same edge count. This suggests the 65.6M vertex count (not the edge count) is the primary factor — adjacency list lookups on 65.6M vertices are more expensive than on 3.18M vertices.

## mmap File State at 788M Edges

```
adj_in.mmap:   454 GB  ← HUGE — this is the bottleneck
adj_out.mmap:  9.0 GB
edges.mmap:    36  GB
vertices.mmap: 4.0 GB
props.mmap:    9.0 GB
string_pool:   256 MB
```

**`adj_in.mmap` is 454 GB** — this is the incoming adjacency list store. It's 50x larger than `adj_out.mmap` (9GB). This extreme asymmetry suggests:

1. **The Friendster edge list is being loaded as directed edges** — each `src → dst` edge creates one entry in `adj_out` for src and one in `adj_in` for dst.

2. **`adj_in` is fragmented** — with 65.6M vertices each receiving random incoming edges, the incoming adjacency lists are scattered across the 454GB file. Each edge insertion requires seeking to a random position in a 454GB mmap'd file.

3. **Page fault storm** — at 454GB, the `adj_in.mmap` file far exceeds the 187GB server RAM. Every adjacency list update triggers a page fault, evicting a cold page from the page cache to make room. With random access across 454GB, the working set never fits in cache.

## I/O Stats During Loading

```
Device   r/s     rkB/s     w/s     wkB/s   %util
md2     849.58  61,720   616.13   26,759   11.75%
```

- **850 reads/sec, 62 MB/s read** — constant page fault reads from the 454GB adj_in.mmap
- **616 writes/sec, 27 MB/s write** — dirty page writebacks
- **Only 11.75% disk utilization** — the disk isn't saturated, but the latency per random read is the bottleneck

The I/O pattern is **random 4KB reads across a 454GB file** — the worst case for any storage system. NVMe can do ~500K random 4KB reads/sec at queue depth 32, but the single-threaded edge insertion only generates ~850 reads/sec because each edge insertion waits for the page fault to resolve before proceeding to the next edge.

## Root Cause

The edge insertion for each edge does:
1. Look up src vertex → `adj_out` adjacency list → append dst_gid (sequential, fast)
2. Look up dst vertex → `adj_in` adjacency list → append src_gid (**random seek in 454GB file, slow**)

The `adj_in` updates are the bottleneck. Each destination vertex's incoming adjacency list is at a random offset in the 454GB file. With 65.6M vertices, the adjacency list base offsets are spread across the entire file, causing constant page cache misses.

## Recommendations

### Short-term: Batch and sort edges by destination before insertion
Pre-sort the edge list by destination vertex ID so that `adj_in` updates are sequential rather than random. This converts random I/O to sequential I/O — ~100x faster on NVMe.

```
# Sort by destination (column 2)
sort -t$'\t' -k2 -n com-friendster.ungraph.txt > friendster_sorted_by_dst.txt
```

Then load in two passes:
1. Pass 1: sorted by source → builds `adj_out` sequentially
2. Pass 2: sorted by destination → builds `adj_in` sequentially

### Medium-term: Use the BULK_IMPORT_FILE (0x2B) path
The server-side CSR builder reads the edge file in a single pass and builds the adjacency structure in memory before flushing to mmap. This avoids the random I/O entirely because it builds the complete adjacency arrays before writing.

### Long-term: Asynchronous adjacency list batching
Buffer incoming adjacency updates in memory and flush them in sorted order periodically. This converts random writes to sequential writes at the cost of memory — exactly what LSM-tree databases do for random key-value writes.

## Impact

- **Friendster 1.8B edges at current rate: 65+ hours** (not viable for benchmarking)
- **LDBC SF1 (4.5M edges): 11 seconds** — no degradation issue at this scale
- **LDBC SF10 (88M edges): 8 minutes** — manageable but starting to slow

The degradation only matters at billion-edge scale on mmap. In-memory engine doesn't have this issue (no page faults), but Friendster doesn't fit in 187GB RAM.
