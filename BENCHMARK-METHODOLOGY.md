# xrayGraphDB vs cuGraph Benchmark Methodology

## Standard: Courtroom-Clean

This benchmark must survive scrutiny from NVIDIA engineers, graph researchers, database experts, HPC practitioners, and skeptical competitors. It is reproducible, adversarial, transparent, and falsifiable.

The goal is NOT to prove "xrayGraphDB wins everything." The goal is to prove **"xrayGraphDB's results are real."** If the methodology is trusted, the wins become credible automatically.

---

## 1. Never Hide Weaknesses

Publish wins, losses, crashes, limitations, semantic mismatches, unsupported algorithms, memory failures, OOM conditions, and timeout conditions. Do NOT silently omit anything.

If cuGraph wins: publish it.
If xray wins: publish it.

**Credibility > temporary optics.**

## 2. Same Hardware or It Does Not Count

Everything must run on the same machine, same GPU, same RAM, same storage, same NUMA topology, same OS, same driver version, same CUDA version, same thermal conditions.

Record and publish:
- CPU model, core count, frequency
- RAM size, type, channels
- GPU model, VRAM, PCIe generation
- SSD model
- Kernel version
- CUDA version
- Driver version
- Compiler version (for built-from-source systems)

## 3. Same Dataset Exactly

Dataset must be byte-identical between systems.

Publish:
- Download source URL
- SHA-256 checksums
- Preprocessing steps
- Deduplication rules
- Directed/undirected semantics
- Edge expansion rules (mirroring)
- Self-loop handling
- Renumbering rules

Clarify the difference between:
- Raw Friendster edges (1,806,067,135 undirected)
- Logical edges
- Physical stored edges (3,612,134,270 after mirroring)
- Bidirectional duplication

## 4. Same Semantics or the Test Is Invalid

**This is the most important rule.**

Algorithms must mean the same thing. If semantics differ, label the test **INVALID**. Do NOT pretend equivalence exists when it does not.

### Triangle Count
- Directed or undirected?
- Unique triangles or duplicate?
- Self loops?
- Multigraph handling?

### PageRank
- Damping factor
- Iteration count
- Tolerance
- Normalization
- Dangling node handling

### Betweenness Centrality
- Exact or approximate?
- Sample count
- Epsilon / delta
- Random seed
- Bucket count
- Normalization

### BFS
- Single-source?
- All reachable nodes?
- Output materialized?
- Depth returned?
- Frontier only?
- Warm cache?
- Traversal cutoff?

### K-Core
- Exact core decomposition?
- Induced subgraph returned?
- Directed semantics?

## 5. Warm vs Cold Must Be Explicit

Every result must be labeled:
- **cold**: first run after restart
- **warm**: subsequent run, data in memory
- **GPU warm**: GPU kernels initialized, data on device
- **filesystem warm**: OS page cache populated
- **memory-resident**: entire graph in RAM
- **mmap warm**: mmap pages faulted in

No ambiguity. If the graph is memory-resident, say so. If cache is warm, say so. If GPU kernels are already initialized, say so.

## 6. Separate Load Phases

Data loading is NOT one thing. Separate and publish:

1. CSV/parquet read time
2. Parsing time
3. Renumbering time
4. Graph construction time
5. CSR build time
6. Indexing time
7. GPU transfer time
8. Preprocessing time
9. Compression time
10. Persistence time

Otherwise people will accuse us of cheating.

## 7. Do Not Use Best-of-N

Never cherry-pick. Use:
- **Median** (primary)
- **p95** (tail latency)
- **Standard deviation**

Recommended: 10 runs, discard first warmup only IF documented, publish all raw numbers.

## 8. GPU Utilization Must Be Recorded

For every GPU test, record:
- GPU utilization %
- VRAM used (MB)
- PCIe transfer behavior
- Spill behavior (GPU → CPU fallback)
- CPU participation

If CPU participates materially: say so.
If GPU is idle: say so.

## 9. Correctness Before Performance

Performance without correctness is meaningless.

Every algorithm must have:
- Correctness verification
- Ground-truth validation (SNAP published values)
- Checksum or deterministic validation
- Comparison to reference outputs (cuGraph, NetworkX, SNAP)

Especially: connected components, triangle count, PageRank ordering, BC approximations.

## 10. Publish the Failures

This is CRITICAL. Publish:
- OOMs
- Crashes
- Unsupported workloads
- Timeouts
- Semantic incompatibilities

This INCREASES credibility. Do NOT hide them.

## 11. No Hand-Wavy Claims

Never say:
- "world's fastest"
- "best graph database"
- "constant time traversal"

Instead say:
- "In this benchmark configuration..."
- "On this dataset..."
- "Under these semantics..."
- "Observed traversal latency remained approximately flat..."

Precision matters.

## 12. Full Reproducibility or It Didn't Happen

Publish:
- Exact commands
- Scripts
- Environment variables
- Docker/native setup
- CUDA configs
- Build flags
- Compiler flags
- Benchmark harnesses
- Random seeds

Someone else must be able to reproduce it.

## 13. Include a "Known Limitations" Section

Mandatory. Include:
- Workloads xrayGraphDB is weak at
- Metadata count weakness
- Cypher planner limitations
- Transactional persistence limitations
- Unsupported semantics
- Algorithms still experimental

This dramatically increases trust.

## 14. Distinguish Database vs Graph-Compute Results

This benchmark compares:
- Graph databases (Neo4j, xrayGraphDB)
- Graph compute engines (cuGraph, Gunrock)
- GPU graph frameworks (GraphBLAS)

These are NOT identical categories. Be honest about category differences.

cuGraph is not a transactional graph database.
xrayGraphDB is not a pure GPU analytics library.

Explain that clearly.

## 15. The Most Important Rule

We are NOT trying to prove "xrayGraphDB wins everything."

We are trying to prove: **"xrayGraphDB's results are real."**

That is the standard. If the methodology is trusted, the wins become credible automatically.
