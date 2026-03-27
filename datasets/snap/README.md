# SNAP datasets

Integration with the Stanford Network Analysis Platform (SNAP) real-world
graph datasets.

**Source:** https://snap.stanford.edu/data/

## Recommended datasets

### soc-LiveJournal1

- **Nodes:** 4,847,571
- **Edges:** 68,993,773
- **Type:** directed social network
- **URL:** https://snap.stanford.edu/data/soc-LiveJournal1.html
- **Format:** edge list (tab-separated)
- **Size:** ~1.1 GB uncompressed

LiveJournal is a free online community where users declare friendship
relationships. The graph is directed (user A can list user B as a friend
without reciprocation).

**Benchmark relevance:** Large-scale traversal, BFS frontier growth,
community detection, reachability queries.

### web-Google

- **Nodes:** 875,713
- **Edges:** 5,105,039
- **Type:** directed web graph
- **URL:** https://snap.stanford.edu/data/web-Google.html
- **Format:** edge list (tab-separated)
- **Size:** ~75 MB uncompressed

Nodes represent web pages; edges represent hyperlinks between them. Released
by Google as part of a 2002 programming contest.

**Benchmark relevance:** Web graph traversal, PageRank-style computations,
reachability on sparse directed graphs.

### com-Friendster

- **Nodes:** 65,608,366
- **Edges:** 1,806,067,135
- **Type:** undirected social network
- **Size:** ~30 GB uncompressed

**Note:** This dataset is very large and intended for stress testing at
scale. Not recommended for initial benchmarking.

## Download and ingestion

### Manual download

```bash
# soc-LiveJournal1
wget https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
gunzip soc-LiveJournal1.txt.gz

# web-Google
wget https://snap.stanford.edu/data/web-Google.txt.gz
gunzip web-Google.txt.gz
```

### Using the benchmark runner

```bash
xraybench dataset download snap/soc-LiveJournal1
xraybench dataset ingest snap/soc-LiveJournal1 --engine xraygraphdb
```

The runner downloads to `datasets/snap/data/`, verifies checksums, and
converts the edge list to Cypher CREATE statements for ingestion.

### Edge list format

SNAP datasets use a tab-separated edge list format:

```
# Comment lines start with #
# FromNodeId    ToNodeId
0   1
0   2
0   3
```

The ingestion pipeline:
1. Downloads and decompresses the file
2. Parses the edge list, skipping comment lines
3. Creates node entries for all unique node IDs
4. Generates CREATE statements for edges
5. Loads via the engine adapter's `load_dataset()` method

## Licensing

SNAP datasets are provided for research use. Check individual dataset pages
for specific licensing terms. Most SNAP datasets are freely available for
research and benchmarking.

## Citation

> J. Leskovec and A. Krevl. SNAP Datasets: Stanford Large Network Dataset
> Collection. http://snap.stanford.edu/data, June 2014.
