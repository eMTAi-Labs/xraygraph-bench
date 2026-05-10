# OGB datasets

Integration with the Open Graph Benchmark (OGB) datasets.

**Source:** https://ogb.stanford.edu/

## Recommended datasets

### ogbn-products

- **Nodes:** 2,449,029
- **Edges:** 61,859,140
- **Type:** undirected product co-purchasing network
- **Node features:** 100-dimensional feature vectors
- **Node labels:** 47 product categories
- **Size:** ~1.5 GB

An Amazon product co-purchasing network where nodes represent products and
edges indicate that two products are frequently bought together.

**Benchmark relevance:** Medium-scale graph traversal, community-like
structure, property-rich nodes (100-dim features test property access cost).

### ogbn-papers100M

- **Nodes:** 111,059,956
- **Edges:** 1,615,685,872
- **Type:** directed citation network
- **Node features:** 128-dimensional feature vectors
- **Node labels:** 172 subject areas
- **Size:** ~57 GB

A citation network extracted from Microsoft Academic Graph. Nodes are papers;
directed edges represent citations.

**Benchmark relevance:** Large-scale stress testing, citation chain
traversal, high-fanout nodes (heavily cited papers), property-heavy workloads.

**Note:** This dataset is very large. Use for stress testing and scale
evaluation, not for initial benchmarking.

## Download and ingestion

### Using the OGB Python package

```bash
pip install ogb

# Download datasets programmatically
python -c "from ogb.nodeproppred import NodePropPredDataset; d = NodePropPredDataset(name='ogbn-products')"
```

### Using the benchmark runner

```bash
xraybench dataset download ogb/ogbn-products
xraybench dataset ingest ogb/ogbn-products --engine xraygraphdb
```

The runner uses the OGB Python library to download and extract the dataset,
then converts the graph structure to Cypher CREATE statements for ingestion.

### Ingestion details

OGB datasets include:
- **Edge index:** pairs of (source, target) node IDs
- **Node features:** dense feature vectors (stored as node properties)
- **Node labels:** classification labels

The ingestion pipeline:
1. Downloads via the `ogb` Python package
2. Extracts edge index and node features
3. Creates nodes with ID, feature vector, and label properties
4. Creates directed edges from the edge index
5. Loads via the engine adapter

**Feature vector handling:** 100+ dimensional feature vectors are stored
as a serialized array property. This is intentional: it tests how engines
handle large property values during scans and projections.

## Licensing

OGB datasets are released under the MIT license for research use.

## Citation

> W. Hu, M. Fey, M. Zitnik, Y. Dong, H. Ren, B. Liu, M. Catasta,
> and J. Leskovec. Open Graph Benchmark: Datasets for Machine Learning
> on Graphs. NeurIPS 2020.
