# Fix: Variable-Length Path DFS → BFS Optimization

## The Problem

```cypher
MATCH (p:Person {id: 933})-[:KNOWS*1..5]-(f) RETURN count(DISTINCT f)
```

Takes **390 seconds** on xrayGraphDB via Cypher/Bolt.
Takes **42ms** on Neo4j.
Takes **~1ms** on xrayGraphDB via native CSR BFS (xrayProtocol).

The native BFS engine already solves this in 1ms. The Cypher planner just doesn't use it.

## Root Cause

**File:** `src/query/plan/operator.cpp` lines 4096-4120

The `ExpandVariable::MakeCursor()` switch routes `*1..N` patterns based on `EdgeAtom::Type`:

```cpp
switch (type_) {
  case EdgeAtom::Type::BREADTH_FIRST:
    return existing_node ? STShortestPathCursor : SingleSourceShortestPathCursor;
  case EdgeAtom::Type::DEPTH_FIRST:
    return ExpandVariableCursor;  // <-- THIS IS THE PROBLEM
  ...
}
```

The Cypher parser sets `type_ = DEPTH_FIRST` for all `*1..N` patterns. This routes to `ExpandVariableCursor` which does **DFS path enumeration** — it explores every possible path through the graph, not just reachable nodes.

With person 933 (5 friends, avg degree 18):
- Hop 1: 5 paths
- Hop 2: ~90 paths
- Hop 3: ~1,600 paths
- Hop 4: ~29,000 paths
- Hop 5: **~500,000+ paths** enumerated → only **9,163 distinct nodes** reached

## The Fix

xrayGraphDB already has all the BFS infrastructure needed:

1. **`SingleSourceShortestPathCursor`** (operator.cpp lines 2843-3102) — standard BFS with visited set
2. **`VectorizedBFS`** (src/query/v4/vector/vectorized_bfs.hpp) — SIMD tiered BFS with bitset visited tracking, multi-threaded parallel expansion, and GPU dispatch

### Option A: Planner Rewrite Rule (Cleanest)

Add a rule in the planner/optimizer that detects this pattern:

```
ExpandVariable(DEPTH_FIRST, *1..N) → Aggregate(DISTINCT endpoint)
```

And rewrites it to:

```
ExpandVariable(BREADTH_FIRST, *1..N) → Aggregate(DISTINCT endpoint)
```

**Where to add it:** `src/query/plan/rewrite/` or wherever the existing plan optimization rules live.

**When it's safe:** When the query only cares about **which nodes are reachable**, not **which paths reach them**. This is true when:
- The endpoint variable is aggregated with `count(DISTINCT ...)`, `collect(DISTINCT ...)`, or just `DISTINCT`
- The edge variable from the `*1..N` pattern is NOT referenced in RETURN, WHERE, or WITH
- No path variable is captured (no `p = (a)-[*1..N]-(b)`)

### Option B: Route to VectorizedBFS (Fastest)

When the planner detects a `*1..N` pattern where only reachability matters (per conditions above), route directly to `VectorizedBFS` instead of any Volcano cursor:

```cpp
// In MakeCursor or plan compilation
if (type_ == EdgeAtom::Type::DEPTH_FIRST && can_use_bfs_optimization()) {
    // Use the tiered VectorizedBFS with bitset visited
    // Tier 1 (frontier < 10K): scalar with prefetch
    // Tier 2 (frontier >= 10K): multi-threaded parallel
    // Tier 3 (frontier > 100K): GPU dispatch
}
```

This would give Cypher queries the same ~1ms performance as the native xrayProtocol CSR BFS.

### Option C: Quick Fix — Change EdgeAtom::Type at Parse Time

In the parser/AST builder, when encountering `*1..N` without a path variable, set `type_ = BREADTH_FIRST` instead of `DEPTH_FIRST`:

**File:** `src/query/frontend/ast/ast.hpp` — `EdgeAtom` construction
**Or:** `src/query/frontend/semantic/symbol_generator.cpp` — where edge types are resolved

This is the quickest fix but least flexible.

## What Neo4j Does

Neo4j's planner detects the `count(DISTINCT endpoint)` pattern and:
1. Uses **BFS level-by-level expansion** with a visited set
2. Never enumerates paths — just tracks which nodes are reached at each level
3. Deduplicates at each level before expanding further
4. Result: O(V+E) instead of O(paths) — plateaus at 42ms for any depth

## Key Files

| File | What It Does |
|------|-------------|
| `src/query/plan/operator.hpp:1215-1295` | `ExpandVariable` class definition |
| `src/query/plan/operator.cpp:2372-2647` | `ExpandVariableCursor` — the DFS path enumerator (the slow path) |
| `src/query/plan/operator.cpp:2843-3102` | `SingleSourceShortestPathCursor` — BFS with visited set (already exists!) |
| `src/query/plan/operator.cpp:4096-4120` | `MakeCursor()` — the switch that routes to DFS vs BFS |
| `src/query/v4/vector/vectorized_bfs.hpp` | `VectorizedBFS` — SIMD tiered BFS (the ~1ms engine) |
| `src/query/v4/vector/vectorized_bfs.cpp` | Three-tier implementation: scalar → parallel → GPU |
| `src/query/frontend/ast/ast.hpp:1893-2010` | `EdgeAtom::Type` enum — DEPTH_FIRST vs BREADTH_FIRST |

## Expected Impact

| Hop | Current (DFS) | After Fix (BFS) | Speedup |
|-----|--------------|-----------------|---------|
| 4   | 4,000ms      | ~1-40ms         | 100-4,000x |
| 5   | 390,000ms    | ~1-40ms         | 10,000-390,000x |
| 6+  | TIMEOUT      | ~1-40ms         | ∞ |

This single fix would make xrayGraphDB via Cypher **faster than every competitor at every hop depth**, matching Neo4j's 42ms plateau or beating it with VectorizedBFS at ~1ms.
