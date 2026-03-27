# Benchmark philosophy

## Why execution engines matter

A graph database query passes through multiple stages: parsing, planning,
optimization, and execution. The execution stage is where performance
differences between engines become concrete. Two engines can parse the same
Cypher query identically but execute it with fundamentally different
strategies:

- **Iterator-based execution** processes one row at a time through a pipeline
  of operators. It is simple to implement but leaves vectorization
  opportunities on the table.
- **Compiled execution** translates the query plan into native code (or a
  low-level intermediate representation). First-run cost is higher, but
  repeated execution can be significantly faster.
- **Vectorized execution** processes batches of rows through each operator,
  improving cache utilization and enabling SIMD operations.

Most benchmarks report a single latency number that blends these differences
together. xraygraph-bench separates them.

## Why cold and warm must be separated

A compiled engine pays a real cost on the first execution of a query: parsing,
planning, code generation, and potentially JIT compilation. Subsequent
executions may hit a plan cache and skip this work entirely.

Reporting only warm-run numbers hides compile cost. Reporting only cold-run
numbers penalizes engines that amortize well. Both measurements are necessary,
and they answer different questions:

- **Cold run:** What does a user experience the first time they run a query?
- **Warm run:** What does a production workload look like after caches are
  populated?

xraygraph-bench requires both, and reports them as separate fields.

## Why correctness is non-negotiable

A benchmark result without correctness validation is meaningless. An engine
that returns wrong results faster has not won anything.

Every benchmark in this suite defines a correctness oracle: a specification
of what the correct output looks like. This can be an exact row count, a
specific set of values, a checksum, or a structural property of the result.

The runner validates correctness before recording timing results. A
correctness failure voids the benchmark run.

## Why compile time is real work

Some graph databases compile queries to native code or bytecode. This
compilation step has measurable cost:

- Memory allocation for generated code
- CPU time for optimization passes
- Cache pressure from code generation

Benchmarks that do not measure compile time allow engines to hide significant
overhead. xraygraph-bench reports `compile_ms` as a separate metric.

## Why fallbacks must be visible

Compiled engines sometimes encounter query patterns they cannot compile
efficiently. The engine may fall back to an interpreted execution path or
deoptimize from compiled code. These fallbacks are important to measure
because they represent worst-case behavior in production.

xraygraph-bench tracks three related fields:

- `cache_hit` -- whether the query plan was served from cache
- `fallback` -- whether the engine fell back to an alternate execution path
- `deopt` -- whether a compiled plan was deoptimized during execution

## Why this suite distinguishes benchmark families

Different aspects of engine behavior require different workloads to expose:

- **core-executor** benchmarks isolate the execution engine from graph
  traversal. They measure scan, filter, projection, aggregation, and sort
  on flat or nearly-flat data.
- **graph-breakers** benchmarks stress the parts of execution that break
  streaming behavior: high-fanout expansion, BFS frontier growth, and
  multi-hop traversal where intermediate cardinality explodes.
- **end-to-end** benchmarks use realistic graph shapes and query patterns
  drawn from production use cases like code dependency analysis, lineage
  tracing, and impact analysis.
- **public-compare** benchmarks use conservative, portable workloads that
  can be run fairly across different engines without favoring any particular
  execution model.
- **compile** benchmarks specifically target compilation and caching behavior:
  cold vs warm, repeated query stability, and mixed workload cache churn.

This separation allows users to identify which engine behaviors matter for
their workload and focus measurement accordingly.
