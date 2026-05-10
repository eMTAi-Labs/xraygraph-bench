# Code graph datasets

Graphs shaped like source code: functions, files, classes, and their
relationships.

## Graph structure

Code graphs model the static structure of a codebase:

- **Nodes:**
  - `Function` -- individual functions/methods with name, complexity, line count
  - `File` -- source files with path and language
  - `Class` -- classes/modules containing functions

- **Edges:**
  - `CALLS` -- function-to-function call relationships
  - `DEFINED_IN` -- function defined in file
  - `CONTAINS` -- class contains function
  - `IMPORTS` -- file imports from file

## Characteristics

Real code graphs have specific structural properties that differ from
random graphs:

- **Skewed degree distribution:** Most functions have few callers; a small
  number of utility functions are called by hundreds or thousands of others.
- **Locality:** Functions in the same file or module tend to call each other
  more frequently than functions in distant modules.
- **Layering:** Code tends to have a layered dependency structure where
  lower layers are more heavily depended upon.
- **Small world:** Most functions are reachable from each other within a
  small number of hops.

## Synthetic generation

The `code_graph` generator in `tools/xraybench/generators/synthetic.py`
produces synthetic code graphs with these properties. Parameters control:
- File, class, and function counts
- Average calls per function
- Average imports per file
- Locality bias

## Real-world options

For benchmarks requiring real code graph data:

- **GitHub dependency graphs** via libraries.io open data
- **GH Archive** for repository-level dependency relationships
- **Custom extraction** from open-source codebases using static analysis

Real-world code graph ingestion is documented separately as it requires
external tooling.
