"""CLI entry point for xraygraph-bench.

Usage:
    xraybench list                           List available benchmarks
    xraybench validate <path>                Validate a benchmark spec or result
    xraybench run <path> --engine <name>     Run a benchmark
    xraybench load-test --engine <name>      Run a load test
    xraybench generate --generator <name> --name <name>  Generate a synthetic dataset
    xraybench verify-dataset --type <type> --name <name> Verify a dataset
    xraybench compare <result_a> <result_b>  Compare two benchmark result files
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import __version__
from .loader import discover_benchmarks
from .schema import validate_file


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="xraybench",
        description="xraygraph-bench benchmark runner",
    )
    parser.add_argument(
        "--version", action="version", version=f"xraybench {__version__}"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # list command
    list_parser = subparsers.add_parser("list", help="List available benchmarks")
    list_parser.add_argument(
        "--family", help="Filter by benchmark family"
    )
    list_parser.add_argument(
        "--tag", help="Filter by tag"
    )
    list_parser.add_argument(
        "--json", action="store_true", dest="json_output", help="Output as JSON"
    )

    # validate command
    validate_parser = subparsers.add_parser(
        "validate", help="Validate a benchmark spec, result, or dataset manifest"
    )
    validate_parser.add_argument("path", help="Path to the file to validate")
    validate_parser.add_argument(
        "--schema",
        choices=["benchmark", "result", "dataset"],
        default=None,
        help="Schema type to validate against (auto-detected if omitted)",
    )

    # run command
    run_parser = subparsers.add_parser("run", help="Run a benchmark")
    run_parser.add_argument("path", help="Path to the benchmark.yaml spec")
    run_parser.add_argument("--engine", required=True, help="Engine adapter name")
    run_parser.add_argument("--host", default="localhost", help="Engine host")
    run_parser.add_argument("--port", type=int, default=7687, help="Engine port")
    run_parser.add_argument("--username", default="", help="Engine username")
    run_parser.add_argument("--password", default="", help="Engine password")
    run_parser.add_argument("--database", default="", help="Database name")
    run_parser.add_argument("--output", help="Output file path for result JSON")
    run_parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Parameter override in key=value format (repeatable)",
    )

    # generate command
    gen_parser = subparsers.add_parser(
        "generate", help="Generate a synthetic benchmark dataset"
    )
    gen_parser.add_argument(
        "--generator",
        required=True,
        choices=["chain", "hub", "power_law", "deep_traversal"],
        help="Generator function to use",
    )
    gen_parser.add_argument(
        "--name", required=True, help="Dataset name (used as directory name)"
    )
    gen_parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Parameter in key=value format (repeatable)",
    )
    gen_parser.add_argument(
        "--data-dir",
        default="/data/xraybench",
        help="Override data directory (default: /data/xraybench)",
    )

    # verify-dataset command
    vd_parser = subparsers.add_parser(
        "verify-dataset", help="Verify the integrity of a dataset"
    )
    vd_parser.add_argument(
        "--type",
        required=True,
        choices=["synthetic", "snap", "ogb"],
        help="Dataset type",
        dest="dataset_type",
    )
    vd_parser.add_argument(
        "--name", required=True, help="Dataset name"
    )
    vd_parser.add_argument(
        "--data-dir",
        default="/data/xraybench",
        help="Override data directory (default: /data/xraybench)",
    )

    # export command
    export_parser = subparsers.add_parser(
        "export", help="Export benchmark results to CSV or Parquet"
    )
    export_parser.add_argument(
        "results_dir", help="Path to directory containing JSON result files"
    )
    export_parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        dest="export_format",
        help="Output format (default: csv)",
    )
    export_parser.add_argument(
        "--output",
        default=None,
        help="Output file path (default: results.csv or results.parquet)",
    )

    # report command
    report_parser = subparsers.add_parser(
        "report", help="Generate a static HTML report from result JSON files"
    )
    report_parser.add_argument(
        "results_dir", help="Path to directory containing JSON result files"
    )
    report_parser.add_argument(
        "--output",
        default="report.html",
        help="Output HTML file path (default: report.html)",
    )
    report_parser.add_argument(
        "--title",
        default="xraygraph-bench Report",
        help="Report title (default: 'xraygraph-bench Report')",
    )

    # compare command
    cmp_parser = subparsers.add_parser(
        "compare", help="Compare two benchmark result JSON files"
    )
    cmp_parser.add_argument("result_a", help="Path to first result JSON file (baseline)")
    cmp_parser.add_argument("result_b", help="Path to second result JSON file (candidate)")
    cmp_parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        dest="cmp_format",
        help="Output format (default: table)",
    )
    cmp_parser.add_argument(
        "--confidence",
        type=float,
        default=0.95,
        help="Statistical confidence level (default: 0.95)",
    )

    # dashboard command
    dash_parser = subparsers.add_parser(
        "dashboard", help="Start the interactive benchmark dashboard"
    )
    dash_parser.add_argument(
        "--results-dir",
        required=True,
        help="Path to directory containing benchmark result JSON files",
    )
    dash_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind the dashboard server (default: 0.0.0.0)",
    )
    dash_parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port for the dashboard server (default: 8080)",
    )

    # load-test command
    lt_parser = subparsers.add_parser("load-test", help="Run a load test")
    lt_parser.add_argument("--engine", required=True, help="Engine adapter name")
    lt_parser.add_argument(
        "--profile",
        choices=["throughput", "saturation", "mixed", "stability"],
        default="throughput",
        help="Load test profile",
    )
    lt_parser.add_argument("--clients", type=int, default=8, help="Concurrent clients")
    lt_parser.add_argument(
        "--duration", type=int, default=60, help="Test duration in seconds"
    )
    lt_parser.add_argument("--host", default="localhost", help="Engine host")
    lt_parser.add_argument("--port", type=int, default=7687, help="Engine port")
    lt_parser.add_argument("--username", default="", help="Engine username")
    lt_parser.add_argument("--password", default="", help="Engine password")
    lt_parser.add_argument("--query", default="RETURN 1", help="Query to load test")
    lt_parser.add_argument("--output", help="Output file path for result JSON")

    args = parser.parse_args(argv)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "list":
        return _cmd_list(args)
    elif args.command == "validate":
        return _cmd_validate(args)
    elif args.command == "run":
        return _cmd_run(args)
    elif args.command == "load-test":
        return _cmd_load_test(args)
    elif args.command == "generate":
        return _cmd_generate(args)
    elif args.command == "verify-dataset":
        return _cmd_verify_dataset(args)
    elif args.command == "export":
        return _cmd_export(args)
    elif args.command == "report":
        return _cmd_report(args)
    elif args.command == "compare":
        return _cmd_compare(args)
    elif args.command == "dashboard":
        return _cmd_dashboard(args)

    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    """List available benchmarks."""
    benchmarks = discover_benchmarks()

    if args.family:
        benchmarks = [b for b in benchmarks if b.get("family") == args.family]

    if args.tag:
        benchmarks = [b for b in benchmarks if args.tag in b.get("tags", [])]

    if not benchmarks:
        print("No benchmarks found.")
        return 0

    if args.json_output:
        print(json.dumps(benchmarks, indent=2))
    else:
        # Table output
        print(f"{'Name':<35} {'Family':<20} {'Version':<10} {'Path'}")
        print("-" * 100)
        for b in benchmarks:
            name = b.get("name", "?")
            family = b.get("family", "?")
            version = b.get("version", "?")
            path = b.get("path", "?")
            error = b.get("error")
            if error:
                print(f"{name:<35} {'ERROR':<20} {'':<10} {error}")
            else:
                print(f"{name:<35} {family:<20} {version:<10} {path}")

    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    """Validate a file against its schema."""
    path = Path(args.path)

    # Auto-detect schema type
    schema_type = args.schema
    if schema_type is None:
        if path.name == "benchmark.yaml" or path.name.endswith(".benchmark.yaml"):
            schema_type = "benchmark"
        elif path.suffix == ".json" and "result" in path.name:
            schema_type = "result"
        elif path.name == "manifest.yaml":
            schema_type = "dataset"
        elif path.suffix in (".yaml", ".yml"):
            schema_type = "benchmark"
        elif path.suffix == ".json":
            schema_type = "result"
        else:
            print(f"Cannot auto-detect schema type for: {path}")
            print("Use --schema to specify: benchmark, result, or dataset")
            return 1

    print(f"Validating {path} against {schema_type} schema...")
    errors = validate_file(path, schema_type)

    if errors:
        print(f"INVALID: {len(errors)} error(s) found:")
        for error in errors:
            print(f"  - {error}")
        return 1
    else:
        print("VALID")
        return 0


def _cmd_run(args: argparse.Namespace) -> int:
    """Run a benchmark."""
    from tools.xraybench.adapters import get_adapter
    from tools.xraybench.runner import BenchmarkRunner

    # Resolve adapter
    try:
        adapter_cls = get_adapter(args.engine)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    adapter = adapter_cls()

    # Build config
    config = {
        "engine": args.engine,
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "password": args.password,
    }
    if args.database:
        config["database"] = args.database

    # Parse parameter overrides
    overrides: dict[str, str] = {}
    for param in args.param:
        if "=" not in param:
            print(f"Invalid parameter format: {param} (expected key=value)")
            return 1
        key, value = param.split("=", 1)
        overrides[key] = value

    # Run
    runner = BenchmarkRunner(adapter, config)
    try:
        result = runner.run(args.path, overrides, args.output)
        print(json.dumps(result.to_dict(), indent=2))
        return 0
    except Exception as e:
        logging.getLogger(__name__).error("Benchmark failed: %s", e)
        return 1


def _cmd_load_test(args: argparse.Namespace) -> int:
    """Run a load test."""
    from tools.xraybench.adapters import get_adapter
    from tools.xraybench.load_test import LoadTestConfig, LoadTester

    try:
        adapter_cls = get_adapter(args.engine)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    adapter = adapter_cls()

    config = {
        "engine": args.engine,
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "password": args.password,
    }

    test_config = LoadTestConfig(
        profile=args.profile,
        clients=args.clients,
        duration_seconds=args.duration,
        query=args.query,
        engine_config=config,
    )

    tester = LoadTester(adapter, config)
    try:
        result = tester.run(test_config)
        bench_result = result.to_benchmark_result(
            benchmark_name=f"load-test-{args.profile}",
            engine=args.engine,
            engine_version=adapter.engine_version(),
            dataset="none",
        )
        output = bench_result.to_dict()
        output["load_test_detail"] = {
            "profile": result.profile,
            "total_queries": result.total_queries,
            "successful_queries": result.successful_queries,
            "failed_queries": result.failed_queries,
            "time_series": result.time_series,
        }

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(output, f, indent=2)
            print(f"Result written to: {output_path}")
        else:
            print(json.dumps(output, indent=2))

        return 0
    except Exception as e:
        logging.getLogger(__name__).error("Load test failed: %s", e)
        return 1


def _parse_param_value(value: str) -> "int | float | list[int] | str":
    """Auto-detect and convert a string value to an appropriate Python type.

    Handles:
    - ``[5,3]`` or ``[5, 3]``  -> list of ints
    - Integer strings           -> int
    - Float strings             -> float
    - Everything else           -> str
    """
    stripped = value.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        inner = stripped[1:-1]
        parts = [p.strip() for p in inner.split(",")]
        try:
            return [int(p) for p in parts]
        except ValueError:
            return stripped
    try:
        return int(stripped)
    except ValueError:
        pass
    try:
        return float(stripped)
    except ValueError:
        pass
    return stripped


def _cmd_generate(args: argparse.Namespace) -> int:
    """Generate a synthetic benchmark dataset."""
    from tools.xraybench.dataset_manager import DatasetManager

    # Parse --param key=value pairs
    params: dict[str, "int | float | list[int] | str"] = {}
    for item in args.param:
        if "=" not in item:
            print(f"Invalid --param format: {item!r} (expected key=value)")
            return 1
        key, raw_value = item.split("=", 1)
        params[key.strip()] = _parse_param_value(raw_value)

    manager = DatasetManager(data_dir=args.data_dir)
    try:
        manifest = manager.generate_synthetic(
            name=args.name,
            generator=args.generator,
            params=params,
        )
    except Exception as e:
        logging.getLogger(__name__).error("Generation failed: %s", e)
        return 1

    dataset_dir = manager.data_dir / "synthetic" / args.name
    print(f"Generated dataset: {args.name}")
    print(f"  Generator : {args.generator}")
    print(f"  Nodes     : {manifest['node_count']}")
    print(f"  Edges     : {manifest['edge_count']}")
    print(f"  Path      : {dataset_dir}")
    return 0


def _cmd_verify_dataset(args: argparse.Namespace) -> int:
    """Verify the integrity of a dataset."""
    from tools.xraybench.dataset_manager import DatasetManager

    manager = DatasetManager(data_dir=args.data_dir)
    result = manager.verify(dataset_type=args.dataset_type, name=args.name)

    if result["valid"]:
        print(f"VALID: {args.name} ({args.dataset_type})")
        return 0
    else:
        print(f"INVALID: {args.name} ({args.dataset_type})")
        for error in result["errors"]:
            print(f"  - {error}")
        return 1


def _cmd_export(args: argparse.Namespace) -> int:
    """Export benchmark results to CSV or Parquet."""
    from tools.xraybench.export import export_csv, export_parquet, load_results

    results_dir = Path(args.results_dir)
    if not results_dir.is_dir():
        print(f"Error: results directory not found: {results_dir}")
        return 1

    fmt = args.export_format
    output = args.output
    if output is None:
        output = f"results.{fmt}"
    output_path = Path(output)

    results = load_results(results_dir)
    if not results:
        print(f"No JSON result files found in: {results_dir}")
        return 1

    try:
        if fmt == "csv":
            count = export_csv(results, output_path)
        else:
            count = export_parquet(results, output_path)
    except ImportError as e:
        print(f"Error: {e}")
        return 1

    print(f"Exported {count} result(s) to {output_path} (format: {fmt})")
    return 0


def _cmd_report(args: argparse.Namespace) -> int:
    """Generate a static HTML report from result JSON files."""
    from .report import generate_report

    results_dir = Path(args.results_dir)
    if not results_dir.is_dir():
        print(f"Error: results directory not found: {results_dir}")
        return 1

    output_path = Path(args.output)
    count = generate_report(results_dir, output_path, title=args.title)

    if count == 0:
        print(f"No JSON result files found in: {results_dir}")
        print(f"Empty report written to: {output_path}")
    else:
        print(f"Report generated from {count} result(s): {output_path}")

    return 0


def _cmd_compare(args: argparse.Namespace) -> int:
    """Compare two benchmark result JSON files."""
    from .compare import compare_results, format_comparison_table, load_result

    try:
        result_a = load_result(args.result_a)
    except (OSError, ValueError) as e:
        print(f"Error loading {args.result_a}: {e}")
        return 1

    try:
        result_b = load_result(args.result_b)
    except (OSError, ValueError) as e:
        print(f"Error loading {args.result_b}: {e}")
        return 1

    comparison = compare_results(result_a, result_b, confidence=args.confidence)

    if args.cmp_format == "json":
        print(json.dumps(comparison, indent=2))
    else:
        print(format_comparison_table(comparison))

    return 0


def _cmd_dashboard(args: argparse.Namespace) -> int:
    """Start the interactive benchmark dashboard."""
    from .dashboard import run_dashboard

    print(f"Starting dashboard at http://{args.host}:{args.port}")
    print(f"Serving results from: {args.results_dir}")
    try:
        run_dashboard(args.results_dir, host=args.host, port=args.port)
    except ImportError as e:
        print(f"Error: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
