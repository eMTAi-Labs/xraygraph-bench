"""CLI entry point for xraygraph-bench.

Usage:
    xraybench list                           List available benchmarks
    xraybench validate <path>                Validate a benchmark spec or result
    xraybench run <path> --engine <name>     Run a benchmark
    xraybench load-test --engine <name>      Run a load test
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import __version__
from .loader import discover_benchmarks, load_benchmark_spec
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
    from .adapters import get_adapter
    from .runner import BenchmarkRunner

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
    from .adapters import get_adapter
    from .load_test import LoadTestConfig, LoadTester

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


if __name__ == "__main__":
    sys.exit(main())
