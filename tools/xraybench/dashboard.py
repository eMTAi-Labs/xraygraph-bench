"""Interactive benchmark dashboard — FastAPI + HTMX."""

import json
from pathlib import Path
from typing import Any

HTMX_CDN = "https://unpkg.com/htmx.org@2.0.4"
PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


def create_app(results_dir: str | Path) -> Any:
    """Create the FastAPI dashboard application.

    Args:
        results_dir: Directory containing benchmark result JSON files.

    Returns:
        FastAPI application instance.
    """
    try:
        from fastapi import FastAPI, Query
        from fastapi.responses import HTMLResponse
    except ImportError:
        raise ImportError(
            "FastAPI is required for the dashboard. "
            "Run: pip install 'fastapi[standard]'"
        )

    results_path = Path(results_dir)
    app = FastAPI(title="xraygraph-bench Dashboard")

    def _load_results() -> list[dict[str, Any]]:
        results = []
        if results_path.exists():
            for path in sorted(results_path.glob("*.json")):
                with open(path) as f:
                    data = json.load(f)
                    data["_file"] = path.name
                    results.append(data)
        return results

    @app.get("/", response_class=HTMLResponse)
    async def index(
        engine: str | None = Query(None),
        family: str | None = Query(None),
        benchmark: str | None = Query(None),
    ) -> HTMLResponse:
        results = _load_results()

        # Extract filter options
        engines = sorted(set(r.get("engine", "unknown") for r in results))
        families = sorted(set(r.get("query_shape", "unknown") for r in results))
        benchmarks = sorted(set(r.get("benchmark", "unknown") for r in results))

        # Apply filters
        filtered = results
        if engine:
            filtered = [r for r in filtered if r.get("engine") == engine]
        if benchmark:
            filtered = [r for r in filtered if r.get("benchmark") == benchmark]

        # Build chart data
        chart_labels = [
            f"{r.get('benchmark', '?')} ({r.get('engine', '?')})" for r in filtered
        ]
        cold_values = [r.get("cold_ms", 0) for r in filtered]
        warm_values = [r.get("warm_ms", 0) for r in filtered]

        return HTMLResponse(
            _render_page(
                title="xraygraph-bench Dashboard",
                results=filtered,
                engines=engines,
                benchmarks=benchmarks,
                chart_labels=chart_labels,
                cold_values=cold_values,
                warm_values=warm_values,
                selected_engine=engine,
                selected_benchmark=benchmark,
            )
        )

    @app.get("/api/results", response_class=HTMLResponse)
    async def api_results(
        engine: str | None = Query(None),
        benchmark: str | None = Query(None),
    ) -> HTMLResponse:
        """HTMX endpoint — returns partial HTML for results table."""
        results = _load_results()
        if engine:
            results = [r for r in results if r.get("engine") == engine]
        if benchmark:
            results = [r for r in results if r.get("benchmark") == benchmark]
        return HTMLResponse(_render_results_table(results))

    @app.get("/api/result/{filename}", response_class=HTMLResponse)
    async def api_result_detail(filename: str) -> HTMLResponse:
        """HTMX endpoint — returns detail view for a single result."""
        path = results_path / filename
        if not path.exists():
            return HTMLResponse("<p>Result not found.</p>")
        with open(path) as f:
            result = json.load(f)
        return HTMLResponse(_render_result_detail(result))

    return app


def _render_page(
    title: str,
    results: list[dict[str, Any]],
    engines: list[str],
    benchmarks: list[str],
    chart_labels: list[str],
    cold_values: list[float],
    warm_values: list[float],
    selected_engine: str | None = None,
    selected_benchmark: str | None = None,
) -> str:
    engine_options = "".join(
        f'<option value="{e}" {"selected" if e == selected_engine else ""}>{e}</option>'
        for e in engines
    )
    benchmark_options = "".join(
        f'<option value="{b}" {"selected" if b == selected_benchmark else ""}>{b}</option>'
        for b in benchmarks
    )

    table_html = _render_results_table(results)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<script src="{HTMX_CDN}"></script>
<script src="{PLOTLY_CDN}"></script>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }}
h1 {{ color: #212529; border-bottom: 2px solid #dee2e6; padding-bottom: 10px; }}
.filters {{ display: flex; gap: 10px; margin: 15px 0; align-items: center; }}
select {{ padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px; }}
table {{ border-collapse: collapse; width: 100%; background: white; }}
th, td {{ border: 1px solid #dee2e6; padding: 8px 12px; text-align: left; }}
th {{ background: #e9ecef; }}
tr:hover {{ background: #f1f3f5; cursor: pointer; }}
.chart {{ width: 100%; height: 400px; margin: 20px 0; }}
.detail {{ background: white; padding: 15px; border-radius: 6px; margin: 10px 0; box-shadow: 0 1px 3px rgba(0,0,0,.1); }}
.stat {{ display: inline-block; padding: 10px 20px; margin: 5px; background: white; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,.1); text-align: center; }}
.stat-value {{ font-size: 24px; font-weight: bold; color: #0d6efd; }}
.stat-label {{ font-size: 11px; color: #6c757d; text-transform: uppercase; }}
footer {{ margin-top: 40px; color: #6c757d; font-size: 12px; }}
</style>
</head>
<body>
<h1>{title}</h1>

<div>
<div class="stat"><div class="stat-value">{len(results)}</div><div class="stat-label">Results</div></div>
<div class="stat"><div class="stat-value">{len(engines)}</div><div class="stat-label">Engines</div></div>
<div class="stat"><div class="stat-value">{len(benchmarks)}</div><div class="stat-label">Benchmarks</div></div>
</div>

<div class="filters">
<label>Engine:</label>
<select hx-get="/api/results" hx-target="#results-table" hx-include="[name='benchmark']" name="engine">
<option value="">All</option>
{engine_options}
</select>
<label>Benchmark:</label>
<select hx-get="/api/results" hx-target="#results-table" hx-include="[name='engine']" name="benchmark">
<option value="">All</option>
{benchmark_options}
</select>
</div>

<div id="main-chart" class="chart"></div>

<h2>Results</h2>
<div id="results-table">
{table_html}
</div>

<div id="detail-view"></div>

<footer>xraygraph-bench dashboard</footer>

<script>
Plotly.newPlot('main-chart', [
    {{x: {json.dumps(chart_labels)}, y: {json.dumps(cold_values)}, name: 'Cold (ms)', type: 'bar'}},
    {{x: {json.dumps(chart_labels)}, y: {json.dumps(warm_values)}, name: 'Warm (ms)', type: 'bar'}}
], {{title: 'Benchmark Results', barmode: 'group', margin: {{t: 40, b: 100}}}});
</script>
</body>
</html>"""


def _render_results_table(results: list[dict[str, Any]]) -> str:
    if not results:
        return "<p>No results match the current filters.</p>"

    rows = []
    for r in results:
        correctness = r.get("correctness", {})
        passed = "PASS" if correctness.get("passed", False) else "FAIL"
        filename = r.get("_file", "")
        rows.append(
            f"<tr hx-get='/api/result/{filename}' hx-target='#detail-view' hx-swap='innerHTML'>"
            f"<td>{r.get('benchmark', '?')}</td>"
            f"<td>{r.get('engine', '?')}</td>"
            f"<td>{r.get('cold_ms', 0):.2f}</td>"
            f"<td>{r.get('warm_ms', 0):.2f}</td>"
            f"<td>{r.get('compile_ms', 'N/A')}</td>"
            f"<td>{r.get('rows_out', '?')}</td>"
            f"<td>{passed}</td>"
            f"</tr>"
        )

    return f"""<table>
<thead><tr><th>Benchmark</th><th>Engine</th><th>Cold (ms)</th><th>Warm (ms)</th><th>Compile (ms)</th><th>Rows</th><th>Correct</th></tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>"""


def _render_result_detail(result: dict[str, Any]) -> str:
    host = result.get("host", {})
    return f"""<div class="detail">
<h3>{result.get('benchmark', '?')} — {result.get('engine', '?')}</h3>
<table>
<tr><th>Cold (ms)</th><td>{result.get('cold_ms', 'N/A')}</td></tr>
<tr><th>Warm (ms)</th><td>{result.get('warm_ms', 'N/A')}</td></tr>
<tr><th>Compile (ms)</th><td>{result.get('compile_ms', 'N/A')}</td></tr>
<tr><th>Rows Out</th><td>{result.get('rows_out', 'N/A')}</td></tr>
<tr><th>Timestamp</th><td>{result.get('timestamp', 'N/A')}</td></tr>
<tr><th>Host</th><td>{host.get('os', '?')} / {host.get('cpu', '?')} / {host.get('cores', '?')} cores</td></tr>
</table>
<details><summary>Raw JSON</summary><pre>{json.dumps(result, indent=2)}</pre></details>
</div>"""


def run_dashboard(
    results_dir: str | Path, host: str = "0.0.0.0", port: int = 8080
) -> None:
    """Start the dashboard server."""
    import uvicorn

    app = create_app(results_dir)
    uvicorn.run(app, host=host, port=port)
