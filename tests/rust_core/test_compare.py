import xraybench_core


def test_compare_identical():
    result = xraybench_core.compare.compare_metric(
        "cold_ms", [100.0] * 30, [100.0] * 30
    )
    assert result["metric_name"] == "cold_ms"
    assert not result["significant"]


def test_compare_improvement():
    result = xraybench_core.compare.compare_metric(
        "cold_ms", [100.0] * 30, [50.0] * 30
    )
    assert result["significant"]
    assert result["percentage_change"] < -40.0
    assert result["classification"] == "Improvement"


def test_compare_regression():
    result = xraybench_core.compare.compare_metric(
        "cold_ms", [50.0] * 30, [100.0] * 30
    )
    assert result["classification"] == "Regression"
