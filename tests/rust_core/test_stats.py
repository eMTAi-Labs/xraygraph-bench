import xraybench_core


def test_percentile_median():
    values = list(range(1, 101))
    p50 = xraybench_core.stats.percentile(values, 0.5)
    assert abs(p50 - 50.5) < 0.01


def test_percentiles_batch():
    values = list(range(1, 101))
    result = xraybench_core.stats.percentiles(values, [0.5, 0.95, 0.99])
    assert len(result) == 3
    assert abs(result[0] - 50.5) < 0.01


def test_descriptive():
    values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    d = xraybench_core.stats.descriptive(values)
    assert d["count"] == 8
    assert abs(d["mean"] - 5.0) < 0.01


def test_bootstrap_ci():
    values = list(range(1, 101))
    ci = xraybench_core.stats.bootstrap_ci([float(v) for v in values])
    assert ci["lower"] < ci["point_estimate"]
    assert ci["upper"] > ci["point_estimate"]
    assert ci["confidence"] == 0.95


def test_detect_outliers():
    values = [10.0] * 100 + [1000000.0]
    result = xraybench_core.stats.detect_outliers(values)
    assert 100 in result["outlier_indices"]


def test_mann_whitney_identical():
    a = [float(v) for v in range(100)]
    b = [float(v) for v in range(100)]
    result = xraybench_core.stats.mann_whitney(a, b)
    assert not result["significant"]


def test_mann_whitney_different():
    a = [float(v) for v in range(100)]
    b = [float(v) for v in range(1000, 1100)]
    result = xraybench_core.stats.mann_whitney(a, b)
    assert result["significant"]
    assert result["p_value"] < 0.001
