import xraybench_core


def test_calibrate_returns_dict():
    cal = xraybench_core.timing.calibrate()
    assert isinstance(cal, dict)
    assert "clock_resolution_ns" in cal
    assert "clock_overhead_ns" in cal
    assert "fence_overhead_ns" in cal
    assert "samples" in cal


def test_calibrate_values_positive():
    cal = xraybench_core.timing.calibrate()
    assert cal["clock_resolution_ns"] > 0
    assert cal["samples"] > 0


def test_monotonic_ns_returns_int():
    t = xraybench_core.timing.monotonic_ns()
    assert isinstance(t, int)
    assert t > 0


def test_monotonic_ns_is_monotonic():
    t1 = xraybench_core.timing.monotonic_ns()
    t2 = xraybench_core.timing.monotonic_ns()
    assert t2 >= t1
