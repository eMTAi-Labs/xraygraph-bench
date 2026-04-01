use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;

/// Calibrate the clock and return a dict with resolution/overhead metrics.
#[pyfunction]
pub fn calibrate(py: Python<'_>) -> PyResult<PyObject> {
    let result = xraybench_timing::calibrate()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("clock_resolution_ns", result.clock_resolution_ns)?;
    dict.set_item("clock_overhead_ns", result.clock_overhead_ns)?;
    dict.set_item("fence_overhead_ns", result.fence_overhead_ns)?;
    dict.set_item("samples", result.samples)?;
    Ok(dict.into())
}

/// Return the current monotonic timestamp in nanoseconds.
#[pyfunction]
pub fn monotonic_ns() -> PyResult<u64> {
    xraybench_timing::monotonic_ns()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calibrate, m)?)?;
    m.add_function(wrap_pyfunction!(monotonic_ns, m)?)?;
    Ok(())
}
