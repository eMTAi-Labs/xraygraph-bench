use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use xraybench_stats::{
    bca_mean_ci, descriptive, detect_outliers, exact_percentile, exact_percentiles, mann_whitney_u,
    BootstrapConfig,
};

/// Compute the p-th percentile (p in [0,1]) of a list of values.
#[pyfunction]
pub fn percentile(mut values: Vec<f64>, p: f64) -> PyResult<f64> {
    exact_percentile(&mut values, p).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Compute multiple percentiles in a single pass. ps must each be in [0,1].
#[pyfunction]
pub fn percentiles(mut values: Vec<f64>, ps: Vec<f64>) -> PyResult<Vec<f64>> {
    exact_percentiles(&mut values, &ps).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Compute descriptive statistics (count, mean, min, max, variance, stddev).
#[pyfunction]
#[pyo3(name = "descriptive")]
pub fn descriptive_stats(py: Python<'_>, values: Vec<f64>) -> PyResult<PyObject> {
    let s = descriptive(&values).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("count", s.count)?;
    dict.set_item("mean", s.mean)?;
    dict.set_item("min", s.min)?;
    dict.set_item("max", s.max)?;
    dict.set_item("variance", s.variance)?;
    dict.set_item("stddev", s.stddev)?;
    Ok(dict.into())
}

/// Compute a BCa bootstrap confidence interval for the mean.
#[pyfunction]
#[pyo3(signature = (values, confidence=0.95, n_resamples=10000, seed=42))]
pub fn bootstrap_ci(
    py: Python<'_>,
    values: Vec<f64>,
    confidence: f64,
    n_resamples: usize,
    seed: u64,
) -> PyResult<PyObject> {
    let config = BootstrapConfig {
        confidence,
        n_resamples,
        seed,
    };
    let ci = bca_mean_ci(&values, config).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("lower", ci.lower)?;
    dict.set_item("upper", ci.upper)?;
    dict.set_item("confidence", ci.confidence)?;
    dict.set_item("point_estimate", ci.point_estimate)?;
    dict.set_item("bias", ci.bias)?;
    dict.set_item("acceleration", ci.acceleration)?;
    Ok(dict.into())
}

/// Detect outliers using the Modified Z-Score method.
#[pyfunction]
#[pyo3(name = "detect_outliers", signature = (values, threshold=3.5))]
pub fn detect_outliers_py(py: Python<'_>, values: Vec<f64>, threshold: f64) -> PyResult<PyObject> {
    let result =
        detect_outliers(&values, threshold).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("outlier_indices", result.outlier_indices)?;
    dict.set_item("median", result.median)?;
    dict.set_item("mad", result.mad)?;
    dict.set_item("threshold", result.threshold)?;
    Ok(dict.into())
}

/// Run a two-sided Mann-Whitney U test.
#[pyfunction]
pub fn mann_whitney(py: Python<'_>, sample_a: Vec<f64>, sample_b: Vec<f64>) -> PyResult<PyObject> {
    let result =
        mann_whitney_u(&sample_a, &sample_b).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("u_a", result.u_a)?;
    dict.set_item("u_b", result.u_b)?;
    dict.set_item("p_value", result.p_value)?;
    dict.set_item("significant", result.significant)?;
    Ok(dict.into())
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(percentile, m)?)?;
    m.add_function(wrap_pyfunction!(percentiles, m)?)?;
    m.add_function(wrap_pyfunction!(descriptive_stats, m)?)?;
    m.add_function(wrap_pyfunction!(bootstrap_ci, m)?)?;
    m.add_function(wrap_pyfunction!(detect_outliers_py, m)?)?;
    m.add_function(wrap_pyfunction!(mann_whitney, m)?)?;
    Ok(())
}
