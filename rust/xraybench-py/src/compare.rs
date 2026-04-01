use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use xraybench_stats::regression::compare_metric as rs_compare_metric;

/// Compare a metric between two measurement vectors.
///
/// Returns a dict with all MetricComparison fields plus classification as a string.
#[pyfunction]
#[pyo3(signature = (name, values_a, values_b, significance_threshold=0.05))]
pub fn compare_metric(
    py: Python<'_>,
    name: String,
    values_a: Vec<f64>,
    values_b: Vec<f64>,
    significance_threshold: f64,
) -> PyResult<PyObject> {
    let result = rs_compare_metric(&name, &values_a, &values_b, significance_threshold)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("metric_name", &result.metric_name)?;
    dict.set_item("value_a", result.value_a)?;
    dict.set_item("value_b", result.value_b)?;
    dict.set_item("absolute_delta", result.absolute_delta)?;
    dict.set_item("percentage_change", result.percentage_change)?;
    dict.set_item("ci_lower", result.ci_lower)?;
    dict.set_item("ci_upper", result.ci_upper)?;
    dict.set_item("p_value", result.p_value)?;
    dict.set_item("significant", result.significant)?;
    dict.set_item("classification", format!("{:?}", result.classification))?;
    Ok(dict.into())
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compare_metric, m)?)?;
    Ok(())
}
