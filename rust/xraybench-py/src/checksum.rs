use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use xraybench_types::PropertyValue;
use xraybench_checksum::{hash_result_set, verify_hash, float_eq_ulp};

/// Convert a Python value to a PropertyValue.
/// Important: check bool BEFORE int because Python bool is an int subclass.
pub fn py_to_property(val: &Bound<'_, PyAny>) -> PyResult<PropertyValue> {
    if val.is_none() {
        return Ok(PropertyValue::Null);
    }
    // bool MUST be checked before int
    if let Ok(b) = val.extract::<bool>() {
        return Ok(PropertyValue::Boolean(b));
    }
    if let Ok(i) = val.extract::<i64>() {
        return Ok(PropertyValue::Integer(i));
    }
    if let Ok(f) = val.extract::<f64>() {
        return Ok(PropertyValue::Float(f));
    }
    if let Ok(s) = val.extract::<String>() {
        return Ok(PropertyValue::Text(s));
    }
    Err(PyValueError::new_err(format!(
        "unsupported Python type for PropertyValue conversion: {}",
        val.get_type().name()?
    )))
}

fn py_rows_to_rust(
    rows: Vec<Vec<Bound<'_, PyAny>>>,
) -> PyResult<Vec<Vec<PropertyValue>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(py_to_property)
                .collect::<PyResult<Vec<_>>>()
        })
        .collect()
}

/// Hash a result set (list of rows, where each row is a list of Python values).
#[pyfunction]
#[pyo3(name = "hash_result_set")]
pub fn hash_result_set_py(rows: Vec<Vec<Bound<'_, PyAny>>>) -> PyResult<String> {
    let rust_rows = py_rows_to_rust(rows)?;
    Ok(hash_result_set(&rust_rows))
}

/// Verify a result set against a reference hash.
#[pyfunction]
#[pyo3(name = "verify_hash")]
pub fn verify_hash_py(rows: Vec<Vec<Bound<'_, PyAny>>>, reference: String) -> PyResult<bool> {
    let rust_rows = py_rows_to_rust(rows)?;
    verify_hash(&rust_rows, &reference)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Compare two floats within max_ulp units in the last place.
#[pyfunction]
#[pyo3(name = "float_eq_ulp")]
pub fn float_eq_ulp_py(a: f64, b: f64, max_ulp: u32) -> bool {
    float_eq_ulp(a, b, max_ulp)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hash_result_set_py, m)?)?;
    m.add_function(wrap_pyfunction!(verify_hash_py, m)?)?;
    m.add_function(wrap_pyfunction!(float_eq_ulp_py, m)?)?;
    Ok(())
}
