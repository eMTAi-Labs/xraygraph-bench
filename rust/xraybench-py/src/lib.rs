// PyO3's PyResult<T> is a type alias for Result<T, PyErr>. Clippy incorrectly
// flags the PyErr position as a "useless conversion" because PyErr implements
// Into<PyErr>. This is a known false-positive with PyO3; suppress it crate-wide.
#![allow(clippy::useless_conversion)]

use pyo3::prelude::*;

pub mod checksum;
pub mod compare;
pub mod generators;
pub mod stats;
pub mod timing;

#[pymodule]
fn xraybench_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Expose version
    m.add("__version__", xraybench_types::VERSION)?;

    // Register submodules
    let timing_mod = PyModule::new_bound(m.py(), "timing")?;
    timing::register(&timing_mod)?;
    m.add_submodule(&timing_mod)?;

    let stats_mod = PyModule::new_bound(m.py(), "stats")?;
    stats::register(&stats_mod)?;
    m.add_submodule(&stats_mod)?;

    let generators_mod = PyModule::new_bound(m.py(), "generators")?;
    generators::register(&generators_mod)?;
    m.add_submodule(&generators_mod)?;

    let checksum_mod = PyModule::new_bound(m.py(), "checksum")?;
    checksum::register(&checksum_mod)?;
    m.add_submodule(&checksum_mod)?;

    let compare_mod = PyModule::new_bound(m.py(), "compare")?;
    compare::register(&compare_mod)?;
    m.add_submodule(&compare_mod)?;

    Ok(())
}
