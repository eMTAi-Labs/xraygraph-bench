use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;
use xraybench_generators::{
    estimate_node_count as rs_estimate_node_count, generate_chain as rs_generate_chain,
    generate_deep_traversal as rs_generate_deep_traversal,
    generate_hub_graph as rs_generate_hub_graph, generate_power_law,
    write_edges_binary as rs_write_edges_binary, write_edges_csv as rs_write_edges_csv,
};
use xraybench_types::Edge;

fn edges_to_tuples(edges: Vec<Edge>) -> Vec<(u64, u64)> {
    edges.into_iter().map(|e| (e.source, e.target)).collect()
}

fn tuples_to_edges(tuples: Vec<(u64, u64)>) -> Vec<Edge> {
    tuples
        .into_iter()
        .map(|(s, t)| Edge {
            source: s,
            target: t,
        })
        .collect()
}

/// Generate a deep traversal graph. Returns (node_count, edges).
#[pyfunction]
pub fn generate_deep_traversal(
    num_roots: u64,
    fanout_per_level: Vec<u32>,
    seed: u64,
) -> PyResult<(u64, Vec<(u64, u64)>)> {
    let (node_count, edges) = rs_generate_deep_traversal(num_roots, &fanout_per_level, seed);
    Ok((node_count, edges_to_tuples(edges)))
}

/// Generate a Barabasi-Albert power-law edge list.
#[pyfunction]
pub fn generate_power_law_edges(node_count: u64, m: u32, seed: u64) -> PyResult<Vec<(u64, u64)>> {
    let edges = generate_power_law(node_count, m, seed);
    Ok(edges_to_tuples(edges))
}

/// Generate a hub-and-spoke graph. Returns (node_count, edges).
#[pyfunction]
pub fn generate_hub_graph(
    hub_count: u64,
    spokes_per_hub: u64,
    seed: u64,
) -> PyResult<(u64, Vec<(u64, u64)>)> {
    let (node_count, edges) = rs_generate_hub_graph(hub_count, spokes_per_hub, seed);
    Ok((node_count, edges_to_tuples(edges)))
}

/// Generate a simple directed chain of the given length.
#[pyfunction]
pub fn generate_chain(length: u64, seed: u64) -> PyResult<Vec<(u64, u64)>> {
    let edges = rs_generate_chain(length, seed);
    Ok(edges_to_tuples(edges))
}

/// Estimate the node count for a deep traversal graph without generating edges.
#[pyfunction]
pub fn estimate_node_count(num_roots: u64, fanout_per_level: Vec<u32>) -> PyResult<u64> {
    Ok(rs_estimate_node_count(num_roots, &fanout_per_level))
}

/// Write edges (as (u64,u64) tuples) to a binary file.
#[pyfunction]
pub fn write_edges_binary(edges: Vec<(u64, u64)>, path: String) -> PyResult<()> {
    let edge_structs = tuples_to_edges(edges);
    rs_write_edges_binary(&edge_structs, Path::new(&path))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Write edges (as (u64,u64) tuples) to a CSV file.
#[pyfunction]
pub fn write_edges_csv(edges: Vec<(u64, u64)>, path: String) -> PyResult<()> {
    let edge_structs = tuples_to_edges(edges);
    rs_write_edges_csv(&edge_structs, Path::new(&path))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_deep_traversal, m)?)?;
    m.add_function(wrap_pyfunction!(generate_power_law_edges, m)?)?;
    m.add_function(wrap_pyfunction!(generate_hub_graph, m)?)?;
    m.add_function(wrap_pyfunction!(generate_chain, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_node_count, m)?)?;
    m.add_function(wrap_pyfunction!(write_edges_binary, m)?)?;
    m.add_function(wrap_pyfunction!(write_edges_csv, m)?)?;
    Ok(())
}
