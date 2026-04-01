use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use xraybench_types::{BenchError, Edge, Result};

// ── Binary I/O ────────────────────────────────────────────────────────────────

/// Write edges as u64 pairs in little-endian binary format (16 bytes per edge).
pub fn write_edges_binary(edges: &[Edge], path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let mut writer = BufWriter::new(file);
    for edge in edges {
        writer
            .write_all(&edge.source.to_le_bytes())
            .map_err(|e| BenchError::IoError(e.to_string()))?;
        writer
            .write_all(&edge.target.to_le_bytes())
            .map_err(|e| BenchError::IoError(e.to_string()))?;
    }
    writer
        .flush()
        .map_err(|e| BenchError::IoError(e.to_string()))?;
    Ok(())
}

/// Read edges from little-endian binary format. File must be a multiple of 16 bytes.
pub fn read_edges_binary(path: &Path) -> Result<Vec<Edge>> {
    let mut file = File::open(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| BenchError::IoError(e.to_string()))?;

    if buf.len() % 16 != 0 {
        return Err(BenchError::InvalidData(format!(
            "binary edge file size {} is not a multiple of 16",
            buf.len()
        )));
    }

    let mut edges = Vec::with_capacity(buf.len() / 16);
    for chunk in buf.chunks_exact(16) {
        let source = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let target = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        edges.push(Edge { source, target });
    }
    Ok(edges)
}

// ── CSV I/O ───────────────────────────────────────────────────────────────────

/// Write edges as CSV with "source,target" header.
pub fn write_edges_csv(edges: &[Edge], path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "source,target").map_err(|e| BenchError::IoError(e.to_string()))?;
    for edge in edges {
        writeln!(writer, "{},{}", edge.source, edge.target)
            .map_err(|e| BenchError::IoError(e.to_string()))?;
    }
    writer
        .flush()
        .map_err(|e| BenchError::IoError(e.to_string()))?;
    Ok(())
}

/// Read edges from CSV format, skipping the "source,target" header line.
pub fn read_edges_csv(path: &Path) -> Result<Vec<Edge>> {
    let file = File::open(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let reader = BufReader::new(file);
    let mut edges = Vec::new();

    for (i, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| BenchError::IoError(e.to_string()))?;
        // Skip header line that starts with "source"
        if i == 0 && line.starts_with("source") {
            continue;
        }
        let mut parts = line.splitn(2, ',');
        let src_str = parts
            .next()
            .ok_or_else(|| BenchError::InvalidData("missing source".to_string()))?;
        let tgt_str = parts
            .next()
            .ok_or_else(|| BenchError::InvalidData("missing target".to_string()))?;
        let source = src_str
            .trim()
            .parse::<u64>()
            .map_err(|e| BenchError::InvalidData(e.to_string()))?;
        let target = tgt_str
            .trim()
            .parse::<u64>()
            .map_err(|e| BenchError::InvalidData(e.to_string()))?;
        edges.push(Edge { source, target });
    }
    Ok(edges)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join("xraybench_test").join(name)
    }

    fn setup_temp_dir() {
        let dir = std::env::temp_dir().join("xraybench_test");
        fs::create_dir_all(&dir).ok();
    }

    #[test]
    fn binary_roundtrip() {
        setup_temp_dir();
        let path = temp_path("binary_roundtrip.bin");
        let edges = vec![
            Edge {
                source: 0,
                target: 1,
            },
            Edge {
                source: 2,
                target: 3,
            },
            Edge {
                source: 100,
                target: 200,
            },
        ];
        write_edges_binary(&edges, &path).expect("write binary");
        let read_back = read_edges_binary(&path).expect("read binary");
        assert_eq!(edges, read_back);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn csv_roundtrip() {
        setup_temp_dir();
        let path = temp_path("csv_roundtrip.csv");
        let edges = vec![
            Edge {
                source: 0,
                target: 1,
            },
            Edge {
                source: 5,
                target: 10,
            },
            Edge {
                source: 999,
                target: 1000,
            },
        ];
        write_edges_csv(&edges, &path).expect("write csv");
        let read_back = read_edges_csv(&path).expect("read csv");
        assert_eq!(edges, read_back);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn binary_invalid_size() {
        setup_temp_dir();
        let path = temp_path("binary_invalid.bin");
        // Write 15 bytes — not a multiple of 16
        fs::write(&path, [0u8; 15]).expect("write invalid binary");
        let result = read_edges_binary(&path);
        assert!(result.is_err(), "should fail on 15-byte file");
        fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_edge_list() {
        setup_temp_dir();
        let bin_path = temp_path("empty.bin");
        let csv_path = temp_path("empty.csv");
        let edges: Vec<Edge> = vec![];

        write_edges_binary(&edges, &bin_path).expect("write empty binary");
        let read_back = read_edges_binary(&bin_path).expect("read empty binary");
        assert!(read_back.is_empty());

        write_edges_csv(&edges, &csv_path).expect("write empty csv");
        let read_back_csv = read_edges_csv(&csv_path).expect("read empty csv");
        assert!(read_back_csv.is_empty());

        fs::remove_file(&bin_path).ok();
        fs::remove_file(&csv_path).ok();
    }
}
