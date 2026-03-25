//! Compression module for tables and indexes.
//!
//! Provides LZ4 and Zstd compression algorithms for storing
//! tuple data and index keys efficiently.
//!
//! # Compression Algorithm
//!
//! PostgreSQL uses TOAST (The Oversized-Attribute Storage Technique) for
//! compression. VaultGres follows a similar approach:
//!
//! - Threshold: 2KB - values larger than this are considered for compression
//! - Target: After compression, tuples should be around 2KB
//! - Algorithms: LZ4 (fast, low CPU) and Zstd (better ratio, more CPU)
//!
//! # Usage
//!
//! ```rust
//! use vaultgres::storage::compression::{compress, decompress, CompressionAlgorithm};
//!
//! let data = vec![0u8; 4000]; // 4KB of data
//! let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
//! let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4, data.len()).unwrap();
//! assert_eq!(data, decompressed);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub const COMPRESSION_THRESHOLD: usize = 2048;
pub const TOAST_TUPLE_TARGET: usize = 2048;
pub const COMPRESSED_HEADER_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    #[default]
    None,
    Lz4,
    Zstd,
}

impl CompressionAlgorithm {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "lz4" => Some(Self::Lz4),
            "zstd" => Some(Self::Zstd),
            "none" | "off" | "" => Some(Self::None),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Error)]
pub enum CompressionError {
    #[error("Compression failed: buffer too small")]
    BufferTooSmall,
    #[error("Decompression failed: invalid data")]
    InvalidData,
    #[error("Decompression failed: corrupted data (checksum mismatch)")]
    CorruptedData,
    #[error("Unsupported compression algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("IO error: {0}")]
    Io(String),
    #[error("Data too large to compress: {0} bytes")]
    DataTooLarge(usize),
    #[error("Compression expanded data beyond allowed limit")]
    CompressionIneffective,
}

pub type CompressionResult<T> = Result<T, CompressionError>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompressionStats {
    pub bytes_compressed: usize,
    pub bytes_uncompressed: usize,
    pub compression_count: usize,
    pub decompression_count: usize,
}

impl CompressionStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_uncompressed == 0 {
            return 1.0;
        }
        self.bytes_compressed as f64 / self.bytes_uncompressed as f64
    }

    pub fn compression_ratio_percentage(&self) -> String {
        format!("{:.2}%", self.compression_ratio() * 100.0)
    }
}

pub struct CompressionStatsCounter {
    stats: std::sync::atomic::AtomicUsize,
}

impl CompressionStatsCounter {
    pub fn new() -> Self {
        Self { stats: std::sync::atomic::AtomicUsize::new(0) }
    }

    pub fn increment(&self, amount: usize) {
        self.stats.fetch_add(amount, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get(&self) -> usize {
        self.stats.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for CompressionStatsCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct CompressionContext {
    algorithm: CompressionAlgorithm,
    stats: CompressionStats,
}

impl CompressionContext {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self { algorithm, stats: CompressionStats::new() }
    }

    pub fn algorithm(&self) -> CompressionAlgorithm {
        self.algorithm
    }

    pub fn stats(&self) -> &CompressionStats {
        &self.stats
    }

    pub fn compress(&mut self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        if data.len() < COMPRESSION_THRESHOLD {
            return Ok(data.to_vec());
        }

        let compressed = match self.algorithm {
            CompressionAlgorithm::Lz4 => compress_lz4(data)?,
            CompressionAlgorithm::Zstd => compress_zstd(data)?,
            CompressionAlgorithm::None => return Ok(data.to_vec()),
        };

        if compressed.len() >= data.len() {
            return Ok(data.to_vec());
        }

        self.stats.bytes_compressed += compressed.len();
        self.stats.bytes_uncompressed += data.len();
        self.stats.compression_count += 1;

        Ok(compressed)
    }

    pub fn decompress(&mut self, data: &[u8], original_size: usize) -> CompressionResult<Vec<u8>> {
        let decompressed = match self.algorithm {
            CompressionAlgorithm::Lz4 => decompress_lz4(data, original_size)?,
            CompressionAlgorithm::Zstd => decompress_zstd(data, original_size)?,
            CompressionAlgorithm::None => return Ok(data.to_vec()),
        };

        self.stats.decompression_count += 1;

        Ok(decompressed)
    }
}

impl Default for CompressionContext {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::None)
    }
}

pub fn compress(data: &[u8], algorithm: CompressionAlgorithm) -> CompressionResult<Vec<u8>> {
    if data.len() < COMPRESSION_THRESHOLD {
        return Ok(data.to_vec());
    }

    match algorithm {
        CompressionAlgorithm::Lz4 => compress_lz4(data),
        CompressionAlgorithm::Zstd => compress_zstd(data),
        CompressionAlgorithm::None => Ok(data.to_vec()),
    }
}

pub fn decompress(
    data: &[u8],
    algorithm: CompressionAlgorithm,
    original_size: usize,
) -> CompressionResult<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::Lz4 => decompress_lz4(data, original_size),
        CompressionAlgorithm::Zstd => decompress_zstd(data, original_size),
        CompressionAlgorithm::None => Ok(data.to_vec()),
    }
}

pub fn should_compress(data_len: usize) -> bool {
    data_len >= COMPRESSION_THRESHOLD
}

pub fn compress_lz4(data: &[u8]) -> CompressionResult<Vec<u8>> {
    let compressed =
        lz4::block::compress(data, None, true).map_err(|_| CompressionError::InvalidData)?;

    if compressed.len() >= data.len() {
        return Ok(data.to_vec());
    }

    Ok(compressed)
}

pub fn decompress_lz4(data: &[u8], _original_size: usize) -> CompressionResult<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let decompressed =
        lz4::block::decompress(data, None).map_err(|_| CompressionError::CorruptedData)?;
    Ok(decompressed)
}

pub fn compress_zstd(data: &[u8]) -> CompressionResult<Vec<u8>> {
    let mut compressed = Vec::with_capacity(data.len());
    let mut encoder = zstd::stream::Encoder::new(&mut compressed, 0)
        .map_err(|_| CompressionError::InvalidData)?;

    std::io::Write::write_all(&mut encoder, data).map_err(|_| CompressionError::InvalidData)?;
    encoder.finish().map_err(|_| CompressionError::InvalidData)?;

    if compressed.len() >= data.len() {
        return Ok(data.to_vec());
    }

    Ok(compressed)
}

pub fn decompress_zstd(data: &[u8], original_size: usize) -> CompressionResult<Vec<u8>> {
    let mut decompressed = Vec::with_capacity(original_size);

    let mut cursor = std::io::Cursor::new(data);
    let mut decoder =
        zstd::stream::Decoder::new(&mut cursor).map_err(|_| CompressionError::InvalidData)?;

    std::io::Read::read_to_end(&mut decoder, &mut decompressed)
        .map_err(|_| CompressionError::InvalidData)?;

    if decompressed.len() != original_size {
        return Err(CompressionError::CorruptedData);
    }

    Ok(decompressed)
}

pub struct CompressedData {
    pub algorithm: CompressionAlgorithm,
    pub original_size: usize,
    pub compressed_data: Vec<u8>,
}

impl CompressedData {
    pub fn new(
        algorithm: CompressionAlgorithm,
        original_size: usize,
        compressed_data: Vec<u8>,
    ) -> Self {
        Self { algorithm, original_size, compressed_data }
    }

    pub fn total_size(&self) -> usize {
        self.compressed_data.len()
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            return 1.0;
        }
        self.compressed_data.len() as f64 / self.original_size as f64
    }
}

pub fn compress_with_header(
    data: &[u8],
    algorithm: CompressionAlgorithm,
) -> CompressionResult<CompressedData> {
    if !should_compress(data.len()) {
        return Ok(CompressedData::new(CompressionAlgorithm::None, data.len(), data.to_vec()));
    }

    let compressed = compress(data, algorithm)?;

    if compressed.len() >= data.len() {
        return Ok(CompressedData::new(CompressionAlgorithm::None, data.len(), data.to_vec()));
    }

    Ok(CompressedData::new(algorithm, data.len(), compressed))
}

pub fn decompress_with_header(header: &[u8], data: &[u8]) -> CompressionResult<Vec<u8>> {
    if header.is_empty() {
        return Err(CompressionError::InvalidData);
    }

    let flags = header[0];
    let algorithm_bits = flags & 0x03;

    let algorithm = match algorithm_bits {
        0x00 => CompressionAlgorithm::None,
        0x01 => CompressionAlgorithm::Lz4,
        0x02 => CompressionAlgorithm::Zstd,
        _ => return Err(CompressionError::InvalidData),
    };

    let original_size = if header.len() >= 5 {
        u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize
    } else {
        return Err(CompressionError::InvalidData);
    };

    if algorithm == CompressionAlgorithm::None {
        return Ok(data.to_vec());
    }

    decompress(data, algorithm, original_size)
}

pub fn make_compressed_header(algorithm: CompressionAlgorithm, original_size: usize) -> [u8; 5] {
    let mut header = [0u8; 5];
    let algo_bits = match algorithm {
        CompressionAlgorithm::None => 0x00,
        CompressionAlgorithm::Lz4 => 0x01,
        CompressionAlgorithm::Zstd => 0x02,
    };
    header[0] = algo_bits;
    header[1..5].copy_from_slice(&(original_size as u32).to_le_bytes());
    header
}

pub fn parse_compressed_header(header: &[u8]) -> CompressionResult<(CompressionAlgorithm, usize)> {
    if header.len() < 5 {
        return Err(CompressionError::InvalidData);
    }

    let algo_bits = header[0] & 0x03;
    let algorithm = match algo_bits {
        0x00 => CompressionAlgorithm::None,
        0x01 => CompressionAlgorithm::Lz4,
        0x02 => CompressionAlgorithm::Zstd,
        _ => return Err(CompressionError::InvalidData),
    };

    let original_size = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

    Ok((algorithm, original_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_threshold() {
        let small_data = vec![0u8; 100];
        assert!(!should_compress(small_data.len()));

        let large_data = vec![0u8; 3000];
        assert!(should_compress(large_data.len()));
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let compressed = compress_lz4(&data).unwrap();
        let decompressed = decompress_lz4(&compressed, data.len()).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_lz4_compression_ratio() {
        let data: Vec<u8> = vec![0u8; 10000];

        let compressed = compress_lz4(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let compressed = compress_zstd(&data).unwrap();
        let decompressed = decompress_zstd(&compressed, data.len()).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_zstd_compression_ratio() {
        let data: Vec<u8> = vec![0u8; 10000];

        let compressed = compress_zstd(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_compress_small_data_not_compressed() {
        let data = vec![0u8; 100];

        let result = compress(&data, CompressionAlgorithm::Lz4).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_compress_no_compression() {
        let data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();

        let result = compress(&data, CompressionAlgorithm::None).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_compression_context() {
        let mut ctx = CompressionContext::new(CompressionAlgorithm::Lz4);
        let data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();

        let compressed = ctx.compress(&data).unwrap();
        let decompressed = ctx.decompress(&compressed, data.len()).unwrap();

        assert_eq!(data, decompressed);
        assert!(ctx.stats().compression_count > 0);
    }

    #[test]
    fn test_compression_stats() {
        let mut ctx = CompressionContext::new(CompressionAlgorithm::Lz4);
        let data: Vec<u8> = vec![0u8; 3000];

        ctx.compress(&data).unwrap();

        assert!(ctx.stats().bytes_compressed > 0);
        assert_eq!(ctx.stats().bytes_uncompressed, data.len());
        assert_eq!(ctx.stats().compression_count, 1);
    }

    #[test]
    fn test_compressed_data_struct() {
        let data: Vec<u8> = vec![0u8; 3000];

        let compressed = compress_with_header(&data, CompressionAlgorithm::Lz4).unwrap();

        assert_eq!(compressed.original_size, data.len());
        assert!(compressed.total_size() < data.len());
        assert_eq!(compressed.compression_ratio() < 1.0, true);
    }

    #[test]
    fn test_header_creation_and_parsing() {
        let algorithm = CompressionAlgorithm::Lz4;
        let original_size = 5000;

        let header = make_compressed_header(algorithm, original_size);
        let (parsed_algo, parsed_size) = parse_compressed_header(&header).unwrap();

        assert_eq!(parsed_algo, algorithm);
        assert_eq!(parsed_size, original_size);
    }

    #[test]
    fn test_header_none_algorithm() {
        let header = make_compressed_header(CompressionAlgorithm::None, 1000);
        let (algo, size) = parse_compressed_header(&header).unwrap();

        assert_eq!(algo, CompressionAlgorithm::None);
        assert_eq!(size, 1000);
    }

    #[test]
    fn test_header_zstd_algorithm() {
        let header = make_compressed_header(CompressionAlgorithm::Zstd, 7000);
        let (algo, size) = parse_compressed_header(&header).unwrap();

        assert_eq!(algo, CompressionAlgorithm::Zstd);
        assert_eq!(size, 7000);
    }

    #[test]
    fn test_compression_error_invalid_header() {
        let header = vec![0u8; 3];
        let data = vec![0u8; 100];

        let result = decompress_with_header(&header, &data);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data() {
        let data: Vec<u8> = vec![];
        let compressed = compress_lz4(&data).unwrap();
        let decompressed = decompress_lz4(&compressed, 0).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_algorithm_display() {
        assert_eq!(CompressionAlgorithm::None.to_string(), "none");
        assert_eq!(CompressionAlgorithm::Lz4.to_string(), "lz4");
        assert_eq!(CompressionAlgorithm::Zstd.to_string(), "zstd");
    }

    #[test]
    fn test_algorithm_from_str() {
        assert_eq!(CompressionAlgorithm::from_str("lz4"), Some(CompressionAlgorithm::Lz4));
        assert_eq!(CompressionAlgorithm::from_str("LZ4"), Some(CompressionAlgorithm::Lz4));
        assert_eq!(CompressionAlgorithm::from_str("zstd"), Some(CompressionAlgorithm::Zstd));
        assert_eq!(CompressionAlgorithm::from_str("none"), Some(CompressionAlgorithm::None));
        assert_eq!(CompressionAlgorithm::from_str("off"), Some(CompressionAlgorithm::None));
        assert_eq!(CompressionAlgorithm::from_str("invalid"), None);
    }

    #[test]
    fn test_compression_ratio_calculation() {
        let stats = CompressionStats {
            bytes_compressed: 1000,
            bytes_uncompressed: 10000,
            compression_count: 10,
            decompression_count: 5,
        };

        assert!((stats.compression_ratio() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_large_data_compression() {
        let data: Vec<u8> = (0..100000).map(|i| (i % 256) as u8).collect();

        let compressed_lz4 = compress_lz4(&data).unwrap();
        let decompressed_lz4 = decompress_lz4(&compressed_lz4, data.len()).unwrap();
        assert_eq!(data, decompressed_lz4);

        let compressed_zstd = compress_zstd(&data).unwrap();
        let decompressed_zstd = decompress_zstd(&compressed_zstd, data.len()).unwrap();
        assert_eq!(data, decompressed_zstd);
    }

    #[test]
    fn test_pattern_data_compression() {
        let data: Vec<u8> = vec![0x41u8; 10000];

        let compressed = compress_zstd(&data).unwrap();
        assert!(compressed.len() < 100);

        let decompressed = decompress_zstd(&compressed, data.len()).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_multiple_compressions_stats() {
        let mut ctx = CompressionContext::new(CompressionAlgorithm::Lz4);

        for _ in 0..5 {
            let data: Vec<u8> = vec![0u8; 3000];
            ctx.compress(&data).unwrap();
        }

        assert_eq!(ctx.stats().compression_count, 5);
        assert_eq!(ctx.stats().bytes_uncompressed, 5 * 3000);
    }

    #[test]
    fn test_decompression_count() {
        let mut ctx = CompressionContext::new(CompressionAlgorithm::Lz4);
        let data: Vec<u8> = vec![0u8; 3000];

        let compressed = ctx.compress(&data).unwrap();
        ctx.decompress(&compressed, data.len()).unwrap();
        ctx.decompress(&compressed, data.len()).unwrap();

        assert_eq!(ctx.stats().decompression_count, 2);
    }
}
