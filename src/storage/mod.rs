//! Storage layer implementation.
//!
//! This module provides the core storage abstractions including:
//! - Page-based storage with 8KB pages
//! - Buffer pool for caching pages in memory
//! - B+Tree indexes for fast lookups
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │  Buffer Pool    │
//! └────────┬────────┘
//!          │
//! ┌────────▼────────┐
//! │  Page Manager   │
//! └─────────────────┘
//! ```

pub mod btree;
pub mod buffer_pool;
pub mod disk;
pub mod error;
pub mod hash_index;
pub mod heap;
pub mod index;
pub mod page;

pub use buffer_pool::BufferPool;
pub use disk::DiskManager;
pub use error::{Result, StorageError};
pub use page::{PAGE_SIZE, Page, PageId};

#[cfg(test)]
mod edge_tests;
