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

pub mod error;
pub mod page;
pub mod buffer_pool;
pub mod btree;
pub mod heap;
pub mod disk;

pub use error::{StorageError, Result};
pub use page::{Page, PageId, PAGE_SIZE};
pub use buffer_pool::BufferPool;
pub use disk::DiskManager;
