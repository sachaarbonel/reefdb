pub mod btree;
pub mod fts;
pub mod index_manager;
pub mod disk;

pub use index_manager::{IndexManager, IndexType, DefaultIndexManager};