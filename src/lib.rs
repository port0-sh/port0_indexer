//! MidenXYK indexer library.
//!
//! The chain reader, decoder, activity classifier, and SQLite store, shared by
//! the two binaries in this package: the indexer, which polls the chain and owns
//! the store, and the web server, which reads the store and renders pages. They
//! are separate processes so that neither takes the other down, and the store is
//! the only thing they share.

pub mod activity;
pub mod api;
pub mod decode;
pub mod registry;
pub mod rpc;
pub mod state;
pub mod store;
