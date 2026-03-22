//! Protocol helpers for ant-node client operations.
//!
//! This module provides low-level protocol support for client-node communication.
//! For high-level client operations, use the `ant-client` crate instead.
//!
//! # Architecture
//!
//! This module contains:
//!
//! 1. **Protocol message handlers**: Send/await pattern for chunks
//! 2. **Data types**: Common types like `XorName`, `DataChunk`, address computation
//!
//! # Migration Note
//!
//! The `QuantumClient` has been deprecated and consolidated into `ant-client::Client`.
//! Use `ant-client` for all client operations.
//!
//! # Example
//!
//! ```rust,ignore
//! use ant_client::Client; // Use ant-client instead of QuantumClient
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // High-level client API
//!     let client = Client::connect(&bootstrap_peers, Default::default()).await?;
//!
//!     // Store data with payment
//!     let address = client.chunk_put(bytes::Bytes::from("hello world")).await?;
//!
//!     // Retrieve data
//!     let chunk = client.chunk_get(&address).await?;
//!
//!     Ok(())
//! }
//! ```

mod chunk_protocol;
mod data_types;

pub use chunk_protocol::send_and_await_chunk_response;
pub use data_types::{
    compute_address, peer_id_to_xor_name, xor_distance, ChunkStats, DataChunk, XorName,
};

// Re-export hex_node_id_to_encoded_peer_id for payment operations
use crate::error::{Error, Result};
use ant_evm::EncodedPeerId;

/// Identity multihash code (stores raw bytes without hashing).
const MULTIHASH_IDENTITY_CODE: u64 = 0x00;

/// Convert a hex-encoded 32-byte node ID to an [`EncodedPeerId`].
///
/// Peer IDs are 64-character hex strings representing 32 raw bytes.
/// libp2p `PeerId` expects a multihash-encoded identity. This function bridges the two
/// formats by wrapping the raw bytes in an identity multihash (code 0x00) and then
/// converting to `EncodedPeerId` via `From<PeerId>`.
///
/// # Errors
///
/// Returns an error if the hex string is invalid or the peer ID cannot be constructed.
pub fn hex_node_id_to_encoded_peer_id(hex_id: &str) -> Result<EncodedPeerId> {
    let raw_bytes = hex::decode(hex_id)
        .map_err(|e| Error::Payment(format!("Invalid hex peer ID '{hex_id}': {e}")))?;

    let multihash =
        multihash::Multihash::<64>::wrap(MULTIHASH_IDENTITY_CODE, &raw_bytes).map_err(|e| {
            Error::Payment(format!(
                "Failed to create multihash for peer '{hex_id}': {e}"
            ))
        })?;

    let peer_id = libp2p::PeerId::from_multihash(multihash).map_err(|_| {
        Error::Payment(format!(
            "Failed to create PeerId from multihash for peer '{hex_id}'"
        ))
    })?;

    Ok(EncodedPeerId::from(peer_id))
}
