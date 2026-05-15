#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(target_family = "wasm")]
mod wasm;

#[cfg(not(target_family = "wasm"))]
pub use native::SessionShardInterface;
#[cfg(target_family = "wasm")]
pub use wasm::SessionShardInterface;
