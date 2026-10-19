//! Local-only reference Adapters for the portable HSM Interface.
//! Not a transparent POSIX filesystem or a distributed metadata authority.

mod archive;
mod executor;
mod storage;
mod util;

pub use archive::LocalArchive;
pub use executor::LocalDataMover;
pub use storage::LocalStorage;
