mod execute;
mod storage;

pub use storage::InMemoryDB;

pub use execute::{compare_evm_execute, mock_eoa_account};

pub const TRANSFER_GAS_LIMIT: u64 = 21_000;
