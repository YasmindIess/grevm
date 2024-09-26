mod execute;
pub mod storage;

pub use execute::{compare_evm_execute, mock_eoa_account};

pub const TRANSFER_GAS_LIMIT: u64 = 21_000;
