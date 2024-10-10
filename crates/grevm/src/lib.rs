use std::fmt::{Display, Formatter};

use lazy_static::lazy_static;
use reth_revm::TransitionAccount;
use revm_primitives::{Address, EVMError, EVMResultGeneric, ExecutionResult, B256, U256};
use tokio::runtime::{Builder, Runtime};
mod hint;
mod partition;
mod scheduler;
mod storage;
mod tx_dependency;

lazy_static! {
    static ref CPU_CORES: usize =
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
}

lazy_static! {
    static ref GREVM_RUNTIME: Runtime = Builder::new_multi_thread()
        // .worker_threads(1) // for debug
        .worker_threads(std::thread::available_parallelism().map(|n| n.get() * 2).unwrap_or(8))
        .thread_name("grevm-tokio-runtime")
        .enable_all()
        .build()
        .unwrap();
}

static MAX_NUM_ROUND: usize = 3;

type PartitionId = usize;

type TxId = usize;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum LocationAndType {
    Basic(Address),
    Storage(Address, U256),
    Code(Address),
}

#[derive(Debug)]
enum TransactionStatus {
    Initial,     // tx that has not yet been run once
    Unconfirmed, // tx that is validated but not the continuous ID
    Pending,     // tx that is pending to wait other txs ready
    Conflict,    // tx that is conflicted and need to rerun
    Finality,    // tx that is validated and is the continuous ID
}

pub struct PartitionIndex {
    tx_index: usize,
    partition_id: usize,
}

#[derive(Debug)]
pub enum GrevmError<DBError> {
    EvmError(EVMError<DBError>),
    ExecutionError(String),
    UnreachableError(String),
}

impl<DBError: Display> Display for GrevmError<DBError> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GrevmError::EvmError(e) => write!(f, "EVM Error: {}", e),
            GrevmError::ExecutionError(e) => write!(f, "Execution Error: {}", e),
            GrevmError::UnreachableError(e) => write!(f, "Unreachable Error: {}", e),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResultAndTransition {
    /// Status of execution
    pub result: Option<ExecutionResult>,
    /// State that got updated
    pub transition: Vec<(Address, TransitionAccount)>,
    /// Rewards to miner
    pub rewards: u128,
}

pub type GrevmResult<DBError> = EVMResultGeneric<ResultAndTransition, DBError>;

pub use scheduler::*;
