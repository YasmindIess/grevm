use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use revm_primitives::{AccountInfo, B256, Bytecode, EvmState};
use revm_primitives::db::{Database, DatabaseRef};

use reth_primitives::{Address, U256};
use reth_revm::{CacheState, TransitionState};
use reth_revm::db::BundleState;

use crate::{GREVM_RUNTIME, LocationAndType};

pub struct CacheDB<DB> {
    pub coinbase: Address,

    pub cache: CacheState,

    pub database: Arc<DB>,
    // whether to yield IO operations
    // only yield for GrevmScheduler's DatabaseRef trait
    pub should_yield: bool,

    pub transition_state: Option<TransitionState>,

    pub bundle_state: BundleState,

    pub use_preloaded_bundle: bool,

    pub block_hashes: BTreeMap<u64, B256>,

    tx_read_set: HashSet<LocationAndType>,
}

impl<DB> CacheDB<DB> {
    pub fn new(db: DB, should_yield: bool) -> Self {
        todo!()
    }

    // consume the read set after evm.transact() for each tx
    pub fn take_read_set(&mut self) -> HashSet<LocationAndType> {
        core::mem::take(&mut self.tx_read_set)
    }

    // temporary commit the state change after evm.transact() for each tx
    pub fn temporary_commit(&mut self, changes: &EvmState) {}

    // commit the state changes of txs in FINALITY state after each round
    pub fn finality_commit(&mut self, changes: &EvmState) {}
}

/// Used to build evm, and hook the read operations
impl<DB> Database for CacheDB<DB>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Send + Sync,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        todo!()
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        todo!()
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        todo!()
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

/// Used for GrevmScheduler::state
impl<DB> DatabaseRef for CacheDB<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    type Error = <Self as Database>::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if self.should_yield {
            let db = self.database.clone();
            tokio::task::block_in_place(move || {
                GREVM_RUNTIME.block_on(async move {
                    tokio::task::spawn_blocking(move || {
                        db.basic_ref(address)
                    }).await.unwrap()
                })
            })
        } else {
            self.database.basic_ref(address)
        }
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        todo!()
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        todo!()
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}
