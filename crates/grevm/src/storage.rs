use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use revm_primitives::{AccountInfo, B256, Bytecode, EvmState};
use revm_primitives::db::{Database, DatabaseRef};

use reth_primitives::{Address, U256};
use reth_revm::{CacheState, TransitionState};
use reth_revm::db::BundleState;

use crate::LocationAndType;

#[async_trait]
trait AsyncDatabaseRef {
    type Error;

    async fn async_basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error>;

    async fn async_code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error>;

    async fn async_storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error>;

    async fn async_block_hash_ref(&self, number: u64) -> Result<B256, Self::Error>;
}


pub struct CacheDB<DB> {
    pub coinbase: Address,

    pub cache: CacheState,

    pub database: DB,

    pub transition_state: Option<TransitionState>,

    pub bundle_state: BundleState,

    pub use_preloaded_bundle: bool,

    pub block_hashes: BTreeMap<u64, B256>,

    tx_read_set: HashSet<LocationAndType>,
}

impl<DB> CacheDB<DB> {
    pub fn new() -> Self {
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

/*
#[async_trait]
impl<DB> AsyncDatabaseRef for CacheDB<DB>
where
    DB: DatabaseRef<Error: Send> + Send + Sync,
{
    type Error = DB::Error;

    async fn async_basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let db = self.database.clone();
        task::spawn_blocking(move || {
            db.basic_ref(address)
        }).await.unwrap()
    }

    async fn async_code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        task::spawn_blocking(move || {
            self.code_by_hash_ref(code_hash)
        }).await.unwrap()
    }

    async fn async_storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        task::spawn_blocking(move || {
            self.storage_ref(address, index)
        }).await.unwrap()
    }

    async fn async_block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        task::spawn_blocking(move || {
            self.block_hash_ref(number)
        }).await.unwrap()
    }
}
*/

impl<DB: DatabaseRef> Database for CacheDB<DB> {
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

impl<DB: DatabaseRef> DatabaseRef for CacheDB<DB> {
    type Error = DB::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        todo!()
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
