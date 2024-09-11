use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use revm_primitives::{Account, AccountInfo, B256, Bytecode, EvmState};
use revm_primitives::db::{Database, DatabaseRef};

use reth_primitives::{Address, U256};
use reth_revm::CacheState;

use crate::{GREVM_RUNTIME, LocationAndType};

/// Read from cache, return Some if found
trait CacheRead {
    fn basic_option(&self, address: Address) -> Option<AccountInfo>;

    fn code_by_hash_option(&self, code_hash: B256) -> Option<Bytecode>;

    fn storage_option(&self, address: Address, index: U256) -> Option<U256>;

    fn block_hash_option(&self, number: u64) -> Option<B256>;
}

impl CacheRead for CacheState {
    fn basic_option(&self, address: Address) -> Option<AccountInfo> {
        todo!()
    }

    fn code_by_hash_option(&self, code_hash: B256) -> Option<Bytecode> {
        todo!()
    }

    fn storage_option(&self, address: Address, index: U256) -> Option<U256> {
        todo!()
    }

    fn block_hash_option(&self, number: u64) -> Option<B256> {
        todo!()
    }
}

pub struct SchedulerDB<DB> {
    // cache committed data in each round
    pub cache: CacheState,
    pub database: DB,
}

impl<DB> SchedulerDB<DB> {
    pub fn new(database: DB) -> Self {
        Self {
            cache: Default::default(),
            database,
        }
    }

    /// Fall back to sequential execute
    pub fn commit(&mut self, changes: HashMap<Address, Account>) {
        todo!()
    }
}

/// Fall back to sequential execute
impl<DB> Database for SchedulerDB<DB>
where
    DB: DatabaseRef
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let info = self.cache.basic_option(address);
        if info.is_some() {
            return Ok(info);
        }
        let info = self.database.basic_ref(address);
        // todo: update cache
        info
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

impl<DB> CacheRead for SchedulerDB<DB> {
    fn basic_option(&self, address: Address) -> Option<AccountInfo> {
        todo!()
    }

    fn code_by_hash_option(&self, code_hash: B256) -> Option<Bytecode> {
        todo!()
    }

    fn storage_option(&self, address: Address, index: U256) -> Option<U256> {
        todo!()
    }

    fn block_hash_option(&self, number: u64) -> Option<B256> {
        todo!()
    }
}

pub struct PartitionDB<DB> {
    pub coinbase: Address,

    // read internal cache
    pub cache: CacheState,
    // read pre-round commit data
    pub scheduler_db: Arc<RwLock<SchedulerDB<DB>>>,
    // read data from origin database
    pub database: DB,

    tx_read_set: HashSet<LocationAndType>,
}

impl<DB> PartitionDB<DB> {
    pub fn new(coinbase: Address,
               scheduler_db: Arc<RwLock<SchedulerDB<DB>>>,
               database: DB) -> Self {
        Self {
            coinbase,
            cache: Default::default(),
            scheduler_db,
            database,
            tx_read_set: Default::default(),
        }
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
impl<DB> Database for PartitionDB<DB>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Send + Sync,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 1. read from internal cache
        let info = self.cache.basic_option(address.clone());
        if info.is_some() {
            return Ok(info);
        }
        // 2. read from pre-round commit data
        let info = self.scheduler_db.read().unwrap().basic_option(address.clone());
        if info.is_some() {
            return Ok(info);
        }
        // 3. read from origin database
        tokio::task::block_in_place(move || {
            let info = self.database.basic_ref(address);
            // todo: update self.cache
            info
        })
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
