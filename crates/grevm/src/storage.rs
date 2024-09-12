use std::collections::{hash_map, HashMap, HashSet};
use std::sync::Arc;

use reth_revm::db::states::CacheAccount;
use revm_primitives::db::{Database, DatabaseRef};
use revm_primitives::{Account, AccountInfo, Bytecode, EvmState, B256};

use reth_primitives::{Address, U256};
use reth_revm::CacheState;

use crate::{GREVM_RUNTIME, LocationAndType};

pub struct SchedulerDB<DB> {
    /// Cached committed data in each round
    pub cache: CacheState,
    pub database: DB,
}

impl<DB> SchedulerDB<DB> {
    pub fn new(cache: CacheState, database: DB) -> Self {
        Self { cache, database }
    }

    /// Fall back to sequential execute
    pub fn commit(&mut self, changes: HashMap<Address, Account>) {
        todo!()
    }
}

fn into_cache_account(account: Option<AccountInfo>) -> CacheAccount {
    match account {
        None => CacheAccount::new_loaded_not_existing(),
        Some(acc) if acc.is_empty() => CacheAccount::new_loaded_empty_eip161(HashMap::new()),
        Some(acc) => CacheAccount::new_loaded(acc.clone(), HashMap::new()),
    }
}

/// Get storage value of address at index.
fn load_storage<DB: DatabaseRef>(
    cache: &mut CacheState,
    database: &DB,
    address: Address,
    index: U256,
) -> Result<U256, DB::Error> {
    // Account is guaranteed to be loaded.
    // Note that storage from bundle is already loaded with account.
    if let Some(account) = cache.accounts.get_mut(&address) {
        // account will always be some, but if it is not, U256::ZERO will be returned.
        let is_storage_known = account.status.is_storage_known();
        Ok(account
            .account
            .as_mut()
            .map(|account| match account.storage.entry(index) {
                hash_map::Entry::Occupied(entry) => Ok(*entry.get()),
                hash_map::Entry::Vacant(entry) => {
                    // if account was destroyed or account is newly built
                    // we return zero and don't ask database.
                    let value = if is_storage_known {
                        U256::ZERO
                    } else {
                        tokio::task::block_in_place(|| database.storage_ref(address, index))?
                    };
                    entry.insert(value);
                    Ok(value)
                }
            })
            .transpose()?
            .unwrap_or_default())
    } else {
        unreachable!("For accessing any storage account is guaranteed to be loaded beforehand")
    }
}

/// Fall back to sequential execute
impl<DB> Database for SchedulerDB<DB>
where
    DB: DatabaseRef,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        match self.cache.accounts.entry(address) {
            hash_map::Entry::Vacant(entry) => {
                let info = tokio::task::block_in_place(|| self.database.basic_ref(address))?;
                Ok(entry.insert(into_cache_account(info)).account_info())
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().account_info()),
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let res = match self.cache.contracts.entry(code_hash) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            hash_map::Entry::Vacant(entry) => {
                let code =
                    tokio::task::block_in_place(|| self.database.code_by_hash_ref(code_hash))?;
                entry.insert(code.clone());
                Ok(code)
            }
        };
        res
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        load_storage(&mut self.cache, &self.database, address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

pub struct PartitionDB<DB> {
    pub coinbase: Address,

    // partition internal cache
    pub cache: CacheState,
    // read-only pre-round commit data
    pub scheduler_cache: Arc<CacheState>,
    // read data from origin database
    pub database: DB,

    tx_read_set: HashSet<LocationAndType>,
}

impl<DB> PartitionDB<DB> {
    pub fn new(coinbase: Address, scheduler_cache: Arc<CacheState>, database: DB) -> Self {
        Self {
            coinbase,
            cache: Default::default(),
            scheduler_cache,
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
        match self.cache.accounts.entry(address) {
            hash_map::Entry::Vacant(entry) => {
                // 2. read from scheduler cache
                if let Some(account) = self.scheduler_cache.accounts.get(&address) {
                    return Ok(entry.insert(account.clone()).account_info());
                }

                // 3. read from origin database
                let info = tokio::task::block_in_place(|| self.database.basic_ref(address))?;
                return Ok(entry.insert(into_cache_account(info)).account_info());
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().account_info()),
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 1. read from internal cache
        let res = match self.cache.contracts.entry(code_hash) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            hash_map::Entry::Vacant(entry) => {
                // 2. read from scheduler cache
                if let Some(code) = self.scheduler_cache.contracts.get(&code_hash) {
                    return Ok(entry.insert(code.clone()).clone());
                }

                // 3. read from origin database
                let code =
                    tokio::task::block_in_place(|| self.database.code_by_hash_ref(code_hash))?;
                entry.insert(code.clone());
                return Ok(code);
            }
        };
        res
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        load_storage(&mut self.cache, &self.database, address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}
