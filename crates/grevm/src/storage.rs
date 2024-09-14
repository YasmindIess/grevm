use std::collections::{btree_map, hash_map, BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use reth_revm::db::states::CacheAccount;
use revm_primitives::db::{Database, DatabaseRef};
use revm_primitives::{Account, AccountInfo, Bytecode, EvmState, B256, BLOCK_HASH_HISTORY};

use reth_primitives::{Address, U256};
use reth_revm::CacheState;

use crate::{GREVM_RUNTIME, LocationAndType};

pub struct SchedulerDB<DB> {
    /// Cache the committed data of finality txns and the read-only data during execution after each
    /// round of execution. Used as the initial state for the next round of partition executors.
    /// When fall back to sequential execution, used as cached state contains both changed from evm
    /// execution and cached/loaded account/storages.
    pub cache: CacheState,
    pub database: DB,
    /// If EVM asks for block hash we will first check if they are found here.
    /// and then ask the database.
    ///
    /// This map can be used to give different values for block hashes if in case
    /// The fork block is different or some blocks are not saved inside database.
    pub block_hashes: BTreeMap<u64, B256>,
}

impl<DB> SchedulerDB<DB> {
    pub fn new(database: DB) -> Self {
        Self { cache: CacheState::default(), database, block_hashes: BTreeMap::new() }
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
        match self.block_hashes.entry(number) {
            btree_map::Entry::Occupied(entry) => Ok(*entry.get()),
            btree_map::Entry::Vacant(entry) => {
                let ret = *entry
                    .insert(tokio::task::block_in_place(|| self.database.block_hash_ref(number))?);

                // prune all hashes that are older then BLOCK_HASH_HISTORY
                let last_block = number.saturating_sub(BLOCK_HASH_HISTORY);
                while let Some(entry) = self.block_hashes.first_entry() {
                    if *entry.key() < last_block {
                        entry.remove();
                    } else {
                        break;
                    }
                }

                Ok(ret)
            }
        }
    }
}

pub struct PartitionDB<DB> {
    pub coinbase: Address,

    // partition internal cache
    pub cache: CacheState,
    pub scheduler_db: Arc<SchedulerDB<DB>>,
    pub block_hashes: BTreeMap<u64, B256>,

    /// Record the read set of current tx, will be consumed after the execution of each tx
    tx_read_set: HashSet<LocationAndType>,
}

impl<DB> PartitionDB<DB> {
    pub fn new(coinbase: Address, scheduler_db: Arc<SchedulerDB<DB>>) -> Self {
        Self {
            coinbase,
            cache: CacheState::default(),
            scheduler_db,
            block_hashes: BTreeMap::new(),
            tx_read_set: HashSet::new(),
        }
    }

    /// consume the read set after evm.transact() for each tx
    pub fn take_read_set(&mut self) -> HashSet<LocationAndType> {
        core::mem::take(&mut self.tx_read_set)
    }

    /// Generate the write set after evm.transact() for each tx
    pub fn generate_write_set(&self, changes: &EvmState) -> HashSet<LocationAndType> {
        let mut write_set = HashSet::new();
        for (address, account) in changes {
            if account.is_selfdestructed() {
                // Only contract code can be selfdestructed
                // TODO(gravity_nekomoto): Write test to check if this is correct.
                // Should we use the code_hash from the `account.info` or the code_hash from the
                // existing account in the `self.cache`?
                assert!(!account.info.is_empty_code_hash());
                write_set.insert(LocationAndType::Code(account.info.code_hash()));
                continue;
            }

            if account.is_touched() {
                let has_code = !account.info.is_empty_code_hash();

                if match self.cache.accounts.get(address) {
                    Some(read_account) => {
                        read_account.account.as_ref().map_or(true, |read_account| {
                            (has_code && read_account.info.is_empty_code_hash()) // is newly created contract
                                || read_account.info.nonce != account.info.nonce
                                || read_account.info.balance != account.info.balance
                        })
                    }
                    None => true,
                } {
                    if has_code {
                        write_set.insert(LocationAndType::Code(account.info.code_hash()));
                    } else {
                        write_set.insert(LocationAndType::Basic(*address));
                    }
                }
            }

            for (slot, _) in account.changed_storage_slots() {
                write_set.insert(LocationAndType::Storage(*address, *slot));
            }
        }
        write_set
    }

    // temporary commit the state change after evm.transact() for each tx
    pub fn temporary_commit(&mut self, changes: &EvmState) {}
}

/// Used to build evm, and hook the read operations
impl<DB> Database for PartitionDB<DB>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Send + Sync,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.tx_read_set.insert(LocationAndType::Basic(address));

        // 1. read from internal cache
        match self.cache.accounts.entry(address) {
            hash_map::Entry::Vacant(entry) => {
                // 2. read initial state of this round from scheduler cache
                if let Some(account) = self.scheduler_db.cache.accounts.get(&address) {
                    return Ok(entry.insert(account.clone()).account_info());
                }

                // 3. read from origin database
                let info =
                    tokio::task::block_in_place(|| self.scheduler_db.database.basic_ref(address))?;
                return Ok(entry.insert(into_cache_account(info)).account_info());
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().account_info()),
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.tx_read_set.insert(LocationAndType::Code(code_hash));

        // 1. read from internal cache
        let res = match self.cache.contracts.entry(code_hash) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            hash_map::Entry::Vacant(entry) => {
                // 2. read initial state of this round from scheduler cache
                if let Some(code) = self.scheduler_db.cache.contracts.get(&code_hash) {
                    return Ok(entry.insert(code.clone()).clone());
                }

                // 3. read from origin database
                let code = tokio::task::block_in_place(|| {
                    self.scheduler_db.database.code_by_hash_ref(code_hash)
                })?;
                entry.insert(code.clone());
                return Ok(code);
            }
        };
        res
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.tx_read_set.insert(LocationAndType::Storage(address, index));

        load_storage(&mut self.cache, &self.scheduler_db.database, address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // FIXME(gravity_nekomoto): too lot repeated code
        match self.block_hashes.entry(number) {
            btree_map::Entry::Occupied(entry) => Ok(*entry.get()),
            btree_map::Entry::Vacant(entry) => {
                // TODO(gravity_nekomoto): read from scheduler_db?
                let ret = *entry.insert(tokio::task::block_in_place(|| {
                    self.scheduler_db.database.block_hash_ref(number)
                })?);

                // prune all hashes that are older then BLOCK_HASH_HISTORY
                let last_block = number.saturating_sub(BLOCK_HASH_HISTORY);
                while let Some(entry) = self.block_hashes.first_entry() {
                    if *entry.key() < last_block {
                        entry.remove();
                    } else {
                        break;
                    }
                }

                Ok(ret)
            }
        }
    }
}
