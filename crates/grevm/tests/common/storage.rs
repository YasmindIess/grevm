use reth_primitives::{Address, B256, U256};
use revm_primitives::db::DatabaseRef;
use revm_primitives::{keccak256, Account, AccountInfo, Bytecode};
use std::collections::HashMap;

/// A DatabaseRef that stores chain data in memory.
#[derive(Debug, Default, Clone)]
pub struct InMemoryDB {
    accounts: HashMap<Address, Account>,
    bytecodes: HashMap<B256, Bytecode>,
    block_hashes: HashMap<u64, B256>,
}

impl InMemoryDB {
    pub(crate) fn new(
        accounts: HashMap<Address, Account>,
        bytecodes: HashMap<B256, Bytecode>,
        block_hashes: HashMap<u64, B256>,
    ) -> Self {
        Self { accounts, bytecodes, block_hashes }
    }
}

impl DatabaseRef for InMemoryDB {
    type Error = String;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(self.accounts.get(&address).map(|account| account.info.clone()))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.bytecodes.get(&code_hash).cloned().ok_or(String::from("can't find code by hash"))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let storage = self.accounts.get(&address).ok_or("can't find account")?;
        Ok(storage.storage.get(&index).cloned().unwrap_or_default().present_value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        Ok(self
            .block_hashes
            .get(&number)
            .cloned()
            // Matching REVM's [EmptyDB] for now
            .unwrap_or_else(|| keccak256(number.to_string().as_bytes())))
    }
}
