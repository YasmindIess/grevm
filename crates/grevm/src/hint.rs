use std::cell::RefCell;
use std::collections::BTreeMap;
use alloy_sol_types::private::Address;
use alloy_sol_types::private::primitives::TxKind;
use revm_primitives::TxEnv;
use reth_revm::db::DbAccount;

pub type ReadKVSet = BTreeMap<Address, Option<DbAccount>>;
pub type WriteKVSet = BTreeMap<Address, Option<DbAccount>>;

#[derive(Debug, Default)]
pub struct TxRWSet {
    // the key contains the address prefix
    read_kv_set: RefCell<ReadKVSet>,
    write_kv_set: RefCell<WriteKVSet>,
}

enum RWType {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}


impl TxRWSet {
    pub(crate) fn insert_key_value(&self, key: &Address, value: Option<DbAccount>, rw_type: RWType) {
        match rw_type {
            RWType::ReadOnly => {
                self.read_kv_set.borrow_mut().insert(key.clone(), value.clone());
            },
            RWType::WriteOnly => {
                self.write_kv_set.borrow_mut().insert(key.clone(), value.clone());
            },
            RWType::ReadWrite => {
                self.read_kv_set.borrow_mut().insert(key.clone(), value.clone());
                self.write_kv_set.borrow_mut().insert(key.clone(), value.clone());
            },
        }
        // TODO(gravity_richard.zhz): return code if exist?
    }
}

#[derive(Debug, Default)]
pub struct ParallelExecutionHints {
    txs_hint: Vec<TxRWSet>,
}

impl ParallelExecutionHints {
    pub fn new(txs: &Vec<TxEnv>) -> Self {
        let mut hints: Vec<TxRWSet> = Vec::with_capacity(txs.len());

        for (index, tx_env) in txs.iter().enumerate() {
            let mut rw_set = TxRWSet::default();
            rw_set.insert_key_value(&tx_env.caller, None, RWType::ReadWrite);
            if let TxKind::Call(to_address) =  tx_env.transact_to {
                rw_set.insert_key_value(&to_address, None, RWType::ReadWrite);
            }
            hints.push(rw_set);
        }

        ParallelExecutionHints {
            txs_hint: hints,
        }
    }

    pub fn update_tx_hint(&mut self, tx_index: usize, new_tx_rw_set: TxRWSet) {
        self.txs_hint[tx_index].read_kv_set = new_tx_rw_set.read_kv_set;
        self.txs_hint[tx_index].write_kv_set = new_tx_rw_set.write_kv_set;
    }
}
