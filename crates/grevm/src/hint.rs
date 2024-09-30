use std::cell::RefCell;
use std::collections::BTreeMap;

use alloy_sol_types::private::primitives::TxKind;
use alloy_sol_types::private::Address;
use revm_primitives::TxEnv;

use reth_primitives::ruint::aliases::U256;
use reth_primitives::{Bytes, B256};
use reth_revm::db::DbAccount;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum HintAccountState {
    #[default]
    Touched,
    Loaded,
}

#[derive(Debug, Clone, Default)]
pub struct HintAccount {
    state: HintAccountState,
    account: DbAccount,
}

pub(crate) type ReadKVSet = BTreeMap<Address, HintAccount>;
pub(crate) type WriteKVSet = BTreeMap<Address, HintAccount>;

enum RWType {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

#[derive(Debug, Default)]
pub(crate) struct TxRWSet {
    // the key contains the address prefix
    pub read_kv_set: RefCell<ReadKVSet>,
    pub write_kv_set: RefCell<WriteKVSet>,
}

impl TxRWSet {
    pub(crate) fn insert_account(&mut self, address: &Address, rw_type: RWType) {
        match rw_type {
            RWType::ReadOnly => {
                self.read_kv_set.borrow_mut().entry(address.clone()).or_default();
            }
            RWType::WriteOnly => {
                self.write_kv_set.borrow_mut().entry(address.clone()).or_default();
            }
            RWType::ReadWrite => {
                self.read_kv_set.borrow_mut().entry(address.clone()).or_default();
                self.write_kv_set.borrow_mut().entry(address.clone()).or_default();
            }
        }
    }

    pub(crate) fn insert_storage_slot(
        &mut self,
        address: &Address,
        storage_slot: U256,
        storage_slot_value: U256,
        rw_type: RWType,
    ) {
        match rw_type {
            RWType::ReadOnly => {
                let mut read_kv_set = self.read_kv_set.borrow_mut();
                read_kv_set
                    .entry(*address)
                    .or_default()
                    .account
                    .storage
                    .entry(storage_slot)
                    .or_insert(storage_slot_value);
            }
            RWType::WriteOnly => {
                let mut write_kv_set = self.write_kv_set.borrow_mut();
                write_kv_set
                    .entry(*address)
                    .or_default()
                    .account
                    .storage
                    .entry(storage_slot)
                    .or_insert(storage_slot_value);
            }
            RWType::ReadWrite => {
                let mut read_kv_set = self.read_kv_set.borrow_mut();
                read_kv_set
                    .entry(*address)
                    .or_default()
                    .account
                    .storage
                    .entry(storage_slot)
                    .or_insert(storage_slot_value);
                let mut write_kv_set = self.write_kv_set.borrow_mut();
                write_kv_set
                    .entry(*address)
                    .or_default()
                    .account
                    .storage
                    .entry(storage_slot)
                    .or_insert(storage_slot_value);
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ParallelExecutionHints {
    pub txs_hint: Vec<TxRWSet>,
}

impl ParallelExecutionHints {
    pub(crate) fn new(txs: &Vec<TxEnv>) -> Self {
        let mut hints: Vec<TxRWSet> = Vec::with_capacity(txs.len());

        for tx_env in txs {
            let mut rw_set = TxRWSet::default();
            // Causing transactions that call the same contract to inevitably
            // conflict with each other. Is this behavior reasonable?
            // TODO(gaoxin): optimize contract account
            rw_set.insert_account(&tx_env.caller, RWType::ReadWrite);
            if let TxKind::Call(to_address) = tx_env.transact_to {
                if !tx_env.data.is_empty() {
                    ParallelExecutionHints::insert_hints_with_contract_data(
                        to_address,
                        None,
                        &tx_env.data,
                        &mut rw_set,
                    );
                } else {
                    rw_set.insert_account(&to_address, RWType::ReadWrite);
                }
            }

            hints.push(rw_set);
        }

        ParallelExecutionHints { txs_hint: hints }
    }

    fn insert_hints_with_contract_data(
        contract_address: Address,
        code: Option<Bytes>,
        data: &Bytes,
        tx_rw_set: &mut TxRWSet,
    ) {
        // It should return if code is empty or data is empty
        // TODO(gravity_richard.zhz): refactor here after preloading the code
        if code.is_none() && data.is_empty() {
            return;
        }
        println!("code {:?} data {:?}", code, data);
        let (func_id, parameters) = Self::decode_contract_parameters(data);
        // TODO(gravity_richard.zhz): refactor the judgement later with contract template
        // ERC20_transfer
        for p in parameters.iter() {
            tx_rw_set.insert_storage_slot(
                &contract_address,
                U256::from_be_bytes(p.0),
                U256::from(0),
                RWType::ReadWrite,
            );
        }
        // TODO(gravity_richard.zhz): just for test and delete later
        if func_id == 0xa9059cbb {
            assert_eq!(data.len(), 68);
            assert_eq!(parameters.len(), 2);
            let from_address: [u8; 20] =
                parameters[0].as_slice()[12..].try_into().expect("try into failed");
            let from_address = Address::new(from_address);
            // println!("from_address {:?}", from_address);
            tx_rw_set.insert_account(&from_address, RWType::ReadWrite);
        } else if func_id == 0x23b872dd {
            // ERC20_transfer_from
            assert_eq!(data.len(), 100);
            assert_eq!(parameters.len(), 3);
            let from_address: [u8; 20] =
                parameters[0].as_slice()[12..].try_into().expect("try into failed");
            let from_address = Address::new(from_address);
            let to_address: [u8; 20] =
                parameters[1].as_slice()[12..].try_into().expect("try into failed");
            let to_address = Address::new(to_address);
            // println!("from_address {:?}, to_address {:?}", from_address, to_address);
            tx_rw_set.insert_account(&from_address, RWType::ReadWrite);
            tx_rw_set.insert_account(&to_address, RWType::ReadWrite);
        } else {
            tx_rw_set.insert_account(&contract_address, RWType::ReadWrite);
        }
    }

    fn decode_contract_parameters(data: &Bytes) -> (u32, Vec<B256>) {
        let func_id: u32 = 0;
        let mut parameters: Vec<B256> = Vec::new();
        if data.len() <= 4 {
            return (func_id, parameters);
        }

        let func_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        // println!("func_id {:?}", func_id);

        for chunk in data[4..].chunks(32) {
            let array: [u8; 32] = chunk.try_into().expect("Slice has wrong length!");
            parameters.push(B256::from(array));
        }
        // println!("parameters {:?}", parameters);

        (func_id, parameters)
    }

    pub(crate) fn update_tx_hint(&mut self, tx_index: usize, new_tx_rw_set: TxRWSet) {
        self.txs_hint[tx_index].read_kv_set = new_tx_rw_set.read_kv_set;
        self.txs_hint[tx_index].write_kv_set = new_tx_rw_set.write_kv_set;
    }
}
