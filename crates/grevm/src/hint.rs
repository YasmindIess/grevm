use alloy_sol_types::private::primitives::TxKind;
use alloy_sol_types::private::Address;
use reth_revm::db::DbAccount;
use revm_primitives::TxEnv;
use std::cell::RefCell;
use std::collections::BTreeMap;
use reth_primitives::{B256, Bytes, hex};

pub(crate) type ReadKVSet = BTreeMap<Address, Option<DbAccount>>;
pub(crate) type WriteKVSet = BTreeMap<Address, Option<DbAccount>>;

#[derive(Debug, Default)]
pub(crate) struct TxRWSet {
    // the key contains the address prefix
    pub read_kv_set: RefCell<ReadKVSet>,
    pub write_kv_set: RefCell<WriteKVSet>,
}

enum RWType {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

impl TxRWSet {
    pub(crate) fn insert_key_value(
        &self,
        key: &Address,
        value: Option<DbAccount>,
        rw_type: RWType,
    ) {
        match rw_type {
            RWType::ReadOnly => {
                self.read_kv_set.borrow_mut().insert(key.clone(), value.clone());
            }
            RWType::WriteOnly => {
                self.write_kv_set.borrow_mut().insert(key.clone(), value.clone());
            }
            RWType::ReadWrite => {
                self.read_kv_set.borrow_mut().insert(key.clone(), value.clone());
                self.write_kv_set.borrow_mut().insert(key.clone(), value.clone());
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
            rw_set.insert_key_value(&tx_env.caller, None, RWType::ReadWrite);
            if let TxKind::Call(to_address) = tx_env.transact_to {
                rw_set.insert_key_value(&to_address, None, RWType::ReadWrite);

                if (!tx_env.data.is_empty()) {
                    ParallelExecutionHints::insert_hints_with_contract_data(
                        None, &tx_env.data, &mut rw_set);
                }
            }
            rw_set.insert_key_value(&tx_env.caller, None, RWType::ReadWrite);

            hints.push(rw_set);
        }

        ParallelExecutionHints { txs_hint: hints }
    }

    fn insert_hints_with_contract_data(code: Option<Bytes>, data: &Bytes, tx_rw_set: &mut TxRWSet) {
        // It should return if code is empty or data is empty
        // TODO(gravity_richard.zhz): refactor here after preloading the code
        if code.is_none() && data.is_empty() {
            return;
        }
        println!("code {:?} data {:?}",code, data);
        let (func_id, parameters) = Self::decode_contract_parameters(data);
        // TODO(gravity_richard.zhz): refactor the judgement later with contract template
        // ERC20_transfer
        if func_id == 0xa9059cbb {
            assert_eq!(data.len(), 68);
            assert_eq!(parameters.len(), 2);
            let from_address: [u8; 20] = parameters[0].as_slice()[12..].try_into().expect("try into failed");
            let from_address = Address::new(from_address);
            // println!("from_address {:?}", from_address);
            // TODO(gravity_richard.zhz): refactor the RWSet later
            tx_rw_set.insert_key_value(&from_address, None, RWType::ReadWrite);
        } else if func_id == 0x23b872dd {
            // ERC20_transfer_from
            assert_eq!(data.len(), 100);
            assert_eq!(parameters.len(), 3);
            let from_address: [u8; 20] = parameters[0].as_slice()[12..].try_into().expect("try into failed");
            let from_address = Address::new(from_address);
            let to_address: [u8; 20] = parameters[1].as_slice()[12..].try_into().expect("try into failed");
            let to_address = Address::new(to_address);
            // println!("from_address {:?}, to_address {:?}", from_address, to_address);
            // TODO(gravity_richard.zhz): refactor the RWSet later
            tx_rw_set.insert_key_value(&from_address, None, RWType::ReadWrite);
            tx_rw_set.insert_key_value(&to_address, None, RWType::ReadWrite);
        }
    }

    fn decode_contract_parameters(data: &Bytes) -> (u32, Vec<B256>) {
        let func_id: u32 = 0;
        let mut parameters: Vec<B256> = Vec::new();
        if data.len() <= 4 {
            return (func_id, parameters)
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
