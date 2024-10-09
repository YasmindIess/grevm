use alloy_sol_types::private::primitives::TxKind;
use alloy_sol_types::private::Address;
use revm_primitives::TxEnv;
use smallvec::SmallVec;

use reth_primitives::ruint::aliases::U256;
use reth_primitives::{Bytes, B256};

use crate::LocationAndType;

type LocationVec = SmallVec<[LocationAndType; 2]>;

enum RWType {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

#[derive(Clone)]
pub(crate) struct TxRWSet {
    pub read_set: LocationVec,
    pub write_set: LocationVec,
}

impl Default for TxRWSet {
    fn default() -> Self {
        Self { read_set: LocationVec::new(), write_set: LocationVec::new() }
    }
}

impl TxRWSet {
    pub(crate) fn insert_location(&mut self, location: LocationAndType, rw_type: RWType) {
        match rw_type {
            RWType::ReadOnly => {
                self.read_set.push(location);
            }
            RWType::WriteOnly => {
                self.write_set.push(location);
            }
            RWType::ReadWrite => {
                self.read_set.push(location.clone());
                self.write_set.push(location);
            }
        }
    }
}

pub struct ParallelExecutionHints {
    pub txs_hint: Vec<TxRWSet>,
}

impl ParallelExecutionHints {
    #[fastrace::trace]
    pub(crate) fn new(txs: &Vec<TxEnv>) -> Self {
        let mut hints: Vec<TxRWSet> = vec![TxRWSet::default(); txs.len()];

        for (index, tx_env) in txs.iter().enumerate() {
            let mut rw_set = &mut hints[index];
            // Causing transactions that call the same contract to inevitably
            // conflict with each other. Is this behavior reasonable?
            // TODO(gaoxin): optimize contract account
            rw_set.insert_location(LocationAndType::Basic(tx_env.caller), RWType::ReadWrite);
            if let TxKind::Call(to_address) = tx_env.transact_to {
                if !tx_env.data.is_empty() {
                    ParallelExecutionHints::insert_hints_with_contract_data(
                        to_address,
                        None,
                        &tx_env.data,
                        &mut rw_set,
                    );
                } else {
                    rw_set.insert_location(LocationAndType::Basic(to_address), RWType::ReadWrite);
                }
            }
        }

        ParallelExecutionHints { txs_hint: hints }
    }

    #[fastrace::trace]
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
        // println!("code {:?} data {:?}", code, data);
        if data.len() < 4 || (data.len() - 4) % 32 != 0 {
            // Invalid tx, or tx that triggers fallback CALL
            return;
        }
        let (func_id, parameters) = Self::decode_contract_parameters(data);
        // TODO(gravity_richard.zhz): refactor the judgement later with contract template
        // ERC20_transfer
        for p in parameters.iter() {
            tx_rw_set.insert_location(
                LocationAndType::Storage(contract_address, U256::from_be_bytes(p.0)),
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
            tx_rw_set.insert_location(LocationAndType::Basic(from_address), RWType::ReadWrite);
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
            tx_rw_set.insert_location(LocationAndType::Basic(from_address), RWType::ReadWrite);
            tx_rw_set.insert_location(LocationAndType::Basic(to_address), RWType::ReadWrite);
        } else {
            tx_rw_set.insert_location(LocationAndType::Basic(contract_address), RWType::ReadWrite);
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
}
