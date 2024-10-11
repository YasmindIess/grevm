// Each cluster has one ERC20 contract and X families.
// Each family has Y people.
// Each person performs Z transfers to random people within the family.

use std::collections::HashMap;

use common::storage::InMemoryDB;
use reth_revm::primitives::TxEnv;

use crate::erc20::{generate_cluster, TransactionModeType, TxnBatchConfig};

#[path = "../common/mod.rs"]
pub mod common;

#[path = "./mod.rs"]
pub mod erc20;

#[test]
fn erc20_independent() {
    const NUM_SCA: usize = 1;
    const NUM_EOA: usize = 100;
    const NUM_TXNS_PER_ADDRESS: usize = 1;
    let batch_txn_config = TxnBatchConfig::new(
        NUM_EOA,
        NUM_SCA,
        NUM_TXNS_PER_ADDRESS,
        erc20::TransactionCallDataType::Transfer,
        TransactionModeType::SameCaller,
    );
    let (mut state, bytecodes, txs) = generate_cluster(&batch_txn_config);
    let miner = common::mock_miner_account();
    state.insert(miner.0, miner.1);
    let db = InMemoryDB::new(state, bytecodes, Default::default());
    common::compare_evm_execute(db, txs, false, HashMap::new());
}

#[test]
fn erc20_batch_transfer() {
    const NUM_SCA: usize = 10;
    const NUM_EOA: usize = 10;
    const NUM_TXNS_PER_ADDRESS: usize = 2;

    let batch_txn_config = TxnBatchConfig::new(
        NUM_EOA,
        NUM_SCA,
        NUM_TXNS_PER_ADDRESS,
        erc20::TransactionCallDataType::Transfer,
        TransactionModeType::Random,
    );

    let mut final_state = HashMap::from([common::mock_miner_account()]);
    let mut final_bytecodes = HashMap::default();
    let mut final_txs = Vec::<TxEnv>::new();
    for _ in 0..1 {
        let (state, bytecodes, txs) = generate_cluster(&batch_txn_config);
        final_state.extend(state);
        final_bytecodes.extend(bytecodes);
        final_txs.extend(txs);
    }

    let db = InMemoryDB::new(final_state, final_bytecodes, Default::default());
    common::compare_evm_execute(db, final_txs, true, HashMap::new());
}
