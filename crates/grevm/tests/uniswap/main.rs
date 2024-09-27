//! Launch K clusters.
//! Each cluster has M people.
//! Each person makes N swaps.

#[path = "../common/mod.rs"]
pub mod common;

#[path = "../erc20/mod.rs"]
pub mod erc20;

#[path = "./mod.rs"]
pub mod uniswap;

use std::collections::HashMap;

use crate::uniswap::generate_cluster;
use common::storage::InMemoryDB;
use reth_revm::primitives::TxEnv;

#[test]
fn uniswap_clusters() {
    const NUM_CLUSTERS: usize = 20;
    const NUM_PEOPLE_PER_CLUSTER: usize = 20;
    const NUM_SWAPS_PER_PERSON: usize = 20;

    let mut final_state = HashMap::from([common::mock_miner_account()]);
    let mut final_bytecodes = HashMap::default();
    let mut final_txs = Vec::<TxEnv>::new();
    for _ in 0..NUM_CLUSTERS {
        let (state, bytecodes, txs) =
            generate_cluster(NUM_PEOPLE_PER_CLUSTER, NUM_SWAPS_PER_PERSON);
        final_state.extend(state);
        final_bytecodes.extend(bytecodes);
        final_txs.extend(txs);
    }

    let db = InMemoryDB::new(final_state, final_bytecodes, Default::default());
    common::compare_evm_execute(db, final_txs, false);
}
