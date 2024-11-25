#![allow(missing_docs)]

mod common;
use std::{io::BufWriter, sync::Arc};

use common::storage::InMemoryDB;
use grevm::GrevmScheduler;
use metrics_util::debugging::DebuggingRecorder;
use revm::db::states::bundle_state::BundleRetention;
use revm_primitives::{EnvWithHandlerCfg, TxEnv};

/// Return gas used
fn test_execute(
    env: EnvWithHandlerCfg,
    txs: Vec<TxEnv>,
    db: InMemoryDB,
    dump_transition: bool,
) -> u64 {
    let txs = Arc::new(txs);
    let db = Arc::new(db);

    let reth_result =
        common::execute_revm_sequential(db.clone(), env.spec_id(), env.env.as_ref().clone(), &*txs)
            .unwrap();

    let sequential_result = {
        let mut executor = GrevmScheduler::new(
            env.spec_id(),
            env.env.as_ref().clone(),
            db.clone(),
            txs.clone(),
            None,
        );
        let parallel_result = executor.force_sequential_execute().unwrap();
        let database = Arc::get_mut(&mut executor.database).unwrap();
        if dump_transition {
            serde_json::to_writer_pretty(
                BufWriter::new(std::fs::File::create("transition_state_sequential.json").unwrap()),
                &database.state.transition_state.as_ref().unwrap().transitions,
            )
            .unwrap();
        }
        database.state.merge_transitions(BundleRetention::Reverts);
        (parallel_result, database.state.take_bundle())
    };

    // create registry for metrics
    let recorder = DebuggingRecorder::new();
    let parallel_result = metrics::with_local_recorder(&recorder, || {
        let mut executor = GrevmScheduler::new(env.spec_id(), *env.env, db, txs, None);
        let parallel_result = executor.force_parallel_execute(true, Some(23)).unwrap();

        let snapshot = recorder.snapshotter().snapshot();
        for (key, _, _, value) in snapshot.into_vec() {
            println!("metrics: {} => value: {:?}", key.key().name(), value);
        }

        let database = Arc::get_mut(&mut executor.database).unwrap();
        if dump_transition {
            serde_json::to_writer(
                BufWriter::new(std::fs::File::create("transition_state_parallel.json").unwrap()),
                &database.state.transition_state.as_ref().unwrap().transitions,
            )
            .unwrap();
        }
        database.state.merge_transitions(BundleRetention::Reverts);
        (parallel_result, database.state.take_bundle())
    });

    common::compare_execution_result(&reth_result.0.results, &sequential_result.0.results);
    common::compare_execution_result(&reth_result.0.results, &parallel_result.0.results);

    common::compare_bundle_state(&reth_result.1, &sequential_result.1);
    common::compare_bundle_state(&reth_result.1, &parallel_result.1);

    // TODO(gravity_nekomoto): compare the receipts root

    reth_result.0.results.iter().map(|r| r.gas_used()).sum()
}

#[test]
fn mainnet() {
    if let Ok(block_number) = std::env::var("BLOCK_NUMBER").map(|s| s.parse().unwrap()) {
        // Test a specific block
        let bytecodes = common::load_bytecodes_from_disk();
        let (env, txs, mut db) = common::load_block_from_disk(block_number);
        if db.bytecodes.is_empty() {
            // Use the global bytecodes if the block doesn't have its own
            db.bytecodes = bytecodes.clone();
        }
        let dump_transition = std::env::var("DUMP_TRANSITION").is_ok();
        test_execute(env, txs, db, dump_transition);
        return;
    }

    common::for_each_block_from_disk(|env, txs, db| {
        let number = env.env.block.number;
        let num_txs = txs.len();
        println!("Test Block {number}");
        let gas_used = test_execute(env, txs, db, false);
        println!("Test Block {number} done({num_txs} txs, {gas_used} gas)");
    });
}
