use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;
use std::thread;
use revm_primitives::{Address, TxEnv};
use revm_primitives::db::DatabaseRef;
use crate::storage::CacheDB;
use crate::{LocationAndType, MAX_NUM_ROUND, TxId};
use crate::partition::PartitionExecutor;

pub struct GrevmScheduler<'a, DB>
where
    DB: DatabaseRef + Send + Sync,
{
    coinbase: Address,
    txs: &'a [TxEnv],

    // update PartitionExecutor::cache_db of FINALITY tx after each round,
    // and merge into GrevmScheduler::state
    state: CacheDB<DB>,

    // if txi depends on txj: txi -> txj (txj should run first)
    // then, dependencies[txj].push(txi)
    dependencies: Vec<Vec<TxId>>,

    // number of partitions. maybe larger in the first round to increase concurrence
    num_partitions: usize,
    // assigned txs ID for each partition
    partitioned_txs: Vec<Vec<TxId>>,
    partition_executors: Vec<PartitionExecutor<'a, DB>>,

    // merge PartitionExecutor::write_set to merged_write_set after each round
    merged_write_set: HashMap<LocationAndType, BTreeSet<TxId>>,

    num_finality_txs: usize,
}

impl<'a, DB> GrevmScheduler<'a, DB>
where
    DB: DatabaseRef + Send + Sync,
{
    pub fn new() -> Self {
        todo!()
    }

    pub fn partition_transactions(&mut self) {
        // compute and assign partitioned_txs
    }

    // initialize dependencies:
    // 1. txs without contract can generate dependencies from 'from/to' address
    // 2. consensus can build the dependencies(hints) of txs with contract
    pub fn init_dependencies(&mut self, hints: Vec<Vec<TxId>>) {
        // self.preload()
        // update dependencies
        self.partition_transactions();
    }

    // Preload data when initializing dependencies
    async fn preload(&mut self, stop: &AtomicBool) {}

    fn round_execute(&mut self) {
        thread::scope(|scope| {
            for partition_id in 0..self.num_partitions {
                /*
                self.partition_executors.push(PartitionExecutor::new(&self.state));
                scope.spawn(|| {
                    self.partition_executors[partition_id].execute();
                });
                */
            }
        });
        // merge write set
        self.merge_write_set();
        // validate transactions
        self.num_finality_txs += self.validate_transactions();
    }

    // merge write set after each round
    fn merge_write_set(&mut self) {}

    // return the number of txs that in FINALITY state
    fn validate_transactions(&mut self) -> usize {
        for executor in &self.partition_executors {
            // Tx validate process:
            // 1. Traverse the read set of each tx in each partition, and find the Range<TxId> less than tx in merged_write_set
            // 2. conflict: 1) exist tx in Range<TxId> not belong to tx's partition; 2) exist tx in Range<TxId> is conflicted
        }
        todo!()
    }

    fn execute_remaining_sequential(&mut self) {}

    fn parallel_execute(&mut self, hints: Vec<Vec<TxId>>) {
        self.init_dependencies(hints);
        for i in 0..MAX_NUM_ROUND {
            if self.num_finality_txs < self.txs.len() {
                self.round_execute();
                if self.num_finality_txs < self.txs.len() && i < MAX_NUM_ROUND - 1 {
                    self.partition_transactions();
                }
            }
        }
        if self.num_finality_txs < self.txs.len() {
            self.execute_remaining_sequential();
        }
    }
}
