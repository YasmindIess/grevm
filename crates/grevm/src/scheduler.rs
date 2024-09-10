use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use lazy_static::lazy_static;

use revm_primitives::{Address, TxEnv};
use revm_primitives::db::DatabaseRef;

use crate::{GREVM_RUNTIME, LocationAndType, MAX_NUM_ROUND, TxId};
use crate::hint::ParallelExecutionHints;
use crate::partition::PartitionExecutor;
use crate::storage::CacheDB;
use crate::tx_dependency::TxDependency;


lazy_static! {
    static ref CPU_CORES: usize =
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
}

pub struct GrevmScheduler<DB>
{
    // The number of transactions in the tx batch size.
    tx_batch_size: usize,

    coinbase: Address,
    txs: Arc<Vec<TxEnv>>,

    // update PartitionExecutor::cache_db of FINALITY tx after each round,
    // and merge into GrevmScheduler::state
    state: Arc<CacheDB<DB>>,

    parallel_execution_hints: ParallelExecutionHints,

    // if txi depends on txj: txi -> txj (txj should run first)
    // then, dependencies[txj].push(txi)
    tx_dependencies: TxDependency,

    // number of partitions. maybe larger in the first round to increase concurrence
    num_partitions: usize,
    // assigned txs ID for each partition
    partitioned_txs: Vec<Vec<TxId>>,
    partition_executors: Vec<Arc<RwLock<PartitionExecutor<Arc<CacheDB<DB>>>>>>,

    // merge PartitionExecutor::write_set to merged_write_set after each round
    merged_write_set: HashMap<LocationAndType, BTreeSet<TxId>>,

    num_finality_txs: usize,
}

impl<DB> GrevmScheduler<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    pub fn new(db: DB, tx_batch_size: usize,
               txs_env: Vec<TxEnv>, partition_count: usize) -> Self {
        // yield the DatabaseRef trait's IO operations
        Self {
            tx_batch_size,
            coinbase: Address::default(),
            state: Arc::from(CacheDB::new(db, true)),
            parallel_execution_hints: ParallelExecutionHints::new(&txs_env),
            tx_dependencies: TxDependency::new(),
            txs: Arc::from(txs_env),
            num_partitions: partition_count,
            partitioned_txs: Vec::new(),
            partition_executors: Vec::new(),
            merged_write_set: HashMap::<LocationAndType, BTreeSet<TxId>>::new(),
            num_finality_txs: 0,
        }
    }


    pub fn partition_transactions(&mut self) {
        // compute and assign partitioned_txs
    }

    // initialize dependencies:
    // 1. txs without contract can generate dependencies from 'from/to' address
    // 2. consensus can build the dependencies(hints) of txs with contract
    pub fn init_tx_dependencies(&mut self, hints: ParallelExecutionHints, partition_count: usize) {
        self.parallel_execution_hints = hints;
        self.tx_dependencies.generate_tx_dependency(&self.parallel_execution_hints);
        self.tx_dependencies.fetch_best_partitions(partition_count);
    }

    fn split_partitions_with_tx_dependency(&mut self) {
        // TODO(gravity_richard.zhz): Split to dependency
        self.partitioned_txs = self.tx_dependencies.fetch_best_partitions(self.num_partitions);
    }

    pub(crate) fn update_partition_status(&self) {
        // TODO(gravity_richard.zhz): update the status of partitions
    }

    pub(crate) fn revert(&self) {
        // TODO(gravity_richard.zhz): update the status of partitions
    }

    // Preload data when initializing dependencies
    async fn preload(&mut self, stop: &AtomicBool) {}

    fn round_execute(&mut self) {
        for partition_id in 0..self.num_partitions {
            self.partition_executors.push(
                Arc::new(RwLock::new(PartitionExecutor::new(partition_id, self.state.clone()))));
        }
        GREVM_RUNTIME.block_on(async {
            let mut tasks = vec![];
            for executor in &self.partition_executors {
                let executor = executor.clone();
                tasks.push(GREVM_RUNTIME.spawn(async move {
                    executor.write().unwrap().execute()
                }));
            }
            futures::future::join_all(tasks).await;
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
