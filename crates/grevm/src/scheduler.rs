use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;

use lazy_static::lazy_static;
use revm_primitives::{Address, Env, ResultAndState, SpecId, TxEnv};
use revm_primitives::db::{Database, DatabaseRef};

use reth_revm::EvmBuilder;

use crate::{GREVM_RUNTIME, GrevmError, LocationAndType, MAX_NUM_ROUND, TxId};
use crate::hint::ParallelExecutionHints;
use crate::partition::PartitionExecutor;
use crate::storage::SchedulerDB;
use crate::tx_dependency::TxDependency;

lazy_static! {
    static ref CPU_CORES: usize =
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
}

pub struct GrevmScheduler<DB>
where
    DB: DatabaseRef,
{
    // The number of transactions in the tx batch size.
    tx_batch_size: usize,

    spec_id: SpecId,
    env: Env,
    coinbase: Address,
    txs: Arc<Vec<TxEnv>>,

    scheduler_db: Arc<RwLock<SchedulerDB<Arc<DB>>>>,
    database: Arc<DB>,

    parallel_execution_hints: ParallelExecutionHints,

    tx_dependencies: TxDependency,

    // number of partitions. maybe larger in the first round to increase concurrence
    num_partitions: usize,
    // assigned txs ID for each partition
    partitioned_txs: Vec<Vec<TxId>>,
    partition_executors: Vec<Arc<RwLock<PartitionExecutor<Arc<DB>>>>>,

    // merge PartitionExecutor::write_set to merged_write_set after each round
    merged_write_set: HashMap<LocationAndType, BTreeSet<TxId>>,

    num_finality_txs: usize,
    execute_states: Vec<ResultAndState>,
}

impl<DB> GrevmScheduler<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync + Display,
{
    pub fn new(spec_id: SpecId,
               env: Env,
               db: DB,
               txs: Vec<TxEnv>) -> Self {
        let coinbase = env.block.coinbase.clone();
        let database = Arc::new(db);
        let scheduler_db = Arc::new(RwLock::new(SchedulerDB::new(database.clone())));
        let parallel_execution_hints = ParallelExecutionHints::new(&txs);
        Self {
            tx_batch_size: txs.len(),
            spec_id,
            env,
            coinbase,
            txs: Arc::new(txs),
            scheduler_db,
            database,
            parallel_execution_hints,
            tx_dependencies: TxDependency::new(),
            num_partitions: 0, // 0 for init
            partitioned_txs: vec![],
            partition_executors: vec![],
            merged_write_set: Default::default(),
            num_finality_txs: 0,
            execute_states: vec![],
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
                Arc::new(RwLock::new(PartitionExecutor::new(self.spec_id.clone(),
                                                            partition_id,
                                                            self.env.clone(),
                                                            self.scheduler_db.clone(),
                                                            self.database.clone(),
                                                            self.txs.clone()))));
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

    fn execute_remaining_sequential(&mut self) -> Result<(), GrevmError> {
        assert_eq!(self.num_finality_txs, self.execute_states.len());
        // execute remaining txs
        let mut db_guard = self.scheduler_db.write().unwrap();
        let mut evm = EvmBuilder::default().with_db(db_guard.deref_mut())
            .with_spec_id(self.spec_id)
            .with_env(Box::new(self.env.clone()))
            .build();
        for txid in self.num_finality_txs..self.txs.len() {
            if let Some(tx) = self.txs.get(txid) {
                *evm.tx_mut() = tx.clone();
            } else {
                return Err(GrevmError::UnreachableError(String::from("Wrong transactions ID")));
            }
            match evm.transact() {
                Ok(result_and_state) => {
                    evm.db_mut().commit(result_and_state.state.clone());
                    self.execute_states.push(result_and_state);
                }
                Err(err) => return Err(GrevmError::ExecutionError(err.to_string())),
            }
        }
        Ok(())
    }

    fn parallel_execute(&mut self, hints: Vec<Vec<TxId>>) -> Result<(), GrevmError> {
        for i in 0..MAX_NUM_ROUND {
            if self.num_finality_txs < self.txs.len() {
                self.partition_transactions();
                self.round_execute();
            } else {
                break;
            }
        }
        if self.num_finality_txs < self.txs.len() {
            self.execute_remaining_sequential()?;
        }
        Ok(())
    }
}