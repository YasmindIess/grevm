use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::ops::DerefMut;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use revm_primitives::db::{Database, DatabaseRef};
use revm_primitives::{Address, Env, ResultAndState, SpecId, TxEnv};

use reth_revm::{CacheState, EvmBuilder};

use crate::hint::ParallelExecutionHints;
use crate::partition::{OrderedVectorExt, PartitionExecutor};
use crate::storage::SchedulerDB;
use crate::tx_dependency::TxDependency;
use crate::{GrevmError, LocationAndType, TxId, CPU_CORES, GREVM_RUNTIME, MAX_NUM_ROUND};

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

    cache: Option<Arc<CacheState>>,
    database: DB,

    parallel_execution_hints: ParallelExecutionHints,

    tx_dependencies: TxDependency,

    // number of partitions. maybe larger in the first round to increase concurrence
    num_partitions: usize,
    // assigned txs ID for each partition
    partitioned_txs: Vec<Vec<TxId>>,
    partition_executors: Vec<Arc<RwLock<PartitionExecutor<DB>>>>,

    num_finality_txs: usize,
    execute_states: Vec<ResultAndState>,

    // update after each round
    conflict_txs: Vec<TxId>,    // tx that is conflicted and need to rerun
    unconfirmed_txs: Vec<TxId>, // tx that is validated but not the continuous ID
}

impl<DB> GrevmScheduler<DB>
where
    DB: DatabaseRef + Clone + Send + Sync + 'static,
    DB::Error: Send + Sync + Display,
{
    pub fn new(spec_id: SpecId, env: Env, db: DB, txs: Vec<TxEnv>) -> Self {
        let coinbase = env.block.coinbase.clone();
        let parallel_execution_hints = ParallelExecutionHints::new(&txs);
        let num_partitions = *CPU_CORES * 2 + 1; // 2 * cpu + 1 for initial partition number
        Self {
            tx_batch_size: txs.len(),
            spec_id,
            env,
            coinbase,
            txs: Arc::new(txs),
            cache: Some(Arc::new(CacheState::default())),
            database: db,
            parallel_execution_hints,
            tx_dependencies: TxDependency::new(),
            num_partitions,
            partitioned_txs: vec![],
            partition_executors: vec![],
            num_finality_txs: 0,
            execute_states: vec![],
            conflict_txs: vec![],
            unconfirmed_txs: vec![],
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

    fn round_execute(&mut self) -> Result<(), GrevmError> {
        for partition_id in 0..self.num_partitions {
            self.partition_executors.push(Arc::new(RwLock::new(PartitionExecutor::new(
                self.spec_id.clone(),
                partition_id,
                self.env.clone(),
                self.cache.as_ref().unwrap().clone(),
                self.database.clone(),
                self.txs.clone(),
            ))));
        }
        GREVM_RUNTIME.block_on(async {
            let mut tasks = vec![];
            for executor in &self.partition_executors {
                let executor = executor.clone();
                tasks.push(GREVM_RUNTIME.spawn(async move { executor.write().unwrap().execute() }));
            }
            futures::future::join_all(tasks).await;
        });
        // validate transactions
        // TODO(gravity_nekomoto): Merge changed state of finality txs
        // MUST drop the `PartitionExecutor::scheduler_cache` before make mut
        // let cache_mut = Arc::make_mut(&mut self.cache);
        self.validate_transactions()
    }

    // merge write set after each round
    fn merge_write_set(&mut self) -> HashMap<LocationAndType, BTreeSet<TxId>> {
        let mut merged_write_set: HashMap<LocationAndType, BTreeSet<TxId>> = HashMap::new();
        for executor in &self.partition_executors {
            let executor = executor.read().unwrap();
            for (txid, ws) in executor.assigned_txs.iter().zip(executor.write_set.iter()) {
                for location in ws {
                    merged_write_set.entry(location.clone()).or_default().insert(*txid);
                }
            }
        }
        merged_write_set
    }

    /// verification of transaction state after each round
    /// Because after each round execution, the read-write set is no longer updated.
    /// We can check in parallel whether the read set is out of bounds.
    fn validate_transactions(&mut self) -> Result<(), GrevmError> {
        GREVM_RUNTIME.block_on(async {
            let mut tasks = vec![];
            let merged_write_set = Arc::new(self.merge_write_set());
            for executor in &self.partition_executors {
                let executor = executor.clone();
                let merged_write_set = merged_write_set.clone();
                tasks.push(GREVM_RUNTIME.spawn(async move {
                    // Tx validate process:
                    // 1. Traverse the read set of each tx in each partition, and find the Range<TxId> less than tx in merged_write_set
                    // 2. conflict: 1) exist tx in Range<TxId> not belong to tx's partition; 2) exist tx in Range<TxId> is conflicted
                    let mut executor = executor.write().unwrap();
                    let executor = executor.deref_mut();

                    for (txid, rs) in executor.assigned_txs.iter().zip(executor.read_set.iter()) {
                        for location in rs {
                            if let Some(written_txs) = merged_write_set.get(location) {
                                for previous_txid in written_txs.range(..txid) {
                                    if executor.assigned_txs.has(previous_txid) {
                                        if executor.conflict_txs.has(previous_txid) {
                                            executor.conflict_txs.push(*txid);
                                        } else {
                                            executor.unconfirmed_txs.push(*txid);
                                        }
                                    } else {
                                        executor.conflict_txs.push(*txid);
                                    }
                                }
                            }
                        }
                    }
                }));
            }
            futures::future::join_all(tasks).await;
        });
        // find the continuous min txid
        let mut unconfirmed_txs = BTreeSet::new();
        for executor in &self.partition_executors {
            for txid in executor.read().unwrap().unconfirmed_txs.iter() {
                unconfirmed_txs.insert(*txid);
            }
        }
        if let Some(min_txid) = unconfirmed_txs.first() {
            if *min_txid != self.num_finality_txs {
                return Err(GrevmError::ExecutionError(String::from(
                    "Discontinuous finality transaction ID",
                )));
            }
        } else {
            return Err(GrevmError::ExecutionError(String::from(
                "Can't finalize a transaction in an execution round",
            )));
        }
        for txid in unconfirmed_txs {
            if txid == self.num_finality_txs {
                self.num_finality_txs += 1;
            } else {
                break;
            }
        }
        // update the final conflict_txs and unconfirmed_txs
        let mut conflict_txs = BTreeSet::new();
        let mut unconfirmed_txs = BTreeSet::new();
        for executor in &self.partition_executors {
            let executor = executor.read().unwrap();
            for txid in executor.conflict_txs.iter() {
                conflict_txs.insert(*txid);
            }
            for txid in executor.unconfirmed_txs.iter() {
                // we didn't delete the finality txs from executor.unconfirmed_txs for we no longer use
                // maybe we should update if we use executor.unconfirmed_txs later
                if *txid >= self.num_finality_txs {
                    unconfirmed_txs.insert(*txid);
                }
            }
        }
        self.conflict_txs = conflict_txs.into_iter().collect();
        self.unconfirmed_txs = unconfirmed_txs.into_iter().collect();
        let validate_len =
            self.num_finality_txs + self.conflict_txs.len() + self.unconfirmed_txs.len();
        if validate_len != self.tx_batch_size {
            Err(GrevmError::ExecutionError(String::from("Invalidate txs length")))
        } else {
            Ok(())
        }
    }

    fn execute_remaining_sequential(&mut self) -> Result<(), GrevmError> {
        assert_eq!(self.num_finality_txs, self.execute_states.len());
        // execute remaining txs
        let cache = Arc::into_inner(self.cache.take().unwrap())
            .expect("cache is shared by other threads/struct here, indicating bugs");
        let mut evm = EvmBuilder::default()
            .with_db(SchedulerDB::new(cache, self.database.clone()))
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
                self.round_execute()?;
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
