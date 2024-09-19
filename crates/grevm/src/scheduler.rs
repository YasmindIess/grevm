use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Display;
use std::ops::DerefMut;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use revm_primitives::db::DatabaseRef;
use revm_primitives::{Address, Env, SpecId, TxEnv};

use reth_revm::{CacheState, EvmBuilder};

use crate::hint::ParallelExecutionHints;
use crate::partition::{
    OrderedVectorExt, PartitionExecutor, PreRoundContext, PreUnconfirmedContext,
};
use crate::storage::SchedulerDB;
use crate::tx_dependency::TxDependency;
use crate::{
    GrevmError, LocationAndType, PartitionId, TxId, CPU_CORES, GREVM_RUNTIME, MAX_NUM_ROUND,
};

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

    database: Arc<SchedulerDB<DB>>,

    parallel_execution_hints: ParallelExecutionHints,

    tx_dependencies: TxDependency,

    // number of partitions. maybe larger in the first round to increase concurrence
    num_partitions: usize,
    // assigned txs ID for each partition
    partitioned_txs: Vec<Vec<TxId>>,
    partition_executors: Vec<Arc<RwLock<PartitionExecutor<DB>>>>,

    num_finality_txs: usize,

    // update after each round
    pre_unconfirmed_txs: BTreeMap<TxId, PreUnconfirmedContext>,
}

impl<DB> GrevmScheduler<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync + Display,
{
    pub fn new(spec_id: SpecId, env: Env, db: DB, txs: Vec<TxEnv>) -> Self {
        let coinbase = env.block.coinbase.clone();
        let parallel_execution_hints = ParallelExecutionHints::new(&txs);
        let tx_dependencies = TxDependency::new(&parallel_execution_hints);
        let num_partitions = *CPU_CORES * 2 + 1; // 2 * cpu + 1 for initial partition number
        Self {
            tx_batch_size: txs.len(),
            spec_id,
            env,
            coinbase,
            txs: Arc::new(txs),
            database: Arc::new(SchedulerDB::new(db)),
            parallel_execution_hints,
            tx_dependencies,
            num_partitions,
            partitioned_txs: vec![],
            partition_executors: vec![],
            num_finality_txs: 0,
            pre_unconfirmed_txs: BTreeMap::new(),
        }
    }

    pub(crate) fn partition_transactions(&mut self) {
        // compute and assign partitioned_txs
        self.partitioned_txs = self.tx_dependencies.fetch_best_partitions(self.num_partitions);
        self.num_partitions = self.partitioned_txs.len();
    }

    pub(crate) fn update_partition_status(&self) {
        // TODO(gravity_richard.zhz): update the status of partitions
    }

    pub(crate) fn revert(&self) {
        // TODO(gravity_richard.zhz): update the status of partitions
    }

    // Preload data when initializing dependencies
    async fn preload(&mut self, stop: &AtomicBool) {}

    fn generate_partition_pre_context(
        &mut self,
        partition_id: PartitionId,
    ) -> Option<PreRoundContext> {
        let mut pre_unconfirmed_txs = BTreeMap::new();
        for txid in &self.partitioned_txs[partition_id] {
            if let Some(ctx) = self.pre_unconfirmed_txs.remove(txid) {
                pre_unconfirmed_txs.insert(*txid, ctx);
            }
        }
        if pre_unconfirmed_txs.is_empty() {
            None
        } else {
            Some(PreRoundContext { pre_unconfirmed_txs })
        }
    }

    fn round_execute(&mut self) -> Result<(), GrevmError> {
        self.partitioned_txs.clear();
        for partition_id in 0..self.num_partitions {
            let mut executor = PartitionExecutor::new(
                self.spec_id.clone(),
                partition_id,
                self.env.clone(),
                self.database.clone(),
                self.txs.clone(),
                self.partitioned_txs[partition_id].clone(),
            );
            executor.set_pre_round_ctx(self.generate_partition_pre_context(partition_id));
            self.partition_executors.push(Arc::new(RwLock::new(executor)));
        }
        // has released pre_unconfirmed_txs
        assert!(self.pre_unconfirmed_txs.is_empty());

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
        // MUST drop the `PartitionExecutor::scheduler_db` before get mut
        // let db_mut = Arc::get_mut(&mut self.database).expect(...);
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

    /// When validating the transaction status, the dependency relationship was updated.
    /// But there are some transactions that have entered the finality state,
    /// and there is no need to record the dependency and dependent relationships of these transactions.
    /// Thus achieving the purpose of pruning.
    fn update_and_pruning_dependency(&mut self) {
        let num_finality_txs = self.num_finality_txs;
        if num_finality_txs == self.txs.len() {
            return;
        }
        let mut new_dependency: Vec<Vec<TxId>> = vec![vec![]; self.txs.len() - num_finality_txs];
        for executor in &self.partition_executors {
            let executor = executor.read().unwrap();
            for (txid, dep) in executor.assigned_txs.iter().zip(executor.tx_dependency.iter()) {
                if *txid >= num_finality_txs {
                    // pruning the tx that is finality state
                    new_dependency[*txid - num_finality_txs] = dep
                        .clone()
                        .into_iter()
                        // pruning the dependent tx that is finality state
                        .filter(|dep_id| *dep_id >= num_finality_txs)
                        .collect();
                }
            }
        }
        self.tx_dependencies.update_tx_dependency(new_dependency, num_finality_txs);
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

                    for (index, rs) in executor.read_set.iter().enumerate() {
                        let txid = executor.assigned_txs[index];
                        let mut conflict = executor.execute_results[txid].is_err();
                        if conflict {
                            // Transactions that fail in partition executor are in conflict state
                            executor.conflict_txs.push(txid);
                        }
                        let mut updated_dependencies = BTreeSet::new();
                        for location in rs {
                            if let Some(written_txs) = merged_write_set.get(location) {
                                for previous_txid in written_txs.range(..txid) {
                                    // update dependencies: previous_txid <- txid
                                    updated_dependencies.insert(*previous_txid);
                                    if !conflict
                                        && (!executor.assigned_txs.has(previous_txid)
                                            || executor.conflict_txs.has(previous_txid))
                                    {
                                        executor.conflict_txs.push(txid);
                                        conflict = true;
                                    }
                                }
                            }
                        }
                        executor.tx_dependency.push(updated_dependencies);
                        if !conflict {
                            executor.unconfirmed_txs.push(txid);
                        }
                    }
                    assert_eq!(
                        executor.unconfirmed_txs.len() + executor.conflict_txs.len(),
                        executor.assigned_txs.len()
                    );
                }));
            }
            futures::future::join_all(tasks).await;
        });
        // find the continuous min txid
        let mut unconfirmed_txs = BTreeMap::new();
        for executor in &self.partition_executors {
            let executor = executor.read().unwrap();
            for txid in executor.unconfirmed_txs.iter() {
                let index = executor.assigned_txs.index(txid);
                match &executor.execute_results[index] {
                    Ok(state) => {
                        unconfirmed_txs.insert(
                            *txid,
                            PreUnconfirmedContext {
                                read_set: executor.read_set[index].clone(),
                                write_set: executor.write_set[index].clone(),
                                execute_state: state.clone(),
                            },
                        );
                    }
                    Err(_) => {
                        return Err(GrevmError::UnreachableError(String::from(
                            "Should take failed transaction as conflict",
                        )));
                    }
                }
            }
        }
        if let Some((min_txid, _)) = unconfirmed_txs.first_key_value() {
            if *min_txid != self.num_finality_txs {
                // Most likely, the execution of the smallest transaction failed in evm.transact()
                // TODO(gaoxin): return the correct evm error
                return Err(GrevmError::ExecutionError(String::from(
                    "Discontinuous finality transaction ID",
                )));
            }
        } else {
            return Err(GrevmError::ExecutionError(String::from(
                "Can't finalize a transaction in an execution round",
            )));
        }

        for txid in unconfirmed_txs.keys() {
            if *txid == self.num_finality_txs {
                self.num_finality_txs += 1;
            } else {
                break;
            }
        }

        // TODO(gravity): early stop if num_finality_txs == txs.len()

        self.pre_unconfirmed_txs = unconfirmed_txs.split_off(&self.num_finality_txs);
        // Now `unconfirmed_txs` only contains the txs that are finality in this round

        // update and pruning tx dependencies
        self.update_and_pruning_dependency();

        let partition_state: Vec<CacheState> = self
            .partition_executors
            .iter()
            .map(|executor| {
                let mut executor = executor.write().unwrap();
                std::mem::take(&mut executor.partition_db.cache)
            })
            .collect();

        // MUST drop the `PartitionExecutor::scheduler_db` before get mut
        self.partition_executors.clear();
        let database = Arc::get_mut(&mut self.database)
            .expect("database is shared by other threads/struct here, indicating bugs");

        Self::merge_not_modified_state(&mut database.cache, partition_state);

        for ctx in unconfirmed_txs.into_values() {
            database.commit_transition(ctx.execute_state.transition);
        }

        Ok(())
    }

    /// Merge not modified state from partition to scheduler. These data are just loaded from
    /// database, so we can merge them to state as original value for next round.
    fn merge_not_modified_state(state: &mut CacheState, partition_state: Vec<CacheState>) {
        for partition in partition_state {
            // merge account state that is not modified
            for (address, account) in partition.accounts {
                if account.status.is_not_modified() && state.accounts.get(&address).is_none() {
                    state.accounts.insert(address, account);
                }
            }

            // merge contract code
            for (hash, code) in partition.contracts {
                if state.contracts.get(&hash).is_none() {
                    state.contracts.insert(hash, code);
                }
            }
        }
    }

    fn execute_remaining_sequential(&mut self) -> Result<(), GrevmError> {
        // execute remaining txs
        let database = Arc::get_mut(&mut self.database)
            .expect("database is shared by other threads/struct here, indicating bugs");
        let mut evm = EvmBuilder::default()
            .with_db(database)
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
                }
                Err(err) => return Err(GrevmError::ExecutionError(err.to_string())),
            }
        }
        Ok(())
    }

    fn parallel_execute(&mut self, hints: Vec<Vec<TxId>>) -> Result<(), GrevmError> {
        if self.txs.len() < self.num_partitions {
            return self.execute_remaining_sequential();
        }

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
