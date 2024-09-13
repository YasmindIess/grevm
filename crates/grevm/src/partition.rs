use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use revm_primitives::db::{Database, DatabaseRef};
use revm_primitives::{Address, EVMResult, Env, EvmState, ResultAndState, SpecId, TxEnv, U256};

use reth_revm::db::DbAccount;
use reth_revm::{CacheState, EvmBuilder};

use crate::hint::TxRWSet;
use crate::storage::PartitionDB;
use crate::{LocationAndType, PartitionId, TransactionStatus, TxId};

#[derive(Debug, Clone, PartialEq)]
enum PartitionStatus {
    Normal,
    Blocked,
    Finalized,
}

#[derive(Debug)]
pub struct Partition {
    finalized_tx_index: TxId,
    current_tx_index: TxId,
    partition_status: PartitionStatus,
    txs_status: Vec<TransactionStatus>,
    pending_tx_result: BTreeMap<TxId, TxRWSet>,
    partition_rw_set: TxRWSet,
    cache_data: BTreeMap<Address, DbAccount>,
}

/// Add some binary search methods for ordered vectors
pub trait OrderedVectorExt<E> {
    fn has(&self, element: &E) -> bool;
}

impl<E: Ord> OrderedVectorExt<E> for Vec<E> {
    fn has(&self, element: &E) -> bool {
        self.binary_search(element).is_ok()
    }
}

#[derive(Clone)]
pub struct PreUnconfirmedContext {
    pub read_set: HashSet<LocationAndType>,
    pub write_set: HashSet<LocationAndType>,
    pub execute_state: ResultAndState,
}

/// Record some status from the previous round,
/// which can accelerate the execution of the current round
#[derive(Default)]
pub struct PreRoundContext {
    pub pre_unconfirmed_txs: BTreeMap<TxId, PreUnconfirmedContext>,
}

impl PreRoundContext {
    pub fn take_pre_unconfirmed_ctx(&mut self, txid: &TxId) -> Option<PreUnconfirmedContext> {
        self.pre_unconfirmed_txs.remove(txid)
    }
}

pub struct PartitionExecutor<DB>
where
    DB: DatabaseRef,
{
    spec_id: SpecId,
    env: Env,
    coinbase: Address,
    partition_id: PartitionId,

    partition_db: PartitionDB<DB>,

    txs: Arc<Vec<TxEnv>>,
    pub assigned_txs: Vec<TxId>,
    pub read_set: Vec<HashSet<LocationAndType>>,
    pub write_set: Vec<HashSet<LocationAndType>>,

    pub execute_results: Vec<EVMResult<DB::Error>>,

    pub conflict_txs: Vec<TxId>,
    pub unconfirmed_txs: Vec<TxId>,
    pub tx_dependency: Vec<BTreeSet<TxId>>,

    pub coinbase_rewards: Vec<U256>,

    pub pre_round_ctx: Option<PreRoundContext>,
}

impl<DB> PartitionExecutor<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    pub fn new(
        spec_id: SpecId,
        partition_id: PartitionId,
        env: Env,
        scheduler_cache: Arc<CacheState>,
        database: DB,
        txs: Arc<Vec<TxEnv>>,
        assigned_txs: Vec<TxId>,
    ) -> Self {
        let coinbase = env.block.coinbase.clone();
        let partition_db = PartitionDB::new(coinbase.clone(), scheduler_cache, database);

        Self {
            spec_id,
            env,
            coinbase,
            partition_id,
            partition_db,
            txs,
            assigned_txs,
            read_set: vec![],
            write_set: vec![],
            execute_results: vec![],
            conflict_txs: vec![],
            unconfirmed_txs: vec![],
            tx_dependency: vec![],
            coinbase_rewards: vec![],
            pre_round_ctx: None,
        }
    }

    pub fn set_pre_round_ctx(&mut self, pre_round_context: Option<PreRoundContext>) {
        self.pre_round_ctx = pre_round_context;
    }

    pub fn generate_write_set(changes: &EvmState) -> HashSet<LocationAndType> {
        todo!()
    }

    pub fn execute(&mut self) {
        let mut update_write_set: HashSet<LocationAndType> = HashSet::new();
        let mut evm = EvmBuilder::default()
            .with_db(&mut self.partition_db)
            .with_spec_id(self.spec_id)
            .with_env(Box::new(self.env.clone()))
            .build();
        for txid in &self.assigned_txs {
            if let Some(tx) = self.txs.get(*txid) {
                *evm.tx_mut() = tx.clone();
            } else {
                panic!("Wrong transactions ID");
            }
            let mut should_rerun = true;
            if let Some(ctx) = self.pre_round_ctx.as_mut() {
                if let Some(PreUnconfirmedContext { read_set, write_set, execute_state }) =
                    ctx.take_pre_unconfirmed_ctx(txid)
                {
                    /// The unconfirmed transactions from the previous round may not require repeated execution.
                    /// The verification process is as follows:
                    /// 1. create a write set(update_write_set) to store write_set of previous conflict tx
                    /// 2. if an unconfirmed tx rerun, store write_set to update_write_set
                    /// 3. when running an unconfirmed tx, take it's pre-round read_set to join with update_write_set
                    /// 4. if the intersection is None, the unconfirmed tx no need to rerun, otherwise rerun
                    if read_set.is_disjoint(&update_write_set) {
                        self.read_set.push(read_set);
                        self.write_set.push(write_set);
                        self.execute_results.push(Ok(execute_state));
                        should_rerun = false;
                    }
                }
            }
            if should_rerun {
                let result = evm.transact();
                match &result {
                    Ok(result_and_state) => {
                        // update read set
                        self.read_set.push(evm.db_mut().take_read_set());
                        // update write set
                        let write_set = Self::generate_write_set(&result_and_state.state);
                        if self.pre_round_ctx.is_some() {
                            update_write_set.extend(write_set.clone().into_iter());
                        }
                        self.write_set.push(write_set);
                        // temporary commit to cache_db, to make use the remaining txs can read the updated data
                        evm.db_mut().temporary_commit(&result_and_state.state);
                    }
                    Err(err) => {
                        // Due to parallel execution, transactions may fail (such as dependent transfers not yet received),
                        // so the transaction failure does not result in block failure.
                        // Only the failure of transactions that in FINALITY state will result in block failure.
                        // During verification, failed transactions are conflict state

                        // update read set
                        self.read_set.push(evm.db_mut().take_read_set());
                        // update write set with empty set
                        self.write_set.push(HashSet::new());
                    }
                }
                self.execute_results.push(result);
            }
        }
        assert_eq!(self.assigned_txs.len(), self.execute_results.len());
    }
}
