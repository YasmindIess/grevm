use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock};

use revm_primitives::{Address, Env, EvmState, ResultAndState, SpecId, TxEnv, U256};
use revm_primitives::db::{Database, DatabaseRef};

use reth_revm::db::DbAccount;
use reth_revm::EvmBuilder;

use crate::{LocationAndType, PartitionId, TransactionStatus, TxId};
use crate::hint::TxRWSet;
use crate::storage::{PartitionDB, SchedulerDB};

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

pub struct PartitionExecutor<DB>
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

    pub execute_states: Vec<ResultAndState>,

    pub conflict_txs: Vec<TxId>,
    pub unconfirmed_txs: Vec<TxId>,

    pub coinbase_rewards: Vec<U256>,
}


impl<DB> PartitionExecutor<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    pub fn new(spec_id: SpecId,
               partition_id: PartitionId,
               env: Env,
               scheduler_db: Arc<RwLock<SchedulerDB<DB>>>,
               database: DB,
               txs: Arc<Vec<TxEnv>>) -> Self {
        let coinbase = env.block.coinbase.clone();
        let cache_db = PartitionDB::new(coinbase.clone(), scheduler_db, database);
        Self {
            spec_id,
            env,
            coinbase,
            partition_id,
            partition_db: cache_db,
            txs,
            assigned_txs: vec![],
            read_set: vec![],
            write_set: vec![],
            execute_states: vec![],
            conflict_txs: vec![],
            unconfirmed_txs: vec![],
            coinbase_rewards: vec![],
        }
    }

    pub fn generate_write_set(changes: &EvmState) -> HashSet<LocationAndType> {
        todo!()
    }

    pub fn execute(&mut self) {
        let mut evm = EvmBuilder::default().with_db(&mut self.partition_db)
            .with_spec_id(self.spec_id)
            .with_env(Box::new(self.env.clone()))
            .build();
        for txid in &self.assigned_txs {
            if let Some(tx) = self.txs.get(*txid) {
                *evm.tx_mut() = tx.clone();
            } else {
                panic!("Wrong transactions ID");
            }
            match evm.transact() {
                Ok(result_and_state) => {
                    // update read set
                    self.read_set.push(evm.db_mut().take_read_set());
                    // update write set
                    self.write_set.push(Self::generate_write_set(&result_and_state.state));
                    // temporary commit to cache_db, to make use the remaining txs can read the updated data
                    evm.db_mut().temporary_commit(&result_and_state.state);
                    // update the result set to get the final result of FINALITY tx in scheduler
                    self.execute_states.push(result_and_state);
                }
                Err(err) => {
                    // Due to parallel execution, transactions may fail (such as dependent transfers not yet received),
                    // so the transaction failure does not result in block failure.
                    // Only the failure of transactions that in FINALITY state will result in block failure.
                }
            }
        }
    }
}
