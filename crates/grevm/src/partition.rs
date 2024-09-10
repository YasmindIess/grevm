use std::collections::HashSet;
use std::sync::Arc;
use revm_primitives::{Address, BlockEnv, EvmState, ResultAndState, TxEnv, U256};
use revm_primitives::db::{Database, DatabaseRef};

use reth_revm::Evm;

use crate::{LocationAndType, PartitionId, TxId};
use crate::storage::CacheDB;

fn build_evm<'a, DB: Database>(
    db: &DB,
    chain_id: u64,
    block_env: &BlockEnv,
) -> Evm<'a, (), DB> {
    todo!()
}

pub struct PartitionExecutor<DB>
{
    chain_id: u64,
    block_env: BlockEnv,
    coinbase: Address,
    partition_id: PartitionId,

    cache_db: CacheDB<DB>,

    txs: Arc<Vec<TxEnv>>,
    assigned_txs: Vec<TxId>,
    read_set: Vec<HashSet<LocationAndType>>,
    write_set: Vec<HashSet<LocationAndType>>,

    execute_states: Vec<ResultAndState>,

    finality_txs: Vec<TxId>,
    conflict_txs: Vec<TxId>,
    unconfirmed_txs: Vec<TxId>,

    coinbase_rewards: Vec<U256>,
}


impl<DB> PartitionExecutor<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    pub fn new(partition_id: PartitionId, db: DB) -> Self {
        // not yield in PartitionExecutor
        let cache_db = CacheDB::new(db, false);
        todo!()
    }

    pub fn generate_write_set(changes: &EvmState) -> HashSet<LocationAndType> {
        todo!()
    }

    pub fn execute(&mut self) {
        let mut evm = build_evm(&self.cache_db, self.chain_id, &self.block_env);
        for txid in &self.assigned_txs {
            if let Some(tx) = self.txs.get(*txid) {
                *evm.tx_mut() = tx.clone();
            }
            match evm.transact() {
                Ok(result_and_state) => {
                    // update read set
                    self.read_set.push(self.cache_db.take_read_set());
                    // update write set
                    self.write_set.push(Self::generate_write_set(&result_and_state.state));
                    // temporary commit to cache_db, to make use the remaining txs can read the updated data
                    self.cache_db.temporary_commit(&result_and_state.state);
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
