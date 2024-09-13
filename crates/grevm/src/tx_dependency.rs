use crate::hint::ParallelExecutionHints;
use crate::TxId;

pub struct TxDependency {
    // if txi <- txj, then tx_dependency[txj - num_finality_txs].push(txi)
    tx_dependency: Vec<Vec<TxId>>,
    // when a tx is in finality state, we don't need to store their dependencies
    num_finality_txs: usize,
    // After one round of transaction execution,
    // the running time can be obtained, which can make the next round of partitioning more balanced.
    tx_running_time: Option<Vec<u64>>,
}

impl TxDependency {
    pub fn new() -> Self {
        TxDependency { tx_dependency: vec![], num_finality_txs: 0, tx_running_time: None }
    }

    pub fn generate_tx_dependency(
        &mut self,
        parallel_execution_hints: &ParallelExecutionHints,
    ) -> Vec<Vec<TxId>> {
        todo!()
    }

    pub fn fetch_best_partitions(&mut self, partition_count: usize) -> Vec<Vec<TxId>> {
        // TODO(gravity_richard.zhz): split to partitions
        vec![vec![]]
    }

    pub fn update_tx_dependency(
        &mut self,
        tx_dependency: Vec<Vec<TxId>>,
        num_finality_txs: usize,
        tx_running_time: Option<Vec<u64>>,
    ) {
        if (self.tx_dependency.len() + self.num_finality_txs)
            != (tx_dependency.len() + num_finality_txs)
        {
            panic!("Different transaction number");
        }
        self.tx_dependency = tx_dependency;
        self.num_finality_txs = num_finality_txs;
        self.tx_running_time = tx_running_time;
    }
}
