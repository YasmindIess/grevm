use crate::hint::ParallelExecutionHints;
use crate::TxId;

pub struct TxDependency {
    tx_dependency: Vec<Vec<TxId>>,
}

impl TxDependency {
    pub fn new() -> Self {
        TxDependency {
            tx_dependency: vec![],
        }
    }

    pub fn generate_tx_dependency(
        &mut self, parallel_execution_hints: &ParallelExecutionHints) -> Vec<Vec<TxId>> {
        todo!()
    }

    pub fn fetch_best_partitions(&mut self, partition_count: usize) -> Vec<Vec<TxId>> {
        // TODO(gravity_richard.zhz): split to partitions
        vec![vec![]]
    }
}