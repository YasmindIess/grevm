use std::cmp::{min, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};

use revm_primitives::Address;

use crate::hint::ParallelExecutionHints;
use crate::TxId;

pub(crate) struct TxDependency {
    // if txi <- txj, then tx_dependency[txj - num_finality_txs].push(txi)
    tx_dependency: Vec<Vec<TxId>>,
    // when a tx is in finality state, we don't need to store their dependencies
    num_finality_txs: usize,
    // After one round of transaction execution,
    // the running time can be obtained, which can make the next round of partitioning more balanced.
    tx_running_time: Option<Vec<u64>>,
    // Partitioning is balanced based on weights.
    // In the first round, weights can be assigned based on transaction type and called contract type,
    // while in the second round, weights can be assigned based on tx_running_time.
    tx_weight: Option<Vec<usize>>,
}

impl TxDependency {
    pub fn new(parallel_execution_hints: &ParallelExecutionHints) -> Self {
        TxDependency {
            tx_dependency: Self::generate_tx_dependency(parallel_execution_hints),
            num_finality_txs: 0,
            tx_running_time: None,
            tx_weight: None,
        }
    }

    pub fn generate_tx_dependency(
        parallel_execution_hints: &ParallelExecutionHints,
    ) -> Vec<Vec<TxId>> {
        let mut tx_dependency: Vec<Vec<TxId>> = vec![];
        let mut write_set: HashMap<Address, BTreeSet<TxId>> = HashMap::new();
        for (txid, rw_set) in parallel_execution_hints.txs_hint.iter().enumerate() {
            let mut dependencies = BTreeSet::new();
            for (address, _) in rw_set.read_kv_set.borrow().iter() {
                if let Some(written_transactions) = write_set.get(address) {
                    if let Some(previous) = written_transactions.range(..txid).next_back() {
                        dependencies.insert(*previous);
                    }
                }
            }
            for (address, _) in rw_set.write_kv_set.borrow().iter() {
                write_set.entry(*address).or_default().insert(txid);
            }
            tx_dependency.push(dependencies.into_iter().collect());
        }
        return tx_dependency;
    }

    pub fn fetch_best_partitions(&mut self, partition_count: usize) -> Vec<Vec<TxId>> {
        let mut num_group = 0;
        let mut weighted_group: BTreeMap<usize, Vec<Vec<TxId>>> = BTreeMap::new();
        let tx_weight = self.tx_weight.clone().unwrap_or_else(|| vec![1; self.tx_dependency.len()]);
        let mut grouped = vec![false; self.tx_dependency.len()];
        let num_finality_txs = self.num_finality_txs;
        let mut txid = self.num_finality_txs + self.tx_dependency.len() - 1;
        // Because transactions only rely on transactions with lower ID,
        // we can search from the transaction with the highest ID from back to front.
        // Despite having three layers of loops, the time complexity is only o(num_txs)
        while txid >= num_finality_txs {
            if !grouped[txid - num_finality_txs] {
                let mut group: Vec<TxId> = vec![];
                let mut weight: usize = 0;
                // Traverse the breadth from back to front
                let mut breadth_queue = VecDeque::new();
                breadth_queue.push_back(txid);
                while let Some(top) = breadth_queue.pop_front() {
                    if !grouped[txid - num_finality_txs] {
                        grouped[txid - num_finality_txs] = true;
                        for top_down in self.tx_dependency[top].iter() {
                            if !grouped[txid - num_finality_txs] {
                                breadth_queue.push_back(*top_down);
                            }
                        }
                        weight += tx_weight[top - num_finality_txs];
                        group.push(top);
                    }
                }
                weighted_group.entry(weight).or_default().push(group);
                num_group += 1;
            }
            txid -= 1;
        }

        let num_partitions = min(partition_count, num_group);
        if num_partitions == 0 {
            return vec![vec![]];
        }
        let mut partitioned_group = vec![BTreeSet::new(); num_partitions];
        let mut partition_weight = BinaryHeap::new();
        for index in 0..num_partitions {
            partition_weight.push(Reverse((0, index)));
        }
        for (add_weight, groups) in weighted_group.into_iter().rev() {
            for group in groups {
                if let Some(Reverse((weight, index))) = partition_weight.pop() {
                    partitioned_group[index].extend(group);
                    let new_weight = weight + add_weight;
                    partition_weight.push(Reverse((new_weight, index)));
                }
            }
        }
        partitioned_group
            .into_iter()
            .filter(|bs| !bs.is_empty())
            .map(|bs| bs.into_iter().collect())
            .collect()
    }

    pub fn update_tx_dependency(&mut self, tx_dependency: Vec<Vec<TxId>>, num_finality_txs: usize) {
        if (self.tx_dependency.len() + self.num_finality_txs)
            != (tx_dependency.len() + num_finality_txs)
        {
            panic!("Different transaction number");
        }
        self.tx_dependency = tx_dependency;
        self.num_finality_txs = num_finality_txs;
    }
}
