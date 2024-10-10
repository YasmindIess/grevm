use crate::hint::ParallelExecutionHints;
use crate::{LocationAndType, TxId};
use smallvec::SmallVec;
use std::cmp::{min, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

pub(crate) type DependentTxsVec = SmallVec<[TxId; 1]>;

const RAW_TRANSFER_WEIGHT: usize = 1;

pub(crate) struct TxDependency {
    // if txi <- txj, then tx_dependency[txj - num_finality_txs].push(txi)
    tx_dependency: Vec<DependentTxsVec>,
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

    pub fn clean_dependency(&mut self) {
        let len = self.tx_dependency.len();
        self.tx_dependency = vec![DependentTxsVec::new(); len];
    }

    #[fastrace::trace]
    fn generate_tx_dependency(
        parallel_execution_hints: &ParallelExecutionHints,
    ) -> Vec<DependentTxsVec> {
        let mut tx_dependency: Vec<DependentTxsVec> =
            Vec::with_capacity(parallel_execution_hints.txs_hint.len());
        let mut write_set: HashMap<LocationAndType, Vec<TxId>> = HashMap::new();
        for (txid, rw_set) in parallel_execution_hints.txs_hint.iter().enumerate() {
            let mut dependencies = DependentTxsVec::new();
            for location in rw_set.read_set.iter() {
                if let Some(written_transactions) = write_set.get(location) {
                    if let Some(previous) = written_transactions.last() {
                        dependencies.push(*previous);
                    }
                }
            }
            for location in rw_set.write_set.iter() {
                write_set.entry(location.clone()).or_default().push(txid);
            }
            tx_dependency.push(dependencies);
        }
        return tx_dependency;
    }

    pub fn fetch_best_partitions(&mut self, partition_count: usize) -> Vec<Vec<TxId>> {
        let mut num_group = 0;
        let mut weighted_group: BTreeMap<usize, Vec<DependentTxsVec>> = BTreeMap::new();
        let tx_weight = self
            .tx_weight
            .take()
            .unwrap_or_else(|| vec![RAW_TRANSFER_WEIGHT; self.tx_dependency.len()]);
        let num_finality_txs = self.num_finality_txs;
        let mut txid = self.num_finality_txs + self.tx_dependency.len() - 1;

        // the revert of self.tx_dependency
        // if txi <- txj, then tx_dependency[txi - num_finality_txs].push(txj)
        let mut revert_dependency: Vec<DependentTxsVec> =
            vec![DependentTxsVec::new(); self.tx_dependency.len()];
        let mut is_related: Vec<bool> = vec![false; self.tx_dependency.len()];
        {
            let mut single_groups = weighted_group.entry(RAW_TRANSFER_WEIGHT).or_default();
            for index in (0..self.tx_dependency.len()).rev() {
                let txj = index + num_finality_txs;
                let txj_dep = &self.tx_dependency[index];
                if txj_dep.is_empty() {
                    if !is_related[index] {
                        let mut single_group = DependentTxsVec::new();
                        single_group.push(txj);
                        single_groups.push(single_group);
                        num_group += 1;
                    }
                } else {
                    is_related[index] = true;
                    for txi in txj_dep {
                        let txi_index = *txi - num_finality_txs;
                        revert_dependency[txi_index].push(txj);
                        is_related[txi_index] = true;
                    }
                }
            }
            if single_groups.is_empty() {
                weighted_group.remove(&RAW_TRANSFER_WEIGHT);
            }
        }
        // Because transactions only rely on transactions with lower ID,
        // we can search from the transaction with the highest ID from back to front.
        // Despite having three layers of loops, the time complexity is only o(num_txs)
        let mut breadth_queue = VecDeque::new();
        while txid >= num_finality_txs {
            let index = txid - num_finality_txs;
            if is_related[index] {
                let mut group = DependentTxsVec::new();
                let mut weight: usize = 0;
                // Traverse the breadth from back to front
                breadth_queue.clear();
                breadth_queue.push_back(index);
                is_related[index] = false;
                while let Some(top_index) = breadth_queue.pop_front() {
                    // txj -> txi, where txj = top_index + num_finality_txs
                    for top_down in self.tx_dependency[top_index]
                        .iter()
                        .chain(revert_dependency[top_index].iter())
                    // txk -> txj, where txj = top_index + num_finality_txs
                    {
                        let next_index = *top_down - num_finality_txs;
                        if is_related[next_index] {
                            breadth_queue.push_back(next_index);
                            is_related[next_index] = false;
                        }
                    }
                    weight += tx_weight[index];
                    group.push(top_index + num_finality_txs);
                }
                weighted_group.entry(weight).or_default().push(group);
                num_group += 1;
            }
            if txid == 0 {
                break;
            }
            txid -= 1;
        }

        let num_partitions = min(partition_count, num_group);
        if num_partitions == 0 {
            return vec![vec![]];
        }
        let mut partitioned_mutex_group = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            partitioned_mutex_group.push(Mutex::new(BTreeSet::new()));
        }
        let mut partition_weight = BinaryHeap::new();
        // Separate processing of groups with a weight of 1
        // Because there is only one transaction in these groups,
        // processing them separately can greatly optimize performance.
        if let Some(groups) = weighted_group.remove(&RAW_TRANSFER_WEIGHT) {
            let chunk_size = (groups.len() as f64 / num_partitions as f64).ceil() as usize;
            let partition_index = AtomicUsize::new(0);
            thread::scope(|scope| {
                for _ in 0..num_partitions {
                    scope.spawn(|| {
                        let index = partition_index.fetch_add(1, Ordering::SeqCst);
                        let start_pos = chunk_size * index;
                        let end_pos = min(groups.len(), start_pos + chunk_size);
                        let mut partition = partitioned_mutex_group[index].lock().unwrap();
                        for pos in start_pos..end_pos {
                            for txid in groups[pos].iter() {
                                partition.insert(*txid);
                            }
                        }
                    });
                }
            });
        }
        let mut partitioned_group: Vec<BTreeSet<TxId>> = partitioned_mutex_group
            .into_iter()
            .map(|partition| partition.into_inner().unwrap())
            .collect();
        for index in 0..num_partitions {
            partition_weight
                .push(Reverse((partitioned_group[index].len() * RAW_TRANSFER_WEIGHT, index)));
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

    pub fn update_tx_dependency(
        &mut self,
        tx_dependency: Vec<DependentTxsVec>,
        num_finality_txs: usize,
    ) {
        if (self.tx_dependency.len() + self.num_finality_txs)
            != (tx_dependency.len() + num_finality_txs)
        {
            panic!("Different transaction number");
        }
        self.tx_dependency = tx_dependency;
        self.num_finality_txs = num_finality_txs;
    }
}
