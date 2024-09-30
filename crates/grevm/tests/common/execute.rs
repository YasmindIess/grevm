use crate::common::MINER_ADDRESS;
use reth_chainspec::NamedChain;
use reth_grevm::{ExecuteOutput, GrevmScheduler};
use reth_revm::db::states::bundle_state::BundleRetention;
use reth_revm::db::states::StorageSlot;
use reth_revm::db::{BundleAccount, BundleState, PlainAccount};
use reth_revm::{DatabaseCommit, EvmBuilder, StateBuilder};
use revm_primitives::alloy_primitives::{U160, U256};
use revm_primitives::db::DatabaseRef;
use revm_primitives::{
    AccountInfo, Address, EVMError, Env, ExecutionResult, SpecId, TxEnv, KECCAK_EMPTY,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

fn compare_bundle_state(left: &BundleState, right: &BundleState) {
    let left = left.clone();
    let right = right.clone();
    assert!(
        left.contracts.keys().all(|k| right.contracts.contains_key(k)),
        "Left contracts: {:?}, Right contracts: {:?}",
        left.contracts.keys(),
        right.contracts.keys()
    );
    assert_eq!(
        left.contracts.len(),
        right.contracts.len(),
        "Left contracts: {:?}, Right contracts: {:?}",
        left.contracts.keys(),
        right.contracts.keys()
    );

    let left_state: BTreeMap<Address, BundleAccount> = left.state.into_iter().collect();
    let right_state: BTreeMap<Address, BundleAccount> = right.state.into_iter().collect();
    assert_eq!(left_state.len(), right_state.len());

    for ((addr1, account1), (addr2, account2)) in
        left_state.into_iter().zip(right_state.into_iter())
    {
        assert_eq!(addr1, addr2);
        let BundleAccount { info, original_info, storage, status } = account1;
        assert_eq!(info, account2.info);
        assert_eq!(original_info, account2.original_info);
        assert_eq!(status, account2.status);
        let left_storage: BTreeMap<U256, StorageSlot> = storage.into_iter().collect();
        let right_storage: BTreeMap<U256, StorageSlot> = account2.storage.into_iter().collect();
        for (s1, s2) in left_storage.into_iter().zip(right_storage.into_iter()) {
            assert_eq!(s1, s2);
        }
    }
}

fn compare_execution_result(left: &Vec<ExecutionResult>, right: &Vec<ExecutionResult>) {
    for (i, (left_res, right_res)) in left.iter().zip(right.iter()).enumerate() {
        assert_eq!(left_res, right_res, "Tx {}", i);
    }
    assert_eq!(left.len(), right.len());
}

pub fn mock_miner_account() -> (Address, PlainAccount) {
    let address = Address::from(U160::from(MINER_ADDRESS));
    let account = PlainAccount {
        info: AccountInfo { balance: U256::from(0), nonce: 1, code_hash: KECCAK_EMPTY, code: None },
        storage: Default::default(),
    };
    (address, account)
}

pub fn mock_eoa_account(idx: usize) -> (Address, PlainAccount) {
    let address = Address::from(U160::from(idx));
    let account = PlainAccount {
        info: AccountInfo {
            balance: U256::from(500_000_000),
            nonce: 1,
            code_hash: KECCAK_EMPTY,
            code: None,
        },
        storage: Default::default(),
    };
    (address, account)
}

pub fn mock_block_accounts(from: usize, size: usize) -> HashMap<Address, PlainAccount> {
    let mut accounts: HashMap<Address, PlainAccount> =
        (from..(from + size)).map(mock_eoa_account).collect();
    let miner = mock_miner_account();
    accounts.insert(miner.0, miner.1);
    accounts
}

pub fn compare_evm_execute<DB>(db: DB, txs: Vec<TxEnv>, with_hints: bool)
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync + Clone + Debug,
{
    let mut env = Env::default();
    env.cfg.chain_id = NamedChain::Mainnet.into();
    env.block.coinbase = Address::from(U160::from(MINER_ADDRESS));
    let db = Arc::new(db);
    let sequential = GrevmScheduler::new(SpecId::LATEST, env.clone(), db.clone(), txs.clone());
    let sequential_result = sequential.sequential_execute();
    let mut parallel = GrevmScheduler::new(SpecId::LATEST, env.clone(), db.clone(), txs.clone());
    if !with_hints {
        parallel.clean_dependency();
    }

    let parallel_result = parallel.evm_execute(Some(false));
    println!("parallel execute round: {:?}", parallel.round);

    let reth_result = execute_revm_sequential(db.clone(), SpecId::LATEST, env.clone(), txs.clone());

    compare_execution_result(
        &reth_result.as_ref().unwrap().results,
        &sequential_result.as_ref().unwrap().results,
    );
    compare_execution_result(
        &reth_result.as_ref().unwrap().results,
        &parallel_result.as_ref().unwrap().results,
    );

    compare_bundle_state(
        &reth_result.as_ref().unwrap().state,
        &sequential_result.as_ref().unwrap().state,
    );
    compare_bundle_state(
        &reth_result.as_ref().unwrap().state,
        &parallel_result.as_ref().unwrap().state,
    );
}

/// Simulate the sequential execution of transactions in reth
pub(crate) fn execute_revm_sequential<DB>(
    db: DB,
    spec_id: SpecId,
    env: Env,
    txs: Vec<TxEnv>,
) -> Result<ExecuteOutput, EVMError<DB::Error>>
where
    DB: DatabaseRef,
    DB::Error: Debug,
{
    let db = StateBuilder::new()
        .with_bundle_update()
        .without_state_clear()
        .with_database_ref(db)
        .build();
    let mut evm =
        EvmBuilder::default().with_db(db).with_spec_id(spec_id).with_env(Box::new(env)).build();

    let mut results = Vec::with_capacity(txs.len());
    for tx in txs {
        *evm.tx_mut() = tx;
        let result_and_state = evm.transact()?;
        evm.db_mut().commit(result_and_state.state);
        results.push(result_and_state.result);
    }

    evm.db_mut().merge_transitions(BundleRetention::Reverts);

    Ok(ExecuteOutput { state: evm.db_mut().take_bundle(), results })
}
