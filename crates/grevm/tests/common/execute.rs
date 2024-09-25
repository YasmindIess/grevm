use reth_chainspec::NamedChain;
use reth_grevm::{ExecuteOutput, GrevmScheduler};
use reth_revm::db::states::bundle_state::BundleRetention;
use reth_revm::db::{BundleAccount, BundleState};
use reth_revm::{DatabaseCommit, EvmBuilder, StateBuilder};
use revm_primitives::alloy_primitives::{U160, U256};
use revm_primitives::db::DatabaseRef;
use revm_primitives::{
    Account, AccountInfo, AccountStatus, Address, Env, SpecId, TxEnv, KECCAK_EMPTY,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

fn compare_bundle_state(left: &BundleState, right: &BundleState) {
    let left_state: BTreeMap<&Address, &BundleAccount> = left.state.iter().collect();
    let right_state: BTreeMap<&Address, &BundleAccount> = right.state.iter().collect();
    for ((addr1, account1), (addr2, account2)) in
        left_state.into_iter().zip(right_state.into_iter())
    {
        assert_eq!(addr1, addr2);
        assert_eq!(account1, account2, "Address: {:?}", addr1);
    }

    assert_eq!(left.contracts, right.contracts);
}

pub fn mock_eoa_account(idx: usize) -> (Address, Account) {
    let address = Address::from(U160::from(idx));
    // 0 for miner(Address::ZERO)
    let balance = if idx == 0 { U256::from(0) } else { U256::from(500_000_000) };
    let account = Account {
        info: AccountInfo { balance, nonce: 1, code_hash: KECCAK_EMPTY, code: None },
        storage: Default::default(),
        status: AccountStatus::Loaded,
    };
    (address, account)
}

pub fn compare_evm_execute<DB>(db: DB, txs: Vec<TxEnv>, with_hints: bool)
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync + Clone + Debug,
{
    let mut env = Env::default();
    env.cfg.chain_id = NamedChain::Mainnet.into();
    // take `Address::ZERO` as the beneficiary account.
    env.block.coinbase = Address::ZERO;
    let db = Arc::new(db);
    let sequential = GrevmScheduler::new(SpecId::LATEST, env.clone(), db.clone(), txs.clone());
    let sequential_result = sequential.sequential_execute();
    let mut parallel = GrevmScheduler::new(SpecId::LATEST, env.clone(), db.clone(), txs.clone());
    if !with_hints {
        parallel.clean_dependency();
    }
    let parallel_result = parallel.parallel_execute();

    let reth_result = execute_revm_sequential(db.clone(), SpecId::LATEST, env.clone(), txs.clone());

    assert_eq!(reth_result.as_ref().unwrap().results, sequential_result.as_ref().unwrap().results);

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
) -> Option<ExecuteOutput>
where
    DB: DatabaseRef,
    DB::Error: Debug,
{
    let db = StateBuilder::new().with_database_ref(db).build();
    let mut evm =
        EvmBuilder::default().with_db(db).with_spec_id(spec_id).with_env(Box::new(env)).build();

    let mut results = Vec::with_capacity(txs.len());
    for tx in txs {
        *evm.tx_mut() = tx;
        match evm.transact() {
            Ok(result_and_state) => {
                evm.db_mut().commit(result_and_state.state);
                results.push(result_and_state.result);
            }
            Err(err) => {
                println!("Error: {:?}", err);
                return None;
            }
        }
    }

    evm.db_mut().merge_transitions(BundleRetention::Reverts);

    Some(ExecuteOutput { state: evm.db_mut().take_bundle(), results })
}
