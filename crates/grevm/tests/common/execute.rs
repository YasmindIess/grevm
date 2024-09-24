use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use reth_chainspec::NamedChain;
use reth_grevm::GrevmScheduler;
use revm_primitives::alloy_primitives::{U160, U256};
use revm_primitives::db::DatabaseRef;
use revm_primitives::{
    Account, AccountInfo, AccountStatus, Address, Env, SpecId, TxEnv, KECCAK_EMPTY,
};
use reth_revm::db::{BundleAccount, BundleState};
use reth_revm::db::states::StorageSlot;

fn compare_bundle_state(left: BundleState, right: BundleState) {
    // only compare state
    let left: BTreeMap<Address, BundleAccount> = left.state.into_iter().collect();
    let right: BTreeMap<Address, BundleAccount> = right.state.into_iter().collect();
    for (b1, b2) in left.into_iter().zip(right.into_iter()) {
        assert_eq!(b1.0, b2.0);
        let BundleAccount{ info, original_info, storage, status } = b1.1;
        assert_eq!(info, b2.1.info);
        assert_eq!(original_info, b2.1.original_info);
        assert_eq!(status, b2.1.status);
        let storage1: BTreeMap<U256, StorageSlot> = storage.into_iter().collect();
        let storage2: BTreeMap<U256, StorageSlot> = b2.1.storage.into_iter().collect();
        assert_eq!(storage1, storage2);
    }
}

pub fn mock_eoa_account(idx: usize) -> (Address, Account) {
    let address = Address::from(U160::from(idx));
    let account = Account {
        info: AccountInfo {
            // half of max balance
            balance: U256::MAX.div_ceil(U256::from(2)),
            nonce: 1,
            code_hash: KECCAK_EMPTY,
            code: None,
        },
        storage: Default::default(),
        status: AccountStatus::Loaded,
    };
    (address, account)
}

pub fn compare_evm_execute<DB>(db: DB, txs: Vec<TxEnv>)
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync + Clone + Debug,
{
    let mut env = Env::default();
    env.cfg.chain_id = NamedChain::Mainnet.into();
    // take `Address::ZERO` as the beneficiary account.
    env.block.coinbase = Address::ZERO;
    let db = Arc::new(db);
    let sequential_result =
        GrevmScheduler::new(SpecId::LATEST, env.clone(), db.clone(), txs.clone()).sequential_execute();
    let parallel_result =
        GrevmScheduler::new(SpecId::LATEST, env.clone(), db.clone(), txs.clone()).parallel_execute();

    compare_bundle_state(sequential_result.unwrap().state, parallel_result.unwrap().state);
}
