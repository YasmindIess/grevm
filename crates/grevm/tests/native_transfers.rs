use std::collections::HashMap;

use revm_primitives::{alloy_primitives::U160, Account, Address, TransactTo, TxEnv, U256};

use common::InMemoryDB;

pub mod common;

#[test]
fn native_transfers_independent() {
    let block_size = 10_000; // number of transactions
    let accounts: HashMap<Address, Account> =
        (0..=block_size).map(common::mock_eoa_account).collect();
    let db = InMemoryDB::new(accounts, Default::default(), Default::default());
    // `Address::ZERO` is the minner address in compare_evm_execute
    // Skipping `Address::ZERO` as the beneficiary account.
    let txs: Vec<TxEnv> = (1..=block_size)
        .map(|i| {
            let address = Address::from(U160::from(i));
            TxEnv {
                caller: address,
                transact_to: TransactTo::Call(address),
                value: U256::from(1),
                gas_limit: common::TRANSFER_GAS_LIMIT,
                gas_price: U256::from(1),
                ..TxEnv::default()
            }
        })
        .collect();
    common::compare_evm_execute(db, txs);
}
